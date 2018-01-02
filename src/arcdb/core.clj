(ns arcdb.core
  (:gen-class))

;; An Archaeology-Inspired Database
;; Yoav Rubin
;; http://aosabook.org/en/500L/an-archaeology-inspired-database.html

;; A database consists of:
;; 1) Layers of entities, each with its own unique timestamp.
;; 2) A top-id value which is the next available unique ID.
;; 3) The time at which the database was last updated.
(defrecord Database [layers top-id curr-time])


;; Each layer consists of:
;; 1) A data store for entities.
;; 2) Indexes that are used to speed up queries to the database.
(defrecord Layer [storage VAET AVET VEAT EAVT])


;; Our database wouldn't be of any use without entities to store,
;; so we define those next. As discussed before, 
;; an entity has an ID and a list of attributes;
;; we create them using the 'make-entity' function.
(defrecord Entity [id attrs])

(defn make-entity
  ([] (make-entity :db/no-id-yet))
  ([id] (Entity. id {})))



;; Each attribute consists of its name, value, and the timestamps 
;; of its most recent update as well as the one before that. 
;; Each attribute also has two fields that describe its type and cardinality.
;;
;; In the case that an attribute is used to represent a relationship 
;; to another entity, its type will be :db/ref and its value will be 
;; the ID of the related entity. This simple type system also acts 
;; as an extension point. Users are free to define their own types and 
;; leverage them to provide additional semantics for their data.
;;
;; An attribute's cardinality specifies whether the attribute represents
;; a single value or a set of values. We use this field to determine the 
;; set of operations that are permitted on this attribute.
;;
;; Creating an attribute is done using the make-attr function.
(defrecord Attr [name value ts prev-ts])

(defn make-attr
  ([name value type & {:keys [cardinality] :or {cardinality :db/single}}]
   {:pre [(contains? #{:db/single :db/multiple} cardinality)]}
   (with-meta (Attr. name value -1 -1) {:type type :cardinality cardinality})))


;; Attributes only have meaning if they are part of an entity. 
;; We make this connection with the add-attr function, 
;; which adds a given attribute to an entity's attribute map (called :attrs).
;;
;; Note that instead of using the attribute’s name directly, 
;; we first convert it into a keyword to adhere to 
;; Clojure’s idiomatic usage of maps.
(defn add-attr [ent attr]
  (let [attr-id (keyword (:name attr))]
    (assoc-in ent [:attrs attr-id] attr)))



;; Storage
;; So far we have talked a lot about what we are going to store, 
;; without thinking about where we are going to store it. 
;; In this chapter, we resort to the simplest 
;; storage mechanism: storing the data in memory. This is certainly not reliable,
;; but it simplifies development and debugging and allows us to 
;; focus on more interesting parts of the program.
;;
;; We will access the storage via a simple protocol, which will make it possible
;; to define additional storage providers for a database owner to select from.
(defprotocol Storage
  (get-entity [storage e-id])
  (write-entity [storage entity])
  (drop-entity [storage entity]))

;; And here's our in-memory implementation of the protocol, which uses a map
;; as the store:
(defrecord InMemory [] Storage
  (get-entity [storage e-id] (e-id storage))
  (write-entity [storage entity] (assoc storage (:id entity) entity))
  (drop-entity [storage entity] (dissoc storage (:id entity))))


;; Indexing the Data
;; Now that we've defined the basic elements of our database, 
;; we can start thinking about how we're going to query it. 
;; By virtue of how we've structured our data, any query is necessarily 
;; going to be interested in at least one of an entity's ID, 
;; and the name and value of some of its attributes. 
;; This triplet of (entity-id, attribute-name, attribute-value) 
;; is important enough to our query process that 
;; we give it an explicit name: a datom.
;;
;; Datoms are important because they represent facts, and our database
;; accumulates facts.

;; Our indexes are implemented as a map of maps, where the keys of the root map
;; act as the first level, each such key points to a map whose keys 
;; act as the index’s second-level and the values are the index’s third level. 
;; Each element in the third level is a set, holding the leaves of the index.
;;
;; Each index stores the components of a datom as some permutation 
;; of its canonical 'EAV' ordering (entity-id, attribute-name, attribute-value).
;; However, when we are working with datoms outside of the index, 
;; we expect them to be in canonical format. We thus provide each index
;; with functions from-eav and to-eav to convert to and from these orderings.
;;
;; In most database systems, indexes are an optional component; 
;; for example, in an RDBMS (Relational Database Management System)
;; like PostgreSQL or MySQL, you will choose to add indexes only to 
;; certain columns in a table. We provide each index with a usage-pred function 
;; that determines for an attribute whether it should be included 
;; in this index or not.
(defn make-index [from-eav to-eav usage-pred]
  (with-meta {} {:from-eav from-eav 
                 :to-eav to-eav 
                 :usage-pred usage-pred}))

(defn from-eav [index]
  (-> index meta :from-eav))

(defn to-eav [index]
  (-> index meta :to-eav))

(defn usage-pred [index]
  (-> index meta :usage-pred))


;; In our database there are four indexes: EAVT, AVET, VEAT and VAET. 
;; We can access these as a vector of values returned from the indexes function.
(defn indexes []
  [:VAET :AVET :VEAT :EAVT])


;; Database
;; We now have all the components we need to construct our database.
;; Initializing our database means:
;; - creating an initial empty layer with no data;
;; - creating a set of empty indexes;
;; - setting its 'top-id' and 'current-time' to be 0;
(defn ref? [attr]
  (= :db/ref (-> attr meta :type)))

(defn always [& more]
  true)

(defn make-db []
  (atom
    (Database. [(Layer.
                  ; storage
                  (InMemory.)
                  ; VAET
                  (make-index #(vector %3 %2 %1) #(vector %3 %2 %1) #(ref? %))
                  ; AVET 
                  (make-index #(vector %2 %3 %1) #(vector %3 %1 %2) always) 
                  ; VEAT
                  (make-index #(vector %3 %1 %2) #(vector %2 %3 %1) always)
                  ; EAVT
                  (make-index #(vector %1 %2 %3) #(vector %1 %2 %3) always))]
                0 0))) 


;; There is one snag, though: all collections in Clojure are immutable. 
;; Since write operations are pretty critical in a database, 
;; we define our structure to be an Atom, which is a Clojure reference type 
;; that provides the capability of atomic writes.



(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
