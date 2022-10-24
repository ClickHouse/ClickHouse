(ns jepsen.clickhouse.server.register
  (:require   [jepsen
               [checker :as checker]
               [client :as client]
               [independent :as independent]
               [generator :as gen]]
              [jepsen.checker.timeline :as timeline]
              [jepsen.clickhouse.utils :as chu]
              [jepsen.clickhouse.server.client :as chc]
              [clojure.tools.logging :refer :all]
              [clojure.java.jdbc :as j]
              [knossos.model :as model]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord RegisterClient [table-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (chc/client node)))

  (setup! [this test]
    (locking table-created?
      (when (compare-and-set! table-created? false true)
        (chc/with-connection [c conn]
          (j/query c "DROP TABLE IF EXISTS register")
          (j/query c "CREATE TABLE register (id Int64, value Int64) ENGINE=MergeTree ORDER BY id")
          (info (j/query c "SHOW CREATE TABLE register"))))))

  (invoke! [_ test op]
    (print "invoke"))

  (teardown! [this test]
    nil)

  (close! [_ test]
    (print "close")))

(defn workload
  "Tests linearizable reads, writes, and compare-and-set operations on
  independent keys."
  [opts]
  {:client    (RegisterClient. (atom false) nil)
   :checker   (independent/checker
               (checker/compose
                {:linear   (checker/linearizable {:model     (model/cas-register)
                                                  :algorithm :linear})
                 :perf     (checker/perf)
                 :timeline (timeline/html)}))
   :generator (independent/concurrent-generator
               10
               (range)
               (fn [k]
                 (->> (gen/mix [r w cas])
                      (gen/limit (:ops-per-key opts)))))})
