(ns jepsen.clickhouse-keeper.counter
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen
    [checker :as checker]
    [client :as client]
    [generator :as gen]]
   [jepsen.clickhouse-keeper.utils :refer :all]
   [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)))

(def root-path "/counter")
(defn r   [_ _] {:type :invoke, :f :read})
(defn add [_ _] {:type :invoke, :f :add, :value (rand-int 5)})

(defrecord CounterClient [conn nodename]
  client/Client
  (open! [this test node]
    (assoc
     (assoc this
            :conn (zk-connect node 9181 30000))
     :nodename node))

  (setup! [this test]
    (exec-with-retries 30 (fn []
      (zk-create-if-not-exists conn root-path ""))))

  (invoke! [this test op]
    (case (:f op)
      :read (exec-with-retries 30 (fn []
                                    (assoc op
                                           :type :ok
                                           :value (count (zk-list conn root-path)))))
      :add (try
             (do
               (zk-multi-create-many-seq-nodes conn (concat-path root-path "seq-") (:value op))
               (assoc op :type :ok))
             (catch Exception _ (assoc op :type :info, :error :connect-error)))))

  (teardown! [_ test])

  (close! [_ test]
    (zk/close conn)))

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client    (CounterClient. nil nil)
   :checker   (checker/compose
                {:counter (checker/counter)
                 :perf    (checker/perf)})
   :generator (->> (range)
                   (map (fn [x]
                          (->> (gen/mix [r add])))))
   :final-generator (gen/once {:type :invoke, :f :read, :value nil})})
