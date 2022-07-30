(ns jepsen.clickhouse-keeper.register
  (:require   [jepsen
               [checker :as checker]
               [client :as client]
               [independent :as independent]
               [generator :as gen]]
              [jepsen.checker.timeline :as timeline]
              [knossos.model :as model]
              [jepsen.clickhouse-keeper.utils :refer :all]
              [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord RegisterClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (zk-connect node 9181 30000)))

  (setup! [this test]
    (zk-create-range conn 300)) ; 300 nodes to be sure

  (invoke! [_ test op]
    (let [[k v] (:value op)
          zk-k (zk-path k)]
      (case (:f op)
        :read (try
                (assoc op :type :ok, :value (independent/tuple k (parse-long (:data (zk-get-str conn zk-k)))))
                (catch Exception _ (assoc op :type :fail, :error :connect-error)))
        :write (try
                 (do (zk-set conn zk-k v)
                     (assoc op :type :ok))
                 (catch Exception _ (assoc op :type :info, :error :connect-error)))
        :cas (try
               (let [[old new] v]
                 (assoc op :type (if (zk-cas conn zk-k old new)
                                   :ok
                                   :fail)))
               (catch KeeperException$BadVersionException _ (assoc op :type :fail, :error :bad-version))
               (catch Exception _ (assoc op :type :info, :error :connect-error))))))

  (teardown! [this test])

  (close! [_ test]
    (zk/close conn)))

(defn workload
  "Tests linearizable reads, writes, and compare-and-set operations on
  independent keys."
  [opts]
  {:client    (RegisterClient. nil)
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
