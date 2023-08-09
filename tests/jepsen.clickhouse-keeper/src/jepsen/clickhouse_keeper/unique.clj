(ns jepsen.clickhouse-keeper.unique
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen
    [checker :as checker]
    [client :as client]
    [generator :as gen]]
   [jepsen.clickhouse-keeper.utils :refer :all]
   [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)))

(defrecord UniqueClient [conn nodename]
  client/Client
  (open! [this test node]
    (assoc
     (assoc this
            :conn (zk-connect node 9181 30000))
     :nodename node))

  (setup! [this test])

  (invoke! [this test op]
    (case
     :generate
      (try
        (let [result-path (zk-create-sequential conn "/seq-" "")]
          (assoc op :type :ok :value (parse-and-get-counter result-path)))
        (catch Exception _ (assoc op :type :info, :error :connect-error)))))

  (teardown! [_ test])

  (close! [_ test]
    (zk/close conn)))

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client    (UniqueClient. nil nil)
   :checker   (checker/compose
                {:perf   (checker/perf)
                 :unique (checker/unique-ids)})
   :generator (->>
               (range)
               (map (fn [_] {:type :invoke, :f :generate})))})
