(ns jepsen.nukeeper.queue
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen
    [checker :as checker]
    [client :as client]
    [generator :as gen]]
   [jepsen.nukeeper.utils :refer :all]
   [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)))

(defn enqueue   [val _ _] {:type :invoke, :f :enqueue :value val})
(defn dequeue [_ _] {:type :invoke, :f :dequeue})

(defrecord QueueClient [conn nodename]
  client/Client
  (open! [this test node]
    (assoc
     (assoc this
            :conn (zk-connect node 9181 30000))
     :nodename node))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :enqueue (try
                 (do
                   (zk-create-if-not-exists conn (str "/" (:value op)) "")
                   (assoc op :type :ok))
                 (catch Exception _ (assoc op :type :info, :error :connect-error)))
      :dequeue
      (try
        (let [result (zk-multi-delete-first-child conn "/")]
          (if (not (nil? result))
            (assoc op :type :ok :value result)
            (assoc op :type :fail :value result)))
        (catch KeeperException$BadVersionException _ (assoc op :type :fail, :error :bad-version))
        (catch Exception _ (assoc op :type :info, :error :connect-error)))
      :drain
      (try
        (loop [result '()]
          (let [deleted-child (zk-multi-delete-first-child conn "/")]
            (if (not (nil? deleted-child))
              (recur (concat result [deleted-child]))
              (assoc op :type :ok :value result))))
        (catch Exception _ (assoc op :type :info, :error :connect-error)))))

  (teardown! [_ test])

  (close! [_ test]
    (zk/close conn)))

(defn sorted-str-range
  [n]
  (sort (map (fn [v] (str v)) (take n (range)))))

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client    (QueueClient. nil nil)
   :checker   (checker/total-queue)
   :generator (->> (sorted-str-range 10000)
                   (map (fn [x]
                          (rand-nth [{:type :invoke, :f :enqueue :value x}
                                     {:type :invoke, :f :dequeue}]))))
   :final-generator (gen/once {:type :invoke, :f :drain, :value nil})})
