(ns jepsen.nukeeper.set
  (:require   [jepsen
               [checker :as checker]
               [client :as client]
               [generator :as gen]]
              [jepsen.nukeeper.utils :refer :all]
              [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)))

(defrecord SetClient [k conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (zk-connect node 9181 30000)))

  (setup! [this test]
    (zk-create-if-not-exists conn k "#{}"))

  (invoke! [_ test op]
    (case (:f op)
      :read ;(try
      (assoc op
             :type :ok
             :value (read-string (:data (zk-get-str conn k))))
              ;(catch Exception _ (assoc op :type :fail, :error :connect-error)))
      :add (try
             (do
               (zk-add-to-set conn k (:value op))
               (assoc op :type :ok))
             (catch KeeperException$BadVersionException _ (assoc op :type :fail, :error :bad-version))
             (catch Exception _ (assoc op :type :info, :error :connect-error)))))

  (teardown! [_ test])

  (close! [_ test]))

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client    (SetClient. "/a-set" nil)
   :checker   (checker/set)
   :generator (->> (range)
                   (map (fn [x] {:type :invoke, :f :add, :value x})))
   :final-generator (gen/once {:type :invoke, :f :read, :value nil})})
