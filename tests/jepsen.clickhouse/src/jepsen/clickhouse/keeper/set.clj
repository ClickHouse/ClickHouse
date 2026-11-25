(ns jepsen.clickhouse.keeper.set
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen
    [checker :as checker]
    [client :as client]
    [generator :as gen]]
   [jepsen.clickhouse.keeper.utils :refer :all]
   [jepsen.clickhouse.utils :as chu]
   [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)))

(defrecord SetClient [k conn nodename]
  client/Client
  (open! [this test node]
    (assoc
     (assoc this
            :conn (zk-connect node 9181 30000 (:with-auth test)))
     :nodename node))

  (setup! [this test]
    (chu/exec-with-retries 30 (fn []
                            (zk-create-if-not-exists conn k "#{}" :with-acl (:with-auth test)))))

  (invoke! [this test op]
    (case (:f op)
      :read (chu/exec-with-retries 30 (fn []
                                    (zk-sync conn)
                                    (assoc op
                                           :type :ok
                                           :value (read-string (:data (zk-get-str conn k))))))
      :add (try
             (do
               (zk-add-to-set conn k (:value op))
               (assoc op :type :ok))
             (catch KeeperException$BadVersionException _ (assoc op :type :fail, :error :bad-version))
             (catch Exception _ (assoc op :type :info, :error :connect-error)))))

  (teardown! [_ test])

  (close! [_ test]
    (zk/close conn)))

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client    (SetClient. "/a-set" nil nil)
   :checker   (checker/compose
                {:set (checker/set)
                 :perf (checker/perf)})
   :generator (->> (range)
                   (map (fn [x] {:type :invoke, :f :add, :value x})))
   :final-generator (gen/once {:type :invoke, :f :read, :value nil})})
