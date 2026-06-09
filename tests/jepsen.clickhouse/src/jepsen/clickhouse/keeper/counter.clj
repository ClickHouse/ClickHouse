(ns jepsen.clickhouse.keeper.counter
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen
    [checker :as checker]
    [client :as client]
    [generator :as gen]
    [history :as h]]
   [jepsen.clickhouse.keeper.utils :refer :all]
   [jepsen.clickhouse.utils :as chu]
   [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)))

(def root-path "/counter")
(defn r   [_ _] {:type :invoke, :f :read})
(defn add [_ _] {:type :invoke, :f :add, :value (long (rand-int 5))})

(defrecord CounterClient [conn nodename]
  client/Client
  (open! [this test node]
    (assoc
     (assoc this
            :conn (zk-connect node 9181 30000 (:with-auth test)))
     :nodename node))

  (setup! [this test]
    (chu/exec-with-retries 30 (fn []
      (zk-create-if-not-exists conn root-path "" :with-acl (:with-auth test)))))

  (invoke! [this test op]
    (case (:f op)
      :read (try
             (assoc op
               :type :ok
               :value (long (count (zk-list conn root-path))))
             (catch Exception _ (assoc op :type :info, :error :connect-error)))
      :final-read (chu/exec-with-retries 30 (fn [] (assoc op
                                                     :type :ok
                                                     :value (long (count (zk-list conn root-path))))))
      :add (try
             (do
               (zk-multi-create-many-seq-nodes conn (concat-path root-path "seq-") (:value op) :with-acl (:with-auth test))
               (assoc op :type :ok))
             (catch Exception _ (assoc op :type :info, :error :connect-error)))))

  (teardown! [_ test])

  (close! [_ test]
    (zk/close conn)))

(defn counter-checker
  "jepsen 0.3.11's `checker/counter` asserts that every operation's `:value` is
  `nil` or a `Long` (checker.clj:801), which trips on nemesis operations that
  carry non-Long values (partition grudges, node lists, keywords). Restrict the
  check to client operations so only counter reads and adds are inspected."
  []
  (let [c (checker/counter)]
    (reify checker/Checker
      (check [_ test history opts]
        (checker/check c test (h/client-ops history) opts)))))

(defn workload
  "A generator, client, and checker for a set test."
  [opts]
  {:client    (CounterClient. nil nil)
   :checker   (checker/compose
                {:counter (counter-checker)
                 :perf    (checker/perf)})
   :generator (gen/mix [r add])
   :final-generator (gen/once {:type :invoke, :f :final-read, :value nil})})
