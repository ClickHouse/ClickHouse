(ns jepsen.clickhouse.keeper.main
  (:require [clojure.tools.logging :refer :all]
            [clojure.pprint :refer [pprint]]
            [jepsen.clickhouse.keeper.utils :refer :all]
            [jepsen.clickhouse.keeper.set :as set]
            [jepsen.clickhouse.keeper.db :refer :all]
            [jepsen.clickhouse.keeper.zookeeperdb :refer :all]
            [jepsen.clickhouse.keeper.nemesis :as custom-nemesis]
            [jepsen.clickhouse.keeper.register :as register]
            [jepsen.clickhouse.keeper.unique :as unique]
            [jepsen.clickhouse.keeper.queue :as queue]
            [jepsen.clickhouse.keeper.counter :as counter]
            [jepsen.clickhouse.keeper.bench :as bench]
            [jepsen.clickhouse.constants :refer :all]
            [jepsen.clickhouse.utils :as chu]
            [clojure.string :as str]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [client :as client]
             [db :as db]
             [nemesis :as nemesis]
             [generator :as gen]
             [independent :as independent]
             [tests :as tests]
             [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.checker.timeline :as timeline]
            [clojure.java.io :as io]
            [zookeeper.data :as data]
            [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)
           (ch.qos.logback.classic Level)
           (org.slf4j Logger LoggerFactory)))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {"set"      set/workload
   "register" register/workload
   "unique-ids" unique/workload
   "counter" counter/workload
   "total-queue" queue/total-workload
   "linear-queue" queue/linear-workload})

(def cli-opts
  "Additional command line options."
  [["-w" "--workload NAME" "What workload should we run?"
    :default "set"
    :validate [workloads (cli/one-of workloads)]]
   [nil "--nemesis NAME" "Which nemesis will poison our lives?"
    :default "random-node-killer"
    :validate [custom-nemesis/custom-nemesises (cli/one-of custom-nemesis/custom-nemesises)]]
   ["-q" "--quorum" "Use quorum reads, instead of reading from any primary."]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   ["-s" "--snapshot-distance NUM" "Number of log entries to create snapshot"
    :default 10000
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--stale-log-gap NUM" "Number of log entries to send snapshot instead of separate logs"
    :default 1000
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--with-auth auth" "Enable auth on connections (0 or 1)"
    :default false
    :parse-fn #(= % "1")
    :validate [boolean? "Must be 0, 1, true or false"]]
   [nil "--reserved-log-items NUM" "Number of log entries to keep after snapshot"
    :default 1000
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn chu/parse-long
    :validate [pos? "Must be a positive integer."]]
   [nil, "--lightweight-run" "Subset of workloads/nemesises which is simple to validate"]
   [nil, "--reuse-binary" "Use already downloaded binary if it exists, don't remove it on shutdown"]
   [nil, "--bench" "Run perf-test mode"]
   [nil, "--zookeeper-version VERSION" "Run zookeeper with version"
    :default ""]
   [nil, "--bench-opts STR" "Run perf-test mode"
    :default "--generator list_medium_nodes -c 30 -i 1000"]
   ["-c" "--clickhouse-source URL" "URL for clickhouse deb or tgz package"]
   [nil "--bench-path path" "Path to keeper-bench util"
    :default "/home/alesap/code/cpp/BuildCH/utils/keeper-bench/keeper-bench"]])

(defn get-db
  [opts]
  (if (empty? (:zookeeper-version opts))
    (db (:clickhouse-source opts) (boolean (:reuse-binary opts)))
    (zookeeper-db (:zookeeper-version opts))))

(defn get-port
  [opts]
  (if (empty? (:zookeeper-version opts))
    9181
    2181))

(defn clickhouse-func-tests
  [opts]
  (info "Test opts\n" (with-out-str (pprint opts)))
  (let [quorum (boolean (:quorum opts))
        workload  ((get workloads (:workload opts)) opts)
        current-nemesis (get custom-nemesis/custom-nemesises (:nemesis opts))]
    (merge tests/noop-test
           opts
           {:name (str "clickhouse-keeper-quorum=" quorum "-"  (name (:workload opts)) "-" (name (:nemesis opts)))
            :os ubuntu/os
            :db (get-db opts)
            :pure-generators true
            :client (:client workload)
            :nemesis (:nemesis current-nemesis)
            :checker (checker/compose
                      {:perf     (checker/perf)
                       :workload (:checker workload)})
            :generator (gen/phases
                        (->> (:generator workload)
                             (gen/stagger (/ (:rate opts)))
                             (gen/nemesis (:generator current-nemesis))
                             (gen/time-limit (:time-limit opts)))
                        (gen/log "Healing cluster")
                        (gen/nemesis (gen/once {:type :info, :f :stop}))
                        (gen/log "Waiting for recovery")
                        (gen/sleep 10)
                        (gen/clients (:final-generator workload)))})))

(defn clickhouse-perf-test
  [opts]
  (info "Starting performance test")
  (let [dct {:type :invoke :bench-opts (:bench-opts opts) :bench-path (:bench-path opts)}]
    (merge tests/noop-test
           opts
           {:name (str "clickhouse-keeper-perf")
            :os ubuntu/os
            :db (get-db opts)
            :pure-generators true
            :client (bench/bench-client (get-port opts))
            :nemesis nemesis/noop
            :generator (->> dct
                            (gen/stagger 1)
                            (gen/nemesis nil))})))

(defn clickhouse-keeper-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (if (boolean (:bench opts))
    (clickhouse-perf-test opts)
    (clickhouse-func-tests opts)))

(def all-nemesises (keys custom-nemesis/custom-nemesises))

(def all-workloads (keys workloads))

(def lightweight-workloads ["set" "unique-ids" "counter" "total-queue"])

(def useful-nemesises ["random-node-killer"
                       "simple-partitioner"
                       "all-nodes-hammer-time"
                       ; can lead to a very rare data loss https://github.com/eBay/NuRaft/issues/185
                       ;"logs-and-snapshots-corruptor"
                       ;"drop-data-corruptor"
                       "bridge-partitioner"
                       "blind-node-partitioner"
                       "blind-others-partitioner"])

(defn all-test-options
  "Takes base cli options, a collection of nemeses, workloads, and a test count,
  and constructs a sequence of test options."
  [cli workload-nemesis-collection]
  (take (:test-count cli)
        (shuffle (for [[workload nemesis] workload-nemesis-collection]
                   (assoc cli
                          :nemesis   nemesis
                          :workload  workload
                          :test-count 1)))))
(defn all-tests
  "Turns CLI options into a sequence of tests."
  [test-fn cli]
  (if (boolean (:lightweight-run cli))
    (map test-fn (all-test-options cli (chu/cart [lightweight-workloads useful-nemesises])))
    (map test-fn (all-test-options cli (chu/cart [all-workloads all-nemesises])))))

(defn main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (.setLevel
   (LoggerFactory/getLogger "org.apache.zookeeper") Level/OFF)
  (cli/run! (merge (cli/single-test-cmd {:test-fn clickhouse-keeper-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn (partial all-tests clickhouse-keeper-test)
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
