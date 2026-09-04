(ns jepsen.clickhouse.server.main
  (:require [clojure.tools.logging :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [generator :as gen]
             [tests :as tests]
             [util :as util :refer [meh]]]
            [jepsen.clickhouse.server
             [db :refer :all]
             [nemesis :as ch-nemesis]]
            [jepsen.clickhouse.server
             [set :as set]]
            [jepsen.clickhouse.utils :as chu]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.checker.timeline :as timeline]
            [clojure.java.io :as io])
  (:import (ch.qos.logback.classic Level)
           (org.slf4j Logger LoggerFactory)))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
   {"set" set/workload})

(def cli-opts
  "Additional command line options."
  [["-w" "--workload NAME" "What workload should we run?"
    :default "set"
    :validate [workloads (cli/one-of workloads)]]
   [nil "--keeper ADDRESS", "Address of a Keeper instance"
    :default ""
    :validate [#(not-empty %) "Address for Keeper cannot be empty"]]
   [nil "--nemesis NAME" "Which nemesis will poison our lives?"
    :default "random-node-killer"
    :validate [ch-nemesis/custom-nemeses (cli/one-of ch-nemesis/custom-nemeses)]]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil, "--reuse-binary" "Use already downloaded binary if it exists, don't remove it on shutdown"]
   ["-c" "--clickhouse-source URL" "URL for clickhouse deb or tgz package"]])

(defn get-db
  [opts]
  (db (:clickhouse-source opts) (boolean (:reuse-binary opts))))

(defn clickhouse-func-tests
  [opts]
  (info "Test opts\n" (with-out-str (pprint opts)))
  (let [quorum (boolean (:quorum opts))
        workload  ((get workloads (:workload opts)) opts)
        current-nemesis (get ch-nemesis/custom-nemeses (:nemesis opts))]
    (merge tests/noop-test
           opts
           {:name (str "clickhouse-server-"  (name (:workload opts)) "-" (name (:nemesis opts)))
            :os ubuntu/os
            :db (get-db opts)
            :pure-generators true
            :nemesis (:nemesis current-nemesis)
            :client (:client workload)
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

(defn clickhouse-server-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (clickhouse-func-tests opts))

(def all-workloads (keys workloads))

(def all-nemeses (keys ch-nemesis/custom-nemeses))

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
  (map test-fn (all-test-options cli (chu/cart [all-workloads all-nemeses]))))

(defn main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (.setLevel
   (LoggerFactory/getLogger "org.apache.zookeeper") Level/OFF)
  (cli/run! (merge (cli/single-test-cmd {:test-fn clickhouse-server-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn (partial all-tests clickhouse-server-test)
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
