(ns jepsen.nukeeper.main
  (:require [clojure.tools.logging :refer :all]
            [jepsen.nukeeper.utils :refer :all]
            [jepsen.nukeeper.set :as set]
            [jepsen.nukeeper.nemesis :as custom-nemesis]
            [jepsen.nukeeper.register :as register]
            [jepsen.nukeeper.constants :refer :all]
            [clojure.string :as str]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
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
            [knossos.model :as model]
            [zookeeper.data :as data]
            [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)))

(defn cluster-config
  [test node config-template]
  (let [nodes (:nodes test)
        replacement-map {#"\{srv1\}" (get nodes 0)
                         #"\{srv2\}" (get nodes 1)
                         #"\{srv3\}" (get nodes 2)
                         #"\{id\}" (str (inc (.indexOf nodes node)))
                         #"\{quorum_reads\}" (str (boolean (:quorum test)))
                         #"\{snapshot_distance\}" (str (:snapshot-distance test))
                         #"\{stale_log_gap\}" (str (:stale-log-gap test))
                         #"\{reserved_log_items\}" (str (:reserved-log-items test))}]
    (reduce #(clojure.string/replace %1 (get %2 0) (get %2 1)) config-template replacement-map)))

(defn db
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing clickhouse" version)
      (c/su
       (if-not (cu/exists? (str binary-path "/clickhouse"))
         (c/exec :sky :get :-d binary-path :-N :Backbone version))
       (c/exec :mkdir :-p logdir)
       (c/exec :touch logfile)
       (c/exec (str binary-path "/clickhouse") :install)
       (c/exec :chown :-R :root dir)
       (c/exec :chown :-R :root logdir)
       (c/exec :echo (slurp (io/resource "listen.xml")) :> "/etc/clickhouse-server/config.d/listen.xml")
       (c/exec :echo (cluster-config test node (slurp (io/resource "test_keeper_config.xml"))) :> "/etc/clickhouse-server/config.d/test_keeper_config.xml")
       (cu/start-daemon!
        {:pidfile pidfile
         :logfile logfile
         :chdir dir}
        (str binary-path "/clickhouse")
        :server
        :--config "/etc/clickhouse-server/config.xml")
       (wait-clickhouse-alive! node test)))

    (teardown! [_ test node]
      (info node "tearing down clickhouse")
      (cu/stop-daemon! (str binary-path "/clickhouse") pidfile)
      (c/su
       ;(c/exec :rm :-f (str binary-path "/clickhouse"))
       (c/exec :rm :-rf dir)
       (c/exec :rm :-rf logdir)
       (c/exec :rm :-rf "/etc/clickhouse-server")))

    db/LogFiles
    (log-files [_ test node]
      (c/su
       (cu/stop-daemon! (str binary-path "/clickhouse") pidfile)
       (c/cd dir
             (c/exec :tar :czf "coordination.tar.gz" "coordination")))
      [logfile serverlog (str dir "/coordination.tar.gz")])))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
  {"set"      set/workload
   "register" register/workload})

(def cli-opts
  "Additional command line options."
  [["-w" "--workload NAME" "What workload should we run?"
    :missing  (str "--workload " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   [nil "--nemesis NAME" "Which nemesis will poison our lives?"
    :missing  (str "--nemesis " (cli/one-of custom-nemesis/custom-nemesises))
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
   [nil "--reserved-log-items NUM" "Number of log entries to keep after snapshot"
    :default 1000
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]])

(defn nukeeper-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [quorum (boolean (:quorum opts))
        workload  ((get workloads (:workload opts)) opts)
        current-nemesis (get custom-nemesis/custom-nemesises (:nemesis opts))]
    (merge tests/noop-test
           opts
           {:name (str "clickhouse-keeper quorum=" quorum " "  (name (:workload opts)) (name (:nemesis opts)))
            :os ubuntu/os
            :db (db "rbtorrent:a122093aee0bdcb70ca42d5e5fb4ba5544372f5f")
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

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn nukeeper-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
