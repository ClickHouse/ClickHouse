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
            [jepsen.clickhouse.server.db :refer :all]
            [jepsen.clickhouse.server
             [register :as register]
             [set :as set]]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.checker.timeline :as timeline]
            [clojure.java.io :as io]))

(def workloads
  "A map of workload names to functions that construct workloads, given opts."
   {"register" register/workload
    "set" set/workload})

(def custom-nemesises
  {})

(def cli-opts
  "Additional command line options."
  [["-w" "--workload NAME" "What workload should we run?"
    :default "set"
    :validate [workloads (cli/one-of workloads)]]
   ;[nil "--nemesis NAME" "Which nemesis will poison our lives?"
   ; :default "random-node-killer"
   ; :validate [custom-nemesises (cli/one-of custom-nemesises)]]
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
        workload  ((get workloads (:workload opts)) opts)]
    (merge tests/noop-test
           opts
           {:name (str "clickhouse-server-"  (name (:workload opts)))
            :os ubuntu/os
            :db (get-db opts)
            :pure-generators true
            :client (:client workload)
            :checker (checker/compose
                      {:perf     (checker/perf)
                       :workload (:checker workload)})
            :generator (gen/phases
                        (->> (:generator workload)
                             (gen/stagger (/ (:rate opts)))
                             (gen/time-limit (:time-limit opts)))
                        (gen/clients (:final-generator workload)))})))

(defn clickhouse-server-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (clickhouse-func-tests opts))

(def all-workloads (keys workloads))

(defn all-test-options
  "Takes base cli options, a collection of nemeses, workloads, and a test count,
  and constructs a sequence of test options."
  [cli]
  (take (:test-count cli)
        (shuffle (for [[workload] all-workloads]
                   (assoc cli
                          :workload  workload
                          :test-count 1)))))

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [test-fn cli]
  (map test-fn (all-test-options cli)))

(defn main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn clickhouse-server-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn (partial all-tests clickhouse-server-test)
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
