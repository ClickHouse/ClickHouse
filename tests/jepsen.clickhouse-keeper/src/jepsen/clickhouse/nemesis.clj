(ns jepsen.clickhouse.nemesis
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen
    [nemesis :as nemesis]
    [generator :as gen]]))

(defn random-node-hammer-time-nemesis
  []
  (nemesis/hammer-time "clickhouse"))

(defn all-nodes-hammer-time-nemesis
  []
  (nemesis/hammer-time identity "clickhouse"))

(defn start-stop-generator
  [time-corrupt time-ok]
  (->>
   (cycle [(gen/sleep time-ok)
           {:type :info, :f :start}
           (gen/sleep time-corrupt)
           {:type :info, :f :stop}])))

