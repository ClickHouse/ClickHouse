(ns jepsen.clickhouse.nemesis
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen.clickhouse.utils :as chu]
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

(defn random-node-killer-nemesis
  [start-clickhouse!]
  (nemesis/node-start-stopper
   rand-nth
   (fn start [test node] (chu/kill-clickhouse! node test))
   (fn stop [test node] (start-clickhouse! node test))))

(defn all-nodes-killer-nemesis
  [start-clickhouse!]
  (nemesis/node-start-stopper
   identity
   (fn start [test node] (chu/kill-clickhouse! node test))
   (fn stop [test node] (start-clickhouse! node test))))

(defn partition-bridge-nemesis
  []
  (nemesis/partitioner nemesis/bridge))

(defn blind-node
  [nodes]
  (let [[[victim] others] (nemesis/split-one nodes)]
    {victim (into #{} others)}))

(defn blind-node-partition-nemesis
  []
  (nemesis/partitioner blind-node))

(defn blind-others
  [nodes]
  (let [[[victim] others] (nemesis/split-one nodes)]
    (into {} (map (fn [node] [node #{victim}])) others)))

(defn blind-others-partition-nemesis
  []
  (nemesis/partitioner blind-others))
