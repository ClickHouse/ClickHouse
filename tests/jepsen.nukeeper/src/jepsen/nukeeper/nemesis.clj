(ns jepsen.nukeeper.nemesis
  (:require [jepsen
             [nemesis :as nemesis]]
            [jepsen.nukeeper.utils :refer :all]))



(defn random-single-node-killer-nemesis
  []
  (nemesis/node-start-stopper
    rand-nth
    (fn start [test node] (kill-clickhouse! node test))
    (fn stop [test node] (start-clickhouse! node test))))
