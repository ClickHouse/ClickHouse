(ns jepsen.clickhouse.server.nemesis
  (:require [jepsen.clickhouse.nemesis :as chnem]))

(def custom-nemesises
  {"random-node-hammer-time"  {:nemesis (chnem/random-node-hammer-time-nemesis)
                                 :generator (chnem/start-stop-generator 5 5)}
   "all-nodes-hammer-time"    {:nemesis (chnem/all-nodes-hammer-time-nemesis)
                               :generator (chnem/start-stop-generator 1 10)}})
