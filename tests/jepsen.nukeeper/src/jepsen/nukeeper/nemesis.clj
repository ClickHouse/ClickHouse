(ns jepsen.nukeeper.nemesis
  (:require [jepsen
             [nemesis :as nemesis]
             [generator :as gen]]
            [jepsen.nukeeper.utils :refer :all]))

(defn random-single-node-killer-nemesis
  []
  (nemesis/node-start-stopper
   rand-nth
   (fn start [test node] (kill-clickhouse! node test))
   (fn stop [test node] (start-clickhouse! node test))))

(def custom-nemesises
  {"killer" {:nemesis (random-single-node-killer-nemesis)
             :generator
             (gen/nemesis
              (cycle [(gen/sleep 5)
                      {:type :info, :f :start}
                      (gen/sleep 5)
                      {:type :info, :f :stop}]))}
   "simple-partitioner" {:nemesis (nemesis/partition-random-halves)
                         :generator
                         (gen/nemesis
                          (cycle [(gen/sleep 5)
                                  {:type :info, :f :start}
                                  (gen/sleep 5)
                                  {:type :info, :f :stop}]))}})
