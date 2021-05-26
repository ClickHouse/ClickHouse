(ns jepsen.clickhouse-keeper.nemesis
  (:require
   [clojure.tools.logging :refer :all]
   [jepsen
    [nemesis :as nemesis]
    [control :as c]
    [generator :as gen]]
   [jepsen.clickhouse-keeper.constants :refer :all]
   [jepsen.clickhouse-keeper.utils :refer :all]))

(defn random-node-killer-nemesis
  []
  (nemesis/node-start-stopper
   rand-nth
   (fn start [test node] (kill-clickhouse! node test))
   (fn stop [test node] (start-clickhouse! node test))))

(defn all-nodes-killer-nemesis
  []
  (nemesis/node-start-stopper
   identity
   (fn start [test node] (kill-clickhouse! node test))
   (fn stop [test node] (start-clickhouse! node test))))

(defn random-node-hammer-time-nemesis
  []
  (nemesis/hammer-time "clickhouse"))

(defn all-nodes-hammer-time-nemesis
  []
  (nemesis/hammer-time identity "clickhouse"))

(defn select-last-file
  [path]
  (last (clojure.string/split
         (c/exec :find path :-type :f :-printf "%T+ %p\n" :| :grep :-v :tmp_ :| :sort :| :awk "{print $2}")
         #"\n")))

(defn random-file-pos
  [fname]
  (let [fsize (Integer/parseInt (c/exec :du :-b fname :| :cut :-f1))]
    (rand-int fsize)))

(defn corrupt-file
  [fname]
  (if (not (empty? fname))
    (do
      (info "Corrupting" fname)
      (c/exec :dd "if=/dev/zero" (str "of=" fname) "bs=1" "count=1" (str "seek=" (random-file-pos fname)) "conv=notrunc"))
    (info "Nothing to corrupt")))

(defn corruptor-nemesis
  [path corruption-op]
  (reify nemesis/Nemesis

    (setup! [this test] this)

    (invoke! [this test op]
      (cond (= (:f op) :corrupt)
            (let [nodes (list (rand-nth (:nodes test)))]
              (info "Corruption on node" nodes)
              (c/on-nodes test nodes
                          (fn [test node]
                            (c/su
                             (kill-clickhouse! node test)
                             (corruption-op path)
                             (start-clickhouse! node test))))
              (assoc op :type :info, :value :corrupted))
            :else (do (c/on-nodes test (:nodes test)
                                  (fn [test node]
                                    (c/su
                                     (start-clickhouse! node test))))
                      (assoc op :type :info, :value :done))))

    (teardown! [this test])))

(defn logs-corruption-nemesis
  []
  (corruptor-nemesis coordination-logs-dir #(corrupt-file (select-last-file %1))))

(defn snapshots-corruption-nemesis
  []
  (corruptor-nemesis coordination-snapshots-dir #(corrupt-file (select-last-file %1))))

(defn logs-and-snapshots-corruption-nemesis
  []
  (corruptor-nemesis coordination-data-dir (fn [path]
                                             (do
                                               (corrupt-file (select-last-file (str path "/snapshots")))
                                               (corrupt-file (select-last-file (str path "/logs")))))))
(defn drop-all-corruption-nemesis
  []
  (corruptor-nemesis coordination-data-dir (fn [path]
                                             (c/exec :rm :-fr path))))

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

(defn network-non-symmetric-nemesis
  []
  (nemesis/partitioner nemesis/bridge))

(defn start-stop-generator
  [time-corrupt time-ok]
  (->>
   (cycle [(gen/sleep time-ok)
           {:type :info, :f :start}
           (gen/sleep time-corrupt)
           {:type :info, :f :stop}])))

(defn corruption-generator
  []
  (->>
   (cycle [(gen/sleep 5)
           {:type :info, :f :corrupt}])))

(def custom-nemesises
  {"random-node-killer" {:nemesis (random-node-killer-nemesis)
                         :generator (start-stop-generator 5 5)}
   "all-nodes-killer" {:nemesis (all-nodes-killer-nemesis)
                       :generator (start-stop-generator 1 10)}
   "simple-partitioner" {:nemesis (nemesis/partition-random-halves)
                         :generator (start-stop-generator 5 5)}
   "random-node-hammer-time"    {:nemesis (random-node-hammer-time-nemesis)
                                 :generator (start-stop-generator 5 5)}
   "all-nodes-hammer-time"    {:nemesis (all-nodes-hammer-time-nemesis)
                               :generator (start-stop-generator 1 10)}
   "logs-corruptor" {:nemesis (logs-corruption-nemesis)
                     :generator (corruption-generator)}
   "snapshots-corruptor" {:nemesis (snapshots-corruption-nemesis)
                          :generator (corruption-generator)}
   "logs-and-snapshots-corruptor" {:nemesis (logs-and-snapshots-corruption-nemesis)
                                   :generator (corruption-generator)}
   "drop-data-corruptor" {:nemesis (drop-all-corruption-nemesis)
                          :generator (corruption-generator)}
   "bridge-partitioner" {:nemesis (partition-bridge-nemesis)
                         :generator (start-stop-generator 5 5)}
   "blind-node-partitioner" {:nemesis (blind-node-partition-nemesis)
                             :generator (start-stop-generator 5 5)}
   "blind-others-partitioner" {:nemesis (blind-others-partition-nemesis)
                               :generator (start-stop-generator 5 5)}})
