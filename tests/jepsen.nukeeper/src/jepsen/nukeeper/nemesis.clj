(ns jepsen.nukeeper.nemesis
  (:require
           [clojure.tools.logging :refer :all]
           [jepsen
             [nemesis :as nemesis]
             [control :as c]
             [generator :as gen]]
            [jepsen.nukeeper.constants :refer :all]
            [jepsen.nukeeper.utils :refer :all]))

(defn random-single-node-killer-nemesis
  []
  (nemesis/node-start-stopper
   rand-nth
   (fn start [test node] (kill-clickhouse! node test))
   (fn stop [test node] (start-clickhouse! node test))))

(defn hammer-time-nemesis
  []
  (nemesis/hammer-time "clickhouse"))

(defn select-last-file
  [path]
  (info "EXECUTE ON PATH" path)
  (last (clojure.string/split
         (c/exec :find path :-type :f :-printf "%T+ %p\n" :| :sort :| :awk "{print $2}")
         #"\n")))

(defn random-file-pos
  [fname]
  (let [fsize (Integer/parseInt (c/exec :du :-b fname :| :cut :-f1))]
    (rand-int fsize)))

(defn corrupt-file
  [fname]
  (info "Corrupting" fname)
  (c/exec :dd "if=/dev/zero" (str "of=" fname) "bs=1" "count=1" (str "seek=" (random-file-pos fname)) "conv=notrunc"))

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
                (let [file-to-corrupt (select-last-file path)]
                  (info "Corrupting file" file-to-corrupt)
                   (c/su
                       (corruption-op (select-last-file path))
                       (kill-clickhouse! node test)
                       (start-clickhouse! node test)))))
          (assoc op :type :info, :value :corrupted))
        :else (assoc op :type :info, :value :not-started)))

    (teardown! [this test])))

(defn logs-corruption-nemesis
  []
  (corruptor-nemesis logsdir corrupt-file))

(defn snapshots-corruption-nemesis
  []
  (corruptor-nemesis snapshotsdir corrupt-file))

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
