(ns jepsen.clickhouse.keeper.bench
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [client :as client]])
  (:import (java.lang ProcessBuilder)
           (java.lang ProcessBuilder$Redirect)))

(defn exec-process-builder
  [command & args]
  (let [pbuilder (ProcessBuilder. (into-array (cons command args)))]
    (.redirectOutput pbuilder ProcessBuilder$Redirect/INHERIT)
    (.redirectError pbuilder ProcessBuilder$Redirect/INHERIT)
    (let [p (.start pbuilder)]
      (.waitFor p))))

(defrecord BenchClient [port]
  client/Client
  (open! [this test node]
    this)

  (setup! [this test]
    this)

  (invoke! [this test op]
    (let [bench-opts (into [] (clojure.string/split (:bench-opts op) #" "))
          bench-path (:bench-path op)
          nodes (into [] (flatten (map (fn [x] (identity ["-h" (str x ":" port)])) (:nodes test))))
          all-args (concat [bench-path] bench-opts nodes)]
      (info "Running cmd" all-args)
      (apply exec-process-builder all-args)
      (assoc op :type :ok :value "ok")))

  (teardown! [_ test])

  (close! [_ test]))

(defn bench-client
  [port]
  (BenchClient. port))
