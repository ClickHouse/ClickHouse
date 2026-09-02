(ns jepsen.keeper-test
  (:require [clojure.test :refer :all]
            [jepsen.clickhouse-keeper.utils :refer :all]
            [zookeeper :as zk]
            [zookeeper.data :as data])
  (:import (ch.qos.logback.classic Level)
           (org.slf4j Logger LoggerFactory)))

(defn multicreate
  [conn]
  (dorun (map (fn [v] (zk/create conn v :persistent? true)) (take 10 (zk-range)))))

(defn multidelete
  [conn]
  (dorun (map (fn [v] (zk/delete conn v)) (take 10 (zk-range)))))

(deftest a-test
  (testing "keeper connection"
    (.setLevel
     (LoggerFactory/getLogger "org.apache.zookeeper") Level/OFF)
    (let [conn (zk/connect "localhost:9181" :timeout-msec 5000)]
      ;(println (take 10 (zk-range)))
      ;(multidelete conn)
      ;(multicreate conn)
      ;(zk/create-all conn "/0")
      ;(zk/create conn "/0")
      ;(println (zk/children conn "/"))
      ;(zk/set-data conn "/0" (data/to-bytes "777") -1)
      (println (zk-parent-path "/sasds/dasda/das"))
      (println (zk-parent-path "/sasds"))
      (zk-multi-create-many-seq-nodes conn "/a-" 5)
      (println (zk/children conn "/"))
      (println (zk-list-with-stat conn "/"))
      (println (zk-multi-delete-first-child conn "/"))
      (println (zk-list-with-stat conn "/"))
      ;(Thread/sleep 5000)
      ;(println "VALUE" (data/to-string (:data (zk/data conn "/0"))))
      ;(is (= (data/to-string (:data (zk/data conn "/0"))) "777"))
      (zk/close conn))))
