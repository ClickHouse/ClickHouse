(ns jepsen.clickhouse-keeper.utils
  (:require [clojure.string :as str]
            [zookeeper.data :as data]
            [zookeeper :as zk]
            [zookeeper.internal :as zi]
            [jepsen.control.util :as cu]
            [jepsen.clickhouse-keeper.constants :refer :all]
            [jepsen.control :as c]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io])
  (:import (org.apache.zookeeper.data Stat)
           (org.apache.zookeeper CreateMode
                                 ZooKeeper)
           (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)
           (java.security MessageDigest)))

(defn exec-with-retries
  [retries f & args]
  (let [res (try {:value (apply f args)}
                 (catch Exception e
                   (if (zero? retries)
                     (throw e)
                     {:exception e})))]
    (if (:exception res)
      (do (Thread/sleep 1000) (recur (dec retries) f args))
      (:value res))))

(defn parse-long
  "Parses a string to a Long. Passes through `nil` and empty strings."
  [s]
  (if (and s (> (count s) 0))
    (Long/parseLong s)))

(defn parse-and-get-counter
  [path]
  (Integer/parseInt (apply str (take-last 10 (seq (str path))))))

(defn zk-range
  []
  (map (fn [v] (str "/" v)) (range)))

(defn zk-path
  [n]
  (str "/" n))

(defn zk-connect
  [host port timeout]
  (exec-with-retries 30 (fn [] (zk/connect (str host ":" port) :timeout-msec timeout))))

(defn zk-create-range
  [conn n]
  (dorun (map (fn [v] (zk/create-all conn v :persistent? true)) (take n (zk-range)))))

(defn zk-set
  ([conn path value]
   (zk/set-data conn path (data/to-bytes (str value)) -1))
  ([conn path value version]
   (zk/set-data conn path (data/to-bytes (str value)) version)))

(defn zk-get-str
  [conn path]
  (let [zk-result (zk/data conn path)]
    {:data (data/to-string (:data zk-result))
     :stat (:stat zk-result)}))

(defn zk-list
  [conn path]
  (zk/children conn path))

(defn zk-list-with-stat
  [conn path]
  (let [stat (new Stat)
        children (seq (.getChildren conn path false stat))]
    {:children children
     :stat (zi/stat-to-map stat)}))

(defn zk-cas
  [conn path old-value new-value]
  (let [current-value (zk-get-str conn path)]
    (if (= (parse-long (:data current-value)) old-value)
      (do (zk-set conn path new-value (:version (:stat current-value)))
          true))))

(defn zk-add-to-set
  [conn path elem]
  (let [current-value (zk-get-str conn path)
        current-set (read-string (:data current-value))
        new-set (conj current-set elem)]
    (zk-set conn path (pr-str new-set) (:version (:stat current-value)))))

(defn zk-create-if-not-exists
  [conn path data]
  (zk/create conn path :data (data/to-bytes (str data)) :persistent? true))

(defn zk-create-sequential
  [conn path-prefix data]
  (zk/create conn path-prefix :data (data/to-bytes (str data)) :persistent? true :sequential? true))

(defn zk-multi-create-many-seq-nodes
  [conn path-prefix num]
  (let [txn (.transaction conn)]
    (loop [i 0]
      (cond (>= i num) (.commit txn)
            :else (do (.create txn path-prefix
                               (data/to-bytes "")
                               (zi/acls :open-acl-unsafe)
                               CreateMode/PERSISTENT_SEQUENTIAL)
                      (recur (inc i)))))))

; sync call not implemented in zookeeper-clj and don't have sync version in java API
(defn zk-sync
  [conn]
  (zk-set conn "/" "" -1))

(defn zk-parent-path
  [path]
  (let [rslash_pos (str/last-index-of path "/")]
    (if (> rslash_pos 0)
      (subs path 0 rslash_pos)
      "/")))

(defn zk-multi-delete-first-child
  [conn path]
  (let [{children :children stat :stat} (zk-list-with-stat conn path)
        txn (.transaction conn)
        first-child (first (sort children))]
    (if (not (nil? first-child))
      (try
        (do (.check txn path (:version stat))
            (.setData txn path (data/to-bytes "") -1) ; I'm just checking multitransactions
            (.delete txn (str path first-child) -1)
            (.commit txn)
            first-child)
        (catch KeeperException$BadVersionException _ nil)
        ; Even if we got connection loss, delete may actually be executed.
        ; This function is used for queue model, which strictly require
        ; all enqueued elements to be dequeued, but allow duplicates.
        ; So even in case when we not sure about delete we return first-child.
        (catch Exception _ first-child))
      nil)))

(defn clickhouse-alive?
  [node test]
  (info "Checking server alive on" node)
  (try
    (zk-connect (name node) 9181 30000)
    (catch Exception _ false)))

(defn wait-clickhouse-alive!
  [node test & {:keys [maxtries] :or {maxtries 30}}]
  (loop [i 0]
    (cond (> i maxtries) false
          (clickhouse-alive? node test) true
          :else (do (Thread/sleep 1000) (recur (inc i))))))

(defn kill-clickhouse!
  [node test]
  (info "Killing server on node" node)
  (c/su
   (cu/stop-daemon! binary-path pid-file-path)
   (c/exec :rm :-fr (str data-dir "/status"))))

(defn start-clickhouse!
  [node test]
  (info "Starting server on node" node)
  (c/su
   (cu/start-daemon!
    {:pidfile pid-file-path
     :logfile stderr-file
     :chdir data-dir}
    binary-path
    :keeper
    :--config (str configs-dir "/keeper_config.xml")
    :--
    :--logger.log (str logs-dir "/clickhouse-keeper.log")
    :--logger.errorlog (str logs-dir "/clickhouse-keeper.err.log")
    :--keeper_server.snapshot_storage_path coordination-snapshots-dir
    :--keeper_server.log_storage_path coordination-logs-dir)
   (wait-clickhouse-alive! node test)))

(defn md5 [^String s]
  (let [algorithm (MessageDigest/getInstance "MD5")
        raw (.digest algorithm (.getBytes s))]
    (format "%032x" (BigInteger. 1 raw))))

(defn non-precise-cached-wget!
  [url]
  (let [encoded-url (md5 url)
        expected-file-name (.getName (io/file url))
        dest-file (str binaries-cache-dir "/" encoded-url)
        dest-symlink (str common-prefix "/" expected-file-name)
        wget-opts (concat cu/std-wget-opts [:-O dest-file])]
    (when-not (cu/exists? dest-file)
      (info "Downloading" url)
      (do (c/exec :mkdir :-p binaries-cache-dir)
          (c/cd binaries-cache-dir
                (cu/wget-helper! wget-opts url))))
    (c/exec :rm :-rf dest-symlink)
    (c/exec :ln :-s dest-file dest-symlink)
    dest-symlink))
