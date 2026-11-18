(ns jepsen.clickhouse.keeper.utils
  (:require [clojure.string :as str]
            [zookeeper.data :as data]
            [zookeeper :as zk]
            [zookeeper.internal :as zi]
            [jepsen.clickhouse.constants :refer :all]
            [jepsen.clickhouse.utils :as chu]
            [clojure.tools.logging :refer :all])
  (:import (org.apache.zookeeper.data Stat)
           (org.apache.zookeeper CreateMode
                                 ZooKeeper)
           (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)))

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
  [host port timeout with-auth]
  (let [conn (zk/connect (str host ":" port) :timeout-msec timeout)]
    (if with-auth
      (do
        (zk/add-auth-info conn "digest" "clickhouse:withauth")
        conn)
      conn)))

(defn zk-create-range
  [conn n & {:keys [with-acl] :or {with-acl false}}]
  (dorun (map (fn [v] (zk/create-all conn v
                                   :persistent? true
                                   :acl (if with-acl
                                         [(zk/auth-acl :read :create :delete :admin :write)]
                                         [(zk/world-acl :read :create :delete :write :admin)])))
               (take n (zk-range)))))

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
    (if (= (chu/parse-long (:data current-value)) old-value)
      (do (zk-set conn path new-value (:version (:stat current-value)))
          true))))

(defn zk-add-to-set
  [conn path elem]
  (let [current-value (zk-get-str conn path)
        current-set (read-string (:data current-value))
        new-set (conj current-set elem)]
    (zk-set conn path (pr-str new-set) (:version (:stat current-value)))))

(defn zk-create
  [conn path data & {:keys [with-acl] :or {with-acl false}}]
  (zk/create conn path
             :data (data/to-bytes (str data))
             :persistent? true
             :acl (if with-acl
                   [(zk/auth-acl :read :create :delete :admin :write)]
                   [(zk/world-acl :read :create :delete :write :admin)])))

(defn zk-create-if-not-exists
  [conn path data & {:keys [with-acl] :or {with-acl false}}]
  (if-not (zk/exists conn path)
    (zk-create conn path data :with-acl with-acl)))

(defn zk-create-sequential
  [conn path-prefix data & {:keys [with-acl] :or {with-acl false}}]
  (zk/create conn path-prefix
             :data (data/to-bytes (str data))
             :persistent? true
             :sequential? true
             :acl (if with-acl
                   [(zk/auth-acl :read :create :delete :admin :write)]
                   [(zk/world-acl :read :create :delete :write :admin)])))

(defn zk-multi-create-many-seq-nodes
  [conn path-prefix num & {:keys [with-acl] :or {with-acl false}}]
  (let [txn (.transaction conn)
        acls (if with-acl
               [(zk/auth-acl :read :create :delete :admin :write)]
               [(zk/world-acl :read :create :delete :write :admin)])]
    (loop [i 0]
      (cond (>= i num) (.commit txn)
            :else (do (.create txn path-prefix
                               (data/to-bytes "")
                               acls
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

(defn concat-path
  [parent child]
  (str parent (if (= parent "/") "" "/") child))

(defn zk-multi-delete-first-child
  [conn path]
  (let [{children :children stat :stat} (zk-list-with-stat conn path)
        txn (.transaction conn)
        first-child (first (sort children))]
    (if (not (nil? first-child))
      (try
        (do (.check txn path (:version stat))
            (.setData txn path (data/to-bytes "") -1) ; I'm just checking multitransactions
            (.delete txn (concat-path path first-child) -1)
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
  (info "Checking Keeper alive on" node)
  (try
    (zk-connect (name node) 9181 30000 false)
    (catch Exception _ false)))

(defn start-clickhouse!
  [node test]
  (info "Starting server on node" node)
  (chu/start-clickhouse!
    node
    test
    clickhouse-alive?
    :keeper
    :--config (str configs-dir "/keeper_config.xml")
    :--
    :--logger.log (str logs-dir "/clickhouse-keeper.log")
    :--logger.errorlog (str logs-dir "/clickhouse-keeper.err.log")
    :--keeper_server.snapshot_storage_disk "snapshot_local"
    :--keeper_server.latest_snapshot_storage_disk "latest_snapshot_local"
    :--keeper_server.log_storage_disk "log_local"
    :--keeper_server.latest_log_storage_disk "latest_log_local"
    :--path coordination-data-dir))
