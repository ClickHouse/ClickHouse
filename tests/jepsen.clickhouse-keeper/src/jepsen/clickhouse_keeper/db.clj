(ns jepsen.clickhouse-keeper.db
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [db :as db]
             [util :as util :refer [meh]]]
            [jepsen.clickhouse-keeper.constants :refer :all]
            [jepsen.clickhouse-keeper.utils :refer :all]
            [clojure.java.io :as io]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]))

(defn get-clickhouse-sky
  [version]
  (c/exec :sky :get :-d common-prefix :-N :Backbone version)
  (str common-prefix "/clickhouse"))

(defn get-clickhouse-url
  [url]
  (non-precise-cached-wget! url))

(defn get-clickhouse-scp
  [path]
  (c/upload path (str common-prefix "/clickhouse")))

(defn download-clickhouse
  [source]
  (info "Downloading clickhouse from" source)
  (cond
    (clojure.string/starts-with? source "rbtorrent:") (get-clickhouse-sky source)
    (clojure.string/starts-with? source "http") (get-clickhouse-url source)
    (.exists (io/file source)) (get-clickhouse-scp source)
    :else (throw (Exception. (str "Don't know how to download clickhouse from" source)))))

(defn unpack-deb
  [path]
  (do
    (c/exec :dpkg :-x path common-prefix)
    (c/exec :rm :-f path)
    (c/exec :mv (str common-prefix "/usr/bin/clickhouse") common-prefix)
    (c/exec :rm :-rf (str common-prefix "/usr") (str common-prefix "/etc"))))

(defn unpack-tgz
  [path]
  (do
    (c/exec :mkdir :-p (str common-prefix "/unpacked"))
    (c/exec :tar :-zxvf path :-C (str common-prefix "/unpacked"))
    (c/exec :rm :-f path)
    (let [subdir (c/exec :ls (str common-prefix "/unpacked"))]
      (c/exec :mv (str common-prefix "/unpacked/" subdir "/usr/bin/clickhouse") common-prefix)
      (c/exec :rm :-fr (str common-prefix "/unpacked")))))

(defn chmod-binary
  [path]
  (info "Binary path chmod" path)
  (c/exec :chmod :+x path))

(defn install-downloaded-clickhouse
  [path]
  (cond
    (clojure.string/ends-with? path ".deb") (unpack-deb path)
    (clojure.string/ends-with? path ".tgz") (unpack-tgz path)
    (clojure.string/ends-with? path "clickhouse") (chmod-binary path)
    :else (throw (Exception. (str "Don't know how to install clickhouse from path" path)))))

(defn prepare-dirs
  []
  (do
    (c/exec :mkdir :-p common-prefix)
    (c/exec :mkdir :-p data-dir)
    (c/exec :mkdir :-p coordination-data-dir)
    (c/exec :mkdir :-p logs-dir)
    (c/exec :mkdir :-p configs-dir)
    (c/exec :mkdir :-p sub-configs-dir)
    (c/exec :touch stderr-file)
    (c/exec :chown :-R :root common-prefix)))

(defn cluster-config
  [test node config-template]
  (let [nodes (:nodes test)
        replacement-map {#"\{srv1\}" (get nodes 0)
                         #"\{srv2\}" (get nodes 1)
                         #"\{srv3\}" (get nodes 2)
                         #"\{id\}" (str (inc (.indexOf nodes node)))
                         #"\{quorum_reads\}" (str (boolean (:quorum test)))
                         #"\{snapshot_distance\}" (str (:snapshot-distance test))
                         #"\{stale_log_gap\}" (str (:stale-log-gap test))
                         #"\{reserved_log_items\}" (str (:reserved-log-items test))}]
    (reduce #(clojure.string/replace %1 (get %2 0) (get %2 1)) config-template replacement-map)))

(defn install-configs
  [test node]
  (c/exec :echo (cluster-config test node (slurp (io/resource "keeper_config.xml"))) :> (str configs-dir "/keeper_config.xml")))

(defn collect-traces
  [test node]
  (let [pid (c/exec :pidof "clickhouse")]
    (c/exec :timeout :-s "KILL" "60" :gdb :-ex "set pagination off" :-ex (str "set logging file " logs-dir "/gdb.log") :-ex
            "set logging on" :-ex "backtrace" :-ex "thread apply all backtrace"
            :-ex "backtrace" :-ex "detach" :-ex "quit" :--pid pid :|| :true)))

(defn db
  [version reuse-binary]
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (do
         (info "Preparing directories")
         (prepare-dirs)
         (if (or (not (cu/exists? binary-path)) (not reuse-binary))
           (do (info "Downloading clickhouse")
               (install-downloaded-clickhouse (download-clickhouse version)))
           (info "Binary already exsist on path" binary-path "skipping download"))
         (info "Installing configs")
         (install-configs test node)
         (info "Starting server")
         (start-clickhouse! node test)
         (info "ClickHouse started"))))

    (teardown! [_ test node]
      (info node "Tearing down clickhouse")
      (c/su
       (kill-clickhouse! node test)
       (if (not reuse-binary)
         (c/exec :rm :-rf binary-path))
       (c/exec :rm :-rf pid-file-path)
       (c/exec :rm :-rf data-dir)
       (c/exec :rm :-rf logs-dir)
       (c/exec :rm :-rf configs-dir)))

    db/LogFiles
    (log-files [_ test node]
      (c/su
       ;(if (cu/exists? pid-file-path)
         ;(do
         ;  (info node "Collecting traces")
         ;  (collect-traces test node))
         ;(info node "Pid files doesn't exists"))
       (kill-clickhouse! node test)
       (if (cu/exists? coordination-data-dir)
         (do
           (info node "Coordination files exists, going to compress")
           (c/cd data-dir
                 (c/exec :tar :czf "coordination.tar.gz" "coordination")))))
      (let [common-logs [stderr-file (str logs-dir "/clickhouse-keeper.log") (str data-dir "/coordination.tar.gz")]
            gdb-log (str logs-dir "/gdb.log")]
        (if (cu/exists? (str logs-dir "/gdb.log"))
          (conj common-logs gdb-log)
          common-logs)))))
