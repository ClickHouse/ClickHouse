(ns jepsen.nukeeper.db
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [db :as db]
             [util :as util :refer [meh]]]
            [jepsen.nukeeper.constants :refer :all]
            [jepsen.nukeeper.utils :refer :all]
            [clojure.java.io :as io]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]))

(defn get-clickhouse-sky
  [version]
  (c/exec :sky :get :-d common-prefix :-N :Backbone version))

(defn get-clickhouse-url
  [url]
  (let [download-result (cu/wget! url)]
    (do (c/exec :mv download-result common-prefix)
        (str common-prefix "/" download-result))))

(defn unpack-deb
  [path]
  (do
  (c/exec :dpkg :-x path :.)
  (c/exec :mv "usr/bin/clickhouse" common-prefix)))

(defn unpack-tgz
  [path]
  (do
  (c/exec :tar :-zxvf path :.)
  (c/exec :mv "usr/bin/clickhouse" common-prefix)))

(defn prepare-dirs
  []
  (do
    (c/exec :rm :-rf common-prefix)
    (c/exec :mkdir :-p common-prefix)
    (c/exec :mkdir :-p data-dir)
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
     (c/exec :echo (slurp (io/resource "config.xml")) :> (str configs-dir "/config.xml"))
     (c/exec :echo (slurp (io/resource "users.xml")) :> (str configs-dir "/users.xml"))
     (c/exec :echo (slurp (io/resource "listen.xml")) :> (str sub-configs-dir "/listen.xml"))
     (c/exec :echo (cluster-config test node (slurp (io/resource "test_keeper_config.xml"))) :> (str sub-configs-dir "/test_keeper_config.xml")))

(defn db
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (do
       (info "Preparing directories")
       (prepare-dirs)
       (info "Downloading clickhouse")
       (get-clickhouse-sky version)
       (info "Installing configs")
       (install-configs test node)
       (info "Starting server")
       (start-clickhouse! node test)
      (info "ClickHouse started"))))


    (teardown! [_ test node]
      (info node "Tearing down clickhouse")
      (kill-clickhouse! node test)
      (c/su
       ;(c/exec :rm :-f binary-path)
       (c/exec :rm :-rf data-dir)
       (c/exec :rm :-rf logs-dir)
       (c/exec :rm :-rf configs-dir)))

    db/LogFiles
    (log-files [_ test node]
      (c/su
       (kill-clickhouse! node test)
       (c/cd data-dir
        (c/exec :tar :czf "coordination.tar.gz" "coordination")))
      [stderr-file (str logs-dir "/clickhouse-server.log") (str data-dir "/coordination.tar.gz")])))
