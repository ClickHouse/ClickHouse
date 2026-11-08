(ns jepsen.clickhouse.keeper.db
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [util :as util :refer [meh]]]
            [jepsen.clickhouse.constants :refer :all]
            [jepsen.clickhouse.keeper.utils :refer :all]
            [jepsen.clickhouse.utils :as chu]
            [clojure.java.io :as io]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]))


(ns jepsen.control.scp)

;; We need to overwrite Jepsen's implementation of scp! because it
;; doesn't use strict-host-key-checking

(defn scp!
  "Runs an SCP command by shelling out. Takes a conn-spec (used for port, key,
  etc), a seq of sources, and a single destination, all as strings."
  [conn-spec sources dest]
  (apply util/sh "scp" "-rpC"
         "-P" (str (:port conn-spec))
         (concat (when-let [k (:private-key-path conn-spec)]
                   ["-i" k])
                 (if-not (:strict-host-key-checking conn-spec)
                   ["-o StrictHostKeyChecking=no"])
                 sources
                 [dest]))
  nil)

(ns jepsen.clickhouse.keeper.db)

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

(defn extra-setup
  [test node]
  (do
    (info "Installing configs")
    (install-configs test node)))

(defn db
  [version reuse-binary]
  (chu/db version reuse-binary start-clickhouse! extra-setup))
