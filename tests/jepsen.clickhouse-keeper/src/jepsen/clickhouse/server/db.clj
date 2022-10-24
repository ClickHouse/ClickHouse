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

(ns jepsen.clickhouse.server.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [jepsen
             [control :as c]
             [db :as db]]
            [jepsen.clickhouse.constants :refer :all]
            [jepsen.clickhouse.server.utils :refer :all]
            [jepsen.clickhouse.utils :as chu]))

(defn install-configs
  [test node]
  (c/exec :echo (slurp (io/resource "config.xml")) :> (str configs-dir "/config.xml"))
  (c/exec :echo (slurp (io/resource "users.xml")) :> (str configs-dir "/users.xml")))

(defn extra-setup
  [test node]
  (do
    (info "Installing configs")
    (install-configs test node)))

(defn db
  [version reuse-binary]
  (chu/db version reuse-binary start-clickhouse! extra-setup))
