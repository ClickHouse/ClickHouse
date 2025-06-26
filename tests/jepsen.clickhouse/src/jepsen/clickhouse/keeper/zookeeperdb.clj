(ns jepsen.clickhouse.keeper.zookeeperdb
  (:require [clojure.tools.logging :refer :all]
            [jepsen.clickhouse.keeper.utils :refer :all]
            [clojure.java.io :as io]
            [jepsen
             [control :as c]
             [db :as db]]
            [jepsen.os.ubuntu :as ubuntu]))

(defn zk-node-ids
  "Returns a map of node names to node ids."
  [test]
  (->> test
       :nodes
       (map-indexed (fn [i node] [node (inc i)]))
       (into {})))

(defn zk-node-id
  "Given a test and a node name from that test, returns the ID for that node."
  [test node]
  ((zk-node-ids test) node))

(defn zoo-cfg-servers
  "Constructs a zoo.cfg fragment for servers."
  [test mynode]
  (->> (zk-node-ids test)
       (map (fn [[node id]]
              (str "server." id "=" (if (= (name node) mynode) "0.0.0.0" (name node)) ":2888:3888")))
       (clojure.string/join "\n")))

(defn zookeeper-db
  "Zookeeper DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (info node "Installing ZK" version)
       (c/exec :apt-get :update)
       (c/exec :apt-get :install (str "zookeeper=" version))
       (c/exec :apt-get :install (str "zookeeperd=" version))
       (c/exec :echo (zk-node-id test node) :> "/etc/zookeeper/conf/myid")

       (c/exec :echo (str (slurp (io/resource "zoo.cfg"))
                          "\n"
                          (zoo-cfg-servers test node))
               :> "/etc/zookeeper/conf/zoo.cfg")

       (info node "ZK restarting")
       (c/exec :service :zookeeper :restart)
       (info "Connecting to zk" (name node))
       (zk-connect (name node) 2181 1000 false)
       (info node "ZK ready")))

    (teardown! [_ test node]
      (info node "tearing down ZK")
      (c/su
       (c/exec :service :zookeeper :stop :|| true)
       (c/exec :rm :-rf
               (c/lit "/var/lib/zookeeper/version-*")
               (c/lit "/var/log/zookeeper/*"))))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/zookeeper/zookeeper.log"])))
