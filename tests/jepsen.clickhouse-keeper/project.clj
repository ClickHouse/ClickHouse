(defproject jepsen.keeper "0.1.0-SNAPSHOT"
  :injections [(.. System (setProperty "zookeeper.request.timeout" "10000"))]
  :description "A jepsen tests for ClickHouse Keeper"
  :url "https://clickhouse.tech/"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main jepsen.clickhouse-keeper.main
  :plugins [[lein-cljfmt "0.7.0"]]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [jepsen "0.2.3"]
                 [zookeeper-clj "0.9.4"]
                 [org.apache.zookeeper/zookeeper "3.6.1" :exclusions [org.slf4j/slf4j-log4j12]]]
  :repl-options {:init-ns jepsen.clickhouse-keeper.main})
