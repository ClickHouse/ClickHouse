(defproject jepsen.clickhouse "0.1.0-SNAPSHOT"
  :injections [(.. System (setProperty "zookeeper.request.timeout" "10000"))]
  :description "A jepsen tests for ClickHouse"
  :url "https://clickhouse.com/"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main jepsen.clickhouse.main
  :plugins [[lein-cljfmt "0.7.0"]]
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [jepsen "0.3.11" :exclusions [net.java.dev.jna/jna
                                                net.java.dev.jna/jna-platform]]
                 [zookeeper-clj "0.9.4"]
                 [org.clojure/java.jdbc "0.7.12"]
                 [com.hierynomus/sshj "0.40.0"
                  :exclusions [org.slf4j/slf4j-api
                               org.bouncycastle/bcutil-jdk18on]]
                 [org.slf4j/slf4j-api "2.0.17"]
                 [net.java.dev.jna/jna "5.14.0"]
                 [net.java.dev.jna/jna-platform "5.14.0"]
                 [com.clickhouse/clickhouse-jdbc "0.3.2-patch11"]
                 [org.apache.zookeeper/zookeeper "3.6.1"
                  :exclusions [org.slf4j/slf4j-log4j12
                               org.slf4j/slf4j-api]]]
  :repl-options {:init-ns jepsen.clickhouse-keeper.main}
  ;; otherwise, target artifacts will be created under the repo root, so that checkout with clear might fail in ci
  :target-path "/tmp/jepsen_clickhouse"
)
