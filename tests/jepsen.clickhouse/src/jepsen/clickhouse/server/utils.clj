(ns jepsen.clickhouse.server.utils
  (:require [jepsen.clickhouse.utils :as chu]
            [jepsen.clickhouse.constants :refer :all]
            [jepsen.clickhouse.server.client :as chc]
            [clojure.tools.logging :refer :all]
            [clojure.java.jdbc :as jdbc]))

(defn clickhouse-alive?
  [node test]
  (try
    (let [c (chc/open-connection node)]
      (jdbc/query c "SELECT 1")
      (chc/close-connection c))
    (catch Exception e false)))

(defn start-clickhouse!
  [node test]
  (chu/start-clickhouse!
    node
    test
    clickhouse-alive?
    :server
    :--config (str configs-dir "/config.xml")
    :--
    :--logger.log (str logs-dir "/clickhouse.log")
    :--logger.errorlog (str logs-dir "/clickhouse.err.log")
    :--path data-dir))
