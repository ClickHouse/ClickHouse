(ns jepsen.clickhouse-keeper.constants)

(def common-prefix "/home/robot-clickhouse")

(def binary-name "clickhouse")

(def binary-path (str common-prefix "/" binary-name))
(def pid-file-path (str common-prefix "/clickhouse.pid"))

(def data-dir (str common-prefix "/db"))
(def logs-dir (str common-prefix "/logs"))
(def configs-dir (str common-prefix "/config"))
(def sub-configs-dir (str configs-dir "/config.d"))
(def coordination-data-dir (str data-dir "/coordination"))
(def coordination-snapshots-dir (str coordination-data-dir "/snapshots"))
(def coordination-logs-dir (str coordination-data-dir "/logs"))

(def stderr-file (str logs-dir "/stderr.log"))

(def binaries-cache-dir (str common-prefix "/binaries"))
