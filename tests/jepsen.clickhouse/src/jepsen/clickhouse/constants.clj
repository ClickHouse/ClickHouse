(ns jepsen.clickhouse.constants)

(def root-folder "/home/robot-clickhouse")

(def binary-name "clickhouse")

(def binary-path (str root-folder "/" binary-name))
(def pid-file-path (str root-folder "/clickhouse.pid"))

(def data-dir (str root-folder "/db"))
(def logs-dir (str root-folder "/logs"))
(def configs-dir (str root-folder "/config"))
(def sub-configs-dir (str configs-dir "/config.d"))

(def coordination-data-dir (str data-dir "/coordination"))
(def coordination-snapshots-dir (str coordination-data-dir "/snapshots"))
(def coordination-latest-snapshot-dir (str coordination-data-dir "/latest_snapshot"))
(def coordination-logs-dir (str coordination-data-dir "/logs"))
(def coordination-latest_log-dir (str coordination-data-dir "/latest_log"))

(def stderr-file (str logs-dir "/stderr.log"))

(def binaries-cache-dir (str root-folder "/binaries"))
