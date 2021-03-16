(ns jepsen.nukeeper.constants)

(def dir "/var/lib/clickhouse")
(def binary "clickhouse")
(def logdir "/var/log/clickhouse-server")
(def logfile "/var/log/clickhouse-server/stderr.log")
(def serverlog "/var/log/clickhouse-server/clickhouse-server.log")
(def pidfile (str dir "/clickhouse.pid"))
(def binary-path "/tmp")
