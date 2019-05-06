# Integration Libraries from Third-party Developers

!!! warning "Disclaimer"
    Yandex does **not** maintain the tools and libraries listed below and haven't done any extensive testing to ensure their quality.

## Infrastructure Products

- Relational database management systems
    - [MySQL](https://www.mysql.com)
        - [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        - [clickhouse-mysql-data-reader](https://github.com/Altinity/clickhouse-mysql-data-reader)
        - [horgh-replicator](https://github.com/larsnovikov/horgh-replicator)
    - [PostgreSQL](https://www.postgresql.org)
        - [clickhousedb_fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
        - [infi.clickhouse_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (uses [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
        - [pg2ch](https://github.com/mkabilov/pg2ch)
    - [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        - [ClickHouseMigrator](https://github.com/zlzforever/ClickHouseMigrator)
- Message queues
    - [Kafka](https://kafka.apache.org)
        - [clickhouse_sinker](https://github.com/housepower/clickhouse_sinker) (uses [Go client](https://github.com/kshvakov/clickhouse/))
- Object storages
    - [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        - [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup)
- Container orchestration
    - [Kubernetes](https://kubernetes.io)
        - [clickhouse-operator](https://github.com/Altinity/clickhouse-operator)
- Configuration management
    - [puppet](https://puppet.com)
        - [innogames/clickhouse](https://forge.puppet.com/innogames/clickhouse)
        - [mfedotov/clickhouse](https://forge.puppet.com/mfedotov/clickhouse)
- Monitoring
    - [Graphite](https://graphiteapp.org)
        - [graphouse](https://github.com/yandex/graphouse)
        - [carbon-clickhouse](https://github.com/lomik/carbon-clickhouse)
    - [Grafana](https://grafana.com/)
        - [clickhouse-grafana](https://github.com/Vertamedia/clickhouse-grafana)
    - [Prometheus](https://prometheus.io/)
        - [clickhouse_exporter](https://github.com/f1yegor/clickhouse_exporter)
        - [PromHouse](https://github.com/Percona-Lab/PromHouse)
        - [clickhouse_exporter](https://github.com/hot-wifi/clickhouse_exporter) (uses [Go client](https://github.com/kshvakov/clickhouse/))
    - [Nagios](https://www.nagios.org/)
        - [check_clickhouse](https://github.com/exogroup/check_clickhouse/)
    - [Zabbix](https://www.zabbix.com)
        - [clickhouse-zabbix-template](https://github.com/Altinity/clickhouse-zabbix-template)
    - [Sematext](https://sematext.com/)
        - [clickhouse integration](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
- Logging
    - [rsyslog](https://www.rsyslog.com/)
        - [omclickhouse](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    - [fluentd](https://www.fluentd.org)
        - [loghouse](https://github.com/flant/loghouse) (for [Kubernetes](https://kubernetes.io))
    - [logagent](https://www.sematext.com/logagent)
        - [logagent output-plugin-clickhouse](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
- Geo
    - [MaxMind](https://dev.maxmind.com/geoip/)
        - [clickhouse-maxmind-geoip](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)

## Programming Language Ecosystems

- Python
    - [SQLAlchemy](https://www.sqlalchemy.org)
        - [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (uses [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
    - [pandas](https://pandas.pydata.org)
        - [pandahouse](https://github.com/kszucs/pandahouse)
- R
    - [dplyr](https://db.rstudio.com/dplyr/)
        - [RClickhouse](https://github.com/IMSMWU/RClickhouse) (uses [clickhouse-cpp](https://github.com/artpaul/clickhouse-cpp))
- Java
    - [Hadoop](http://hadoop.apache.org)
        - [clickhouse-hdfs-loader](https://github.com/jaykelin/clickhouse-hdfs-loader) (uses [JDBC](../../query_language/table_functions/jdbc.md))
- Scala
    - [Akka](https://akka.io)
        - [clickhouse-scala-client](https://github.com/crobox/clickhouse-scala-client)
- C#
    - [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
        - [ClickHouse.Ado](https://github.com/killwort/ClickHouse-Net)
        - [ClickHouse.Net](https://github.com/ilyabreev/ClickHouse.Net)
        - [ClickHouse.Net.Migrations](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
- Elixir
    - [Ecto](https://github.com/elixir-ecto/ecto)
        - [clickhouse_ecto](https://github.com/appodeal/clickhouse_ecto)


[Original article](https://clickhouse.yandex/docs/en/interfaces/third-party/integrations/) <!--hide-->
