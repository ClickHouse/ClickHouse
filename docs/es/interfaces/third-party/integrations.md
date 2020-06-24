---
toc_priority: 27
toc_title: Integrations
---

# Integration Libraries from Third-party Developers {#integration-libraries-from-third-party-developers}

!!! warning "Disclaimer"
    Yandex does **not** maintain the tools and libraries listed below and haven’t done any extensive testing to ensure their quality.

## Infrastructure Products {#infrastructure-products}

-   Relational database management systems
    -   [MySQL](https://www.mysql.com)
        -   [mysql2ch](https://github.com/long2ice/mysql2ch)
        -   [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        -   [clickhouse-mysql-data-reader](https://github.com/Altinity/clickhouse-mysql-data-reader)
        -   [horgh-replicator](https://github.com/larsnovikov/horgh-replicator)
    -   [PostgreSQL](https://www.postgresql.org)
        -   [clickhousedb\_fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
        -   [infi.clickhouse\_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (uses [infi.clickhouse\_orm](https://github.com/Infinidat/infi.clickhouse_orm))
        -   [pg2ch](https://github.com/mkabilov/pg2ch)
        -   [clickhouse\_fdw](https://github.com/adjust/clickhouse_fdw)
    -   [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        -   [ClickHouseMigrator](https://github.com/zlzforever/ClickHouseMigrator)
-   Message queues
    -   [Kafka](https://kafka.apache.org)
        -   [clickhouse\_sinker](https://github.com/housepower/clickhouse_sinker) (uses [Go client](https://github.com/ClickHouse/clickhouse-go/))
        -   [stream-loader-clickhouse](https://github.com/adform/stream-loader)
-   Stream processing
    -   [Flink](https://flink.apache.org)
        -   [flink-clickhouse-sink](https://github.com/ivi-ru/flink-clickhouse-sink)
-   Object storages
    -   [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        -   [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup)
-   Container orchestration
    -   [Kubernetes](https://kubernetes.io)
        -   [clickhouse-operator](https://github.com/Altinity/clickhouse-operator)
-   Configuration management
    -   [puppet](https://puppet.com)
        -   [innogames/clickhouse](https://forge.puppet.com/innogames/clickhouse)
        -   [mfedotov/clickhouse](https://forge.puppet.com/mfedotov/clickhouse)
-   Monitoring
    -   [Graphite](https://graphiteapp.org)
        -   [graphouse](https://github.com/yandex/graphouse)
        -   [carbon-clickhouse](https://github.com/lomik/carbon-clickhouse) +
        -   [graphite-clickhouse](https://github.com/lomik/graphite-clickhouse)
        -   [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer) - optimizes staled partitions in [\*GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md#graphitemergetree) if rules from [rollup configuration](../../engines/table-engines/mergetree-family/graphitemergetree.md#rollup-configuration) could be applied
    -   [Grafana](https://grafana.com/)
        -   [clickhouse-grafana](https://github.com/Vertamedia/clickhouse-grafana)
    -   [Prometheus](https://prometheus.io/)
        -   [clickhouse\_exporter](https://github.com/f1yegor/clickhouse_exporter)
        -   [PromHouse](https://github.com/Percona-Lab/PromHouse)
        -   [clickhouse\_exporter](https://github.com/hot-wifi/clickhouse_exporter) (uses [Go client](https://github.com/kshvakov/clickhouse/))
    -   [Nagios](https://www.nagios.org/)
        -   [check\_clickhouse](https://github.com/exogroup/check_clickhouse/)
        -   [check\_clickhouse.py](https://github.com/innogames/igmonplugins/blob/master/src/check_clickhouse.py)
    -   [Zabbix](https://www.zabbix.com)
        -   [clickhouse-zabbix-template](https://github.com/Altinity/clickhouse-zabbix-template)
    -   [Sematext](https://sematext.com/)
        -   [clickhouse integration](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
-   Logging
    -   [rsyslog](https://www.rsyslog.com/)
        -   [omclickhouse](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    -   [fluentd](https://www.fluentd.org)
        -   [loghouse](https://github.com/flant/loghouse) (for [Kubernetes](https://kubernetes.io))
    -   [logagent](https://www.sematext.com/logagent)
        -   [logagent output-plugin-clickhouse](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
-   Geo
    -   [MaxMind](https://dev.maxmind.com/geoip/)
        -   [clickhouse-maxmind-geoip](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)

## Programming Language Ecosystems {#programming-language-ecosystems}

-   Python
    -   [SQLAlchemy](https://www.sqlalchemy.org)
        -   [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (uses [infi.clickhouse\_orm](https://github.com/Infinidat/infi.clickhouse_orm))
    -   [pandas](https://pandas.pydata.org)
        -   [pandahouse](https://github.com/kszucs/pandahouse)
-   PHP
    -   [Doctrine](https://www.doctrine-project.org/)
        -   [dbal-clickhouse](https://packagist.org/packages/friendsofdoctrine/dbal-clickhouse)
-   R
    -   [dplyr](https://db.rstudio.com/dplyr/)
        -   [RClickHouse](https://github.com/IMSMWU/RClickHouse) (uses [clickhouse-cpp](https://github.com/artpaul/clickhouse-cpp))
-   Java
    -   [Hadoop](http://hadoop.apache.org)
        -   [clickhouse-hdfs-loader](https://github.com/jaykelin/clickhouse-hdfs-loader) (uses [JDBC](../../sql-reference/table-functions/jdbc.md))
-   Scala
    -   [Akka](https://akka.io)
        -   [clickhouse-scala-client](https://github.com/crobox/clickhouse-scala-client)
-   C\#
    -   [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview)
        -   [ClickHouse.Ado](https://github.com/killwort/ClickHouse-Net)
        -   [ClickHouse.Client](https://github.com/DarkWanderer/ClickHouse.Client)
        -   [ClickHouse.Net](https://github.com/ilyabreev/ClickHouse.Net)
        -   [ClickHouse.Net.Migrations](https://github.com/ilyabreev/ClickHouse.Net.Migrations)
-   Elixir
    -   [Ecto](https://github.com/elixir-ecto/ecto)
        -   [clickhouse\_ecto](https://github.com/appodeal/clickhouse_ecto)
-   Ruby
    -   [Ruby on Rails](https://rubyonrails.org/)
        -   [activecube](https://github.com/bitquery/activecube)
        -   [ActiveRecord](https://github.com/PNixx/clickhouse-activerecord)
    -   [GraphQL](https://github.com/graphql)
        -   [activecube-graphql](https://github.com/bitquery/activecube-graphql)

[Original article](https://clickhouse.tech/docs/en/interfaces/third-party/integrations/) <!--hide-->
