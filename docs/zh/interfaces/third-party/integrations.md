# 第三方集成库

!!! warning "声明"
    Yandex不维护下面列出的库，也没有进行任何广泛的测试以确保其质量。

## 基建产品
- 关系数据库管理系统
    - [MySQL](https://www.mysql.com)
        - [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        - [clickhouse-mysql-data-reader](https://github.com/Altinity/clickhouse-mysql-data-reader)
        - [horgh-replicator](https://github.com/larsnovikov/horgh-replicator)
    - [PostgreSQL](https://www.postgresql.org)
        - [clickhousedb_fdw](https://github.com/Percona-Lab/clickhousedb_fdw)
        - [infi.clickhouse_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (使用 [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
        - [pg2ch](https://github.com/mkabilov/pg2ch)
    - [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        - [ClickHouseMightrator](https://github.com/zlzforever/ClickHouseMigrator)
- 消息队列
    - [Kafka](https://kafka.apache.org)
        - [clickhouse_sinker](https://github.com/housepower/clickhouse_sinker) (使用 [Go client](https://github.com/kshvakov/clickhouse/))
- 对象存储
    - [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        - [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup)
- 容器编排
    - [Kubernetes](https://kubernetes.io)
        - [clickhouse-operator](https://github.com/Altinity/clickhouse-operator)
- 配置管理
    - [puppet](https://puppet.com)
        - [innogames/clickhouse](https://forge.puppet.com/innogames/clickhouse)
        - [mfedotov/clickhouse](https://forge.puppet.com/mfedotov/clickhouse)
- 监控
    - [Graphite](https://graphiteapp.org)
        - [graphouse](https://github.com/yandex/graphouse)
        - [carbon-clickhouse](https://github.com/lomik/carbon-clickhouse)
    - [Grafana](https://grafana.com/)
        - [clickhouse-grafana](https://github.com/Vertamedia/clickhouse-grafana)
    - [Prometheus](https://prometheus.io/)
        - [clickhouse_exporter](https://github.com/f1yegor/clickhouse_exporter)
        - [PromHouse](https://github.com/Percona-Lab/PromHouse)
    - [Nagios](https://www.nagios.org/)
        - [check_clickhouse](https://github.com/exogroup/check_clickhouse/)
    - [Zabbix](https://www.zabbix.com)
        - [clickhouse-zabbix-template](https://github.com/Altinity/clickhouse-zabbix-template)
    - [Sematext](https://sematext.com/)
        - [clickhouse积分](https://github.com/sematext/sematext-agent-integrations/tree/master/clickhouse)
- 记录
    - [rsyslog](https://www.rsyslog.com/)
        - [omclickhouse](https://www.rsyslog.com/doc/master/configuration/modules/omclickhouse.html)
    - [fluentd](https://www.fluentd.org)
        - [loghouse](https://github.com/flant/loghouse) (对于 [Kubernetes](https://kubernetes.io))
    - [logagent](https://www.sematext.com/logagent)
        - [logagent output-plugin-clickhouse](https://sematext.com/docs/logagent/output-plugin-clickhouse/)
- 地理
    - [MaxMind](https://dev.maxmind.com/geoip/)
        - [clickhouse-maxmind-geoip](https://github.com/AlexeyKupershtokh/clickhouse-maxmind-geoip)

## 编程语言生态系统

- Python
    - [SQLAlchemy](https://www.sqlalchemy.org)
        - [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (使用 [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
    - [pandas](https://pandas.pydata.org)
        - [pandahouse](https://github.com/kszucs/pandahouse)
- R
    - [dplyr](https://db.rstudio.com/dplyr/)
        - [RClickhouse](https://github.com/IMSMWU/RClickhouse) (使用 [clickhouse-cpp](https://github.com/artpaul/clickhouse-cpp))
- Java
    - [Hadoop](http://hadoop.apache.org)
        - [clickhouse-hdfs-loader](https://github.com/jaykelin/clickhouse-hdfs-loader) (使用 [JDBC](../../query_language/table_functions/jdbc.md))
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

[来源文章](https://clickhouse.yandex/docs/zh/interfaces/third-party/integrations/) <!--hide-->
