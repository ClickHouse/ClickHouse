# 第三方集成库

!!! warning "声明"
    Yandex不维护下面列出的库，也没有进行任何广泛的测试以确保其质量。

## 基建产品
- 关系数据库管理系统
    - [MySQL](https://www.mysql.com)
        - [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
        - [clickhouse-mysql-data-reader](https://github.com/Altinity/clickhouse-mysql-data-reader)
    - [PostgreSQL](https://www.postgresql.org)
        - [infi.clickhouse_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (使用 [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
    - [MSSQL](https://en.wikipedia.org/wiki/Microsoft_SQL_Server)
        - [ClickHouseMightrator](https://github.com/zlzforever/ClickHouseMigrator)
- 消息队列
    - [Kafka](https://kafka.apache.org)
        - [clickhouse_sinker](https://github.com/housepower/clickhouse_sinker) (使用 [Go client](https://github.com/kshvakov/clickhouse/))
- 对象存储
    - [S3](https://en.wikipedia.org/wiki/Amazon_S3)
        - [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup)
- 监控
    - [Graphite](https://graphiteapp.org)
        - [graphouse](https://github.com/yandex/graphouse)
        - [carbon-clickhouse](https://github.com/lomik/carbon-clickhouse)
    - [Grafana](https://grafana.com/)
        - [clickhouse-grafana](https://github.com/Vertamedia/clickhouse-grafana)
    - [Prometheus](https://prometheus.io/)
        - [clickhouse_exporter](https://github.com/f1yegor/clickhouse_exporter)
        - [PromHouse](https://github.com/Percona-Lab/PromHouse)
- 记录
    - [fluentd](https://www.fluentd.org)
        - [loghouse](https://github.com/flant/loghouse) (对于 [Kubernetes](https://kubernetes.io))

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
        - [clickhouse-hdfs-loader](https://github.com/jaykelin/clickhouse-hdfs-loader) (使用 [JDBC](../jdbc.md))
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
