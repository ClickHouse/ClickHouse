# 第三方集成库

!!! warning "放弃"
    Yandex不维护下面列出的库，也没有进行任何广泛的测试以确保其质量。

- 关系数据库管理系统
    - [MySQL](https://www.mysql.com)
        - [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
    - [PostgreSQL](https://www.postgresql.org)
        - [infi.clickhouse_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (使用 [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
- Python
    - [SQLAlchemy](https://www.sqlalchemy.org)
        - [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (使用 [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
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
