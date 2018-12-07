# Библиотеки для интеграции от сторонних разработчиков

!!! warning "Disclaimer"
    Яндекс не поддерживает перечисленные ниже библиотеки и не проводит тщательного тестирования для проверки их качества.

- Реляционные системы управления базами данных
    - [MySQL](https://www.mysql.com)
        - [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
    - [PostgreSQL](https://www.postgresql.org)
        - [infi.clickhouse_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (использует [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
- Python
    - [SQLAlchemy](https://www.sqlalchemy.org)
        - [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (использует [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
- Java
    - [Hadoop](http://hadoop.apache.org)
        - [clickhouse-hdfs-loader](https://github.com/jaykelin/clickhouse-hdfs-loader) (использует [JDBC](../jdbc.md))
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

[Оригинальная статья](https://clickhouse.yandex/docs/ru/interfaces/third-party/integrations/) <!--hide-->
