<div dir="rtl" markdown="1">

# کتابخانه ادغام ثالث

!!! warning "سلب مسئولیت"
    Yandex نه حفظ کتابخانه ها در زیر ذکر شده و نشده انجام هر آزمایش های گسترده ای برای اطمینان از کیفیت آنها.

- سیستم های مدیریت پایگاه داده رابطه ای
    - [MySQL](https://www.mysql.com)
        - [ProxySQL](https://github.com/sysown/proxysql/wiki/ClickHouse-Support)
    - [PostgreSQL](https://www.postgresql.org)
        - [infi.clickhouse_fdw](https://github.com/Infinidat/infi.clickhouse_fdw) (استفاده می کند [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
- فروشگاه شی
    - S3
        - [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup)
- Python
    - [SQLAlchemy](https://www.sqlalchemy.org)
        - [sqlalchemy-clickhouse](https://github.com/cloudflare/sqlalchemy-clickhouse) (استفاده می کند [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm))
- Java
    - [Hadoop](http://hadoop.apache.org)
        - [clickhouse-hdfs-loader](https://github.com/jaykelin/clickhouse-hdfs-loader) (استفاده می کند [JDBC](../jdbc.md))
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

</div>
[مقاله اصلی](https://clickhouse.yandex/docs/fa/interfaces/third-party/integrations/) <!--hide-->
