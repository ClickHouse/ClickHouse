# system.databases  {#system-databases}

包含当前用户可用的数据库的相关信息。

列:

-   `name` ([String](../../sql-reference/data-types/string.md)) — 数据库的名称。
-   `engine` ([String](../../sql-reference/data-types/string.md)) — [数据库的引擎](../../engines/database-engines/index.md)。
-   `data_path` ([String](../../sql-reference/data-types/string.md)) — 数据的路径。
-   `metadata_path` ([String](../../sql-reference/data-types/enum.md)) — 元数据的路径。
-   `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — 数据库的 UUID。
-   `comment` ([String](../../sql-reference/data-types/enum.md)) — 数据库的注释。

这个系统表的 `name` 列被用于实现 `SHOW DATABASES` 查询。

**示例**

创建一个数据库。

``` sql
CREATE DATABASE test;
```

查询此用户所有可用的数据库。

``` sql
SELECT * FROM system.databases;
```

``` text
┌─name───────────────┬─engine─┬─data_path──────────────────┬─metadata_path───────────────────────────────────────────────────────┬─uuid─────────────────────────────────┬─comment─┐
│ INFORMATION_SCHEMA │ Memory │ /var/lib/clickhouse/       │                                                                     │ 00000000-0000-0000-0000-000000000000 │         │
│ default            │ Atomic │ /var/lib/clickhouse/store/ │ /var/lib/clickhouse/store/d31/d317b4bd-3595-4386-81ee-c2334694128a/ │ 24363899-31d7-42a0-a436-389931d752a0 │         │
│ information_schema │ Memory │ /var/lib/clickhouse/       │                                                                     │ 00000000-0000-0000-0000-000000000000 │         │
│ system             │ Atomic │ /var/lib/clickhouse/store/ │ /var/lib/clickhouse/store/1d1/1d1c869d-e465-4b1b-a51f-be033436ebf9/ │ 03e9f3d1-cc88-4a49-83e9-f3d1cc881a49 │         │
└────────────────────┴────────┴────────────────────────────┴─────────────────────────────────────────────────────────────────────┴──────────────────────────────────────┴─────────┘
```

[原文](https://clickhouse.com/docs/zh/operations/system-tables/databases) <!--hide-->
