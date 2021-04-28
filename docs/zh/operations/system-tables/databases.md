# system.databases {#system-databases}

包含有关当前用户可用的数据库的信息。

列:

-   `name` ([String](../../sql-reference/data-types/string.md)) — 数据库名称。
-   `engine` ([String](../../sql-reference/data-types/string.md)) — [数据库引擎](../../engines/database-engines/index.md)。
-   `data_path` ([String](../../sql-reference/data-types/string.md)) — 数据路径。
-   `metadata_path` ([String](../../sql-reference/data-types/enum.md)) — 元数据路径。
-   `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — 数据库UUID。

该系统表中的 `name` 列用于实现 `SHOW DATABASES` 查询。

**示例**

创建一个数据库。

``` sql
CREATE DATABASE test
```

检查提供给用户的所有可用数据库。

``` sql
SELECT * FROM system.databases
```

``` text
┌─name───────────────────────────┬─engine─┬─data_path──────────────────┬─metadata_path───────────────────────────────────────────────────────┬─────────────────────────────────uuid─┐
│ _temporary_and_external_tables │ Memory │ /var/lib/clickhouse/       │                                                                     │ 00000000-0000-0000-0000-000000000000 │
│ default                        │ Atomic │ /var/lib/clickhouse/store/ │ /var/lib/clickhouse/store/d31/d317b4bd-3595-4386-81ee-c2334694128a/ │ d317b4bd-3595-4386-81ee-c2334694128a │
│ test                           │ Atomic │ /var/lib/clickhouse/store/ │ /var/lib/clickhouse/store/39b/39bf0cc5-4c06-4717-87fe-c75ff3bd8ebb/ │ 39bf0cc5-4c06-4717-87fe-c75ff3bd8ebb │
│ system                         │ Atomic │ /var/lib/clickhouse/store/ │ /var/lib/clickhouse/store/1d1/1d1c869d-e465-4b1b-a51f-be033436ebf9/ │ 1d1c869d-e465-4b1b-a51f-be033436ebf9 │
└────────────────────────────────┴────────┴────────────────────────────┴─────────────────────────────────────────────────────────────────────┴──────────────────────────────────────┘
```

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/databases) <!--hide-->
