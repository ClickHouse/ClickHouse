# system.databases {#system-databases}

Contains information about the databases that are available to the current user.

Columns:

-   `name` ([String](../../sql-reference/data-types/string.md)) — Database name.
-   `engine` ([String](../../sql-reference/data-types/string.md)) — [Database engine](../../engines/database-engines/index.md).
-   `data_path` ([String](../../sql-reference/data-types/string.md)) — Data path.
-   `metadata_path` ([String](../../sql-reference/data-types/enum.md)) — Metadata path.
-   `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — Database UUID.

The `name` column from this system table is used for implementing the `SHOW DATABASES` query.

**Example**

Create a database.

``` sql
CREATE DATABASE test
```

Check all of the available databases to the user.

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

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/databases) <!--hide-->
