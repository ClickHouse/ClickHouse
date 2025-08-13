---
slug: /en/operations/system-tables/databases
---
# databases

Contains information about the databases that are available to the current user.

Columns:

- `name` ([String](../../sql-reference/data-types/string.md)) — Database name.
- `engine` ([String](../../sql-reference/data-types/string.md)) — [Database engine](../../engines/database-engines/index.md).
- `data_path` ([String](../../sql-reference/data-types/string.md)) — Data path.
- `metadata_path` ([String](../../sql-reference/data-types/enum.md)) — Metadata path.
- `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — Database UUID.
- `comment` ([String](../../sql-reference/data-types/enum.md)) — Database comment.
- `engine_full` ([String](../../sql-reference/data-types/enum.md)) — Parameters of the database engine.
- `database` ([String](../../sql-reference/data-types/string.md)) – Alias for `name`.

The `name` column from this system table is used for implementing the `SHOW DATABASES` query.

**Example**

Create a database.

``` sql
CREATE DATABASE test;
```

Check all of the available databases to the user.

``` sql
SELECT * FROM system.databases;
```

``` text
┌─name────────────────┬─engine─────┬─data_path────────────────────┬─metadata_path─────────────────────────────────────────────────────────┬─uuid─────────────────────────────────┬─engine_full────────────────────────────────────────────┬─comment─┐
│ INFORMATION_SCHEMA  │ Memory     │ /data/clickhouse_data/       │                                                                       │ 00000000-0000-0000-0000-000000000000 │ Memory                                                 │         │
│ default             │ Atomic     │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/f97/f97a3ceb-2e8a-4912-a043-c536e826a4d4/ │ f97a3ceb-2e8a-4912-a043-c536e826a4d4 │ Atomic                                                 │         │
│ information_schema  │ Memory     │ /data/clickhouse_data/       │                                                                       │ 00000000-0000-0000-0000-000000000000 │ Memory                                                 │         │
│ replicated_database │ Replicated │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/da8/da85bb71-102b-4f69-9aad-f8d6c403905e/ │ da85bb71-102b-4f69-9aad-f8d6c403905e │ Replicated('some/path/database', 'shard1', 'replica1') │         │
│ system              │ Atomic     │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/b57/b5770419-ac7a-4b67-8229-524122024076/ │ b5770419-ac7a-4b67-8229-524122024076 │ Atomic                                                 │         │
└─────────────────────┴────────────┴──────────────────────────────┴───────────────────────────────────────────────────────────────────────┴──────────────────────────────────────┴────────────────────────────────────────────────────────┴─────────┘

```
