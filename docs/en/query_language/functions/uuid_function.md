# generateUUIDv4 function

Generates [UUID](../../data_types/uuid.md) of version 4.

```sql
generateUUIDv4()
```

## Returned value

The UUID value.

## Usage example

This example demonstrates creating a table with the UUID type column and inserting a value into the table.

``` sql
:) CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

:) INSERT INTO t_uuid SELECT generateUUIDv4()

:) SELECT * FROM t_uuid

┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/uuid_function/) <!--hide-->
