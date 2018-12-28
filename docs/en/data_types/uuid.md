# UUID {#uuid-data-type}

Universally unique identifier (UUID) is a 16-byte number used to identify the records. For detailed information about the UUID, see [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

The UUID format is represented below: 

```
xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

If you do not specify the UUID column value when inserting a new record, the UUID value is filled with zero:

```
00000000-0000-0000-0000-000000000000
```

## How to generate

To generate the UUID value, ClickHouse provides the [generateUUIDv4](../query_language/functions/uuid_function.md) function.

## Usage example

**Example 1**

This example demonstrates creating a table with the UUID type column and inserting a value into the table.

``` sql
:) CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

:) INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

:) SELECT * FROM t_uuid

┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Example 2**

In this example, the UUID column value is not specified when inserting a new record.

``` sql
:) INSERT INTO t_uuid (y) VALUES ('Example 2')

:) SELECT * FROM t_uuid

┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## Restrictions

Use the UUID data type only with functions which support the [String](string.md) data type (for example, [min](../query_language/agg_functions/reference.md#agg_function-min), [max](../query_language/agg_functions/reference.md#agg_function-max), and [count](../query_language/agg_functions/reference.md#agg_function-count)).

The UUID type is not supported by arithmetic operations (for example, [abs](../query_language/functions/arithmetic_functions.md#arithm_func-abs)) neither aggregate functions, such as [sum](../query_language/agg_functions/reference.md#agg_function-sum) and [avg](../query_language/agg_functions/reference.md#agg_function-avg).

[Original article](https://clickhouse.yandex/docs/en/data_types/uuid/) <!--hide-->
