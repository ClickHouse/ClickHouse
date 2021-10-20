---
toc_priority: 46
toc_title: UUID
---

# UUID {#uuid-data-type}

A universally unique identifier (UUID) is a 16-byte number used to identify records. For detailed information about the UUID, see [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

The example of UUID type value is represented below:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

If you do not specify the UUID column value when inserting a new record, the UUID value is filled with zero:

``` text
00000000-0000-0000-0000-000000000000
```

## How to Generate {#how-to-generate}

To generate the UUID value, ClickHouse provides the [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) function.

## Usage Example {#usage-example}

**Example 1**

This example demonstrates creating a table with the UUID type column and inserting a value into the table.

``` sql
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog
```

``` sql
INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Example 2**

In this example, the UUID column value is not specified when inserting a new record.

``` sql
INSERT INTO t_uuid (y) VALUES ('Example 2')
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## Restrictions {#restrictions}

The UUID data type only supports functions which [String](../../sql-reference/data-types/string.md) data type also supports (for example, [min](../../sql-reference/aggregate-functions/reference/min.md#agg_function-min), [max](../../sql-reference/aggregate-functions/reference/max.md#agg_function-max), and [count](../../sql-reference/aggregate-functions/reference/count.md#agg_function-count)).

The UUID data type is not supported by arithmetic operations (for example, [abs](../../sql-reference/functions/arithmetic-functions.md#arithm_func-abs)) or aggregate functions, such as [sum](../../sql-reference/aggregate-functions/reference/sum.md#agg_function-sum) and [avg](../../sql-reference/aggregate-functions/reference/avg.md#agg_function-avg).

[Original article](https://clickhouse.com/docs/en/data_types/uuid/) <!--hide-->
