---
slug: /en/sql-reference/data-types/uuid
sidebar_position: 24
sidebar_label: UUID
---

# UUID

A Universally Unique Identifier (UUID) is a 16-byte value used to identify records. For detailed information about UUIDs, see [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

While different UUID variants exist (see [here](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis)), ClickHouse does not validate that inserted UUIDs conform to a particular variant. UUIDs are internally treated as a sequence of 16 random bytes with [8-4-4-4-12 representation](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation) at SQL level.

Example UUID value:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

The default UUID is all-zero. It is used, for example, when a new record is inserted but no value for a UUID column is specified:

``` text
00000000-0000-0000-0000-000000000000
```

## Generating UUIDs

ClickHouse provides the [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) function to generate random UUID version 4 values.

## Usage Example

**Example 1**

This example demonstrates the creation of a table with a UUID column and the insertion of a value into the table.

``` sql
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

Result:

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Example 2**

In this example, no UUID column value is specified when the record is inserted, i.e. the default UUID value is inserted:

``` sql
INSERT INTO t_uuid (y) VALUES ('Example 2')

SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## Restrictions

The UUID data type only supports functions which [String](../../sql-reference/data-types/string.md) data type also supports (for example, [min](../../sql-reference/aggregate-functions/reference/min.md#agg_function-min), [max](../../sql-reference/aggregate-functions/reference/max.md#agg_function-max), and [count](../../sql-reference/aggregate-functions/reference/count.md#agg_function-count)).

The UUID data type is not supported by arithmetic operations (for example, [abs](../../sql-reference/functions/arithmetic-functions.md#arithm_func-abs)) or aggregate functions, such as [sum](../../sql-reference/aggregate-functions/reference/sum.md#agg_function-sum) and [avg](../../sql-reference/aggregate-functions/reference/avg.md#agg_function-avg).
