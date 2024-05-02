---
slug: /en/sql-reference/data-types/uuid
sidebar_position: 24
sidebar_label: UUID
---

# UUID

A Universally Unique Identifier (UUID) is a 16-byte value used to identify records. For detailed information about UUIDs, see [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

While different UUID variants exist (see [here](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis)), ClickHouse does not validate that inserted UUIDs conform to a particular variant.
UUIDs are internally treated as a sequence of 16 random bytes with [8-4-4-4-12 representation](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation) at SQL level.

Example UUID value:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

The default UUID is all-zero. It is used, for example, when a new record is inserted but no value for a UUID column is specified:

``` text
00000000-0000-0000-0000-000000000000
```

Due to historical reasons, UUIDs are sorted by their second half (which is unintuitive).
UUIDs should therefore not be used in an primary key (or sorting key) of a table, or as partition key.

Example:

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;
INSERT INTO tab SELECT generateUUIDv4() FROM numbers(50);
SELECT * FROM tab ORDER BY uuid;
```

Result:

``` text
┌─uuid─────────────────────────────────┐
│ 36a0b67c-b74a-4640-803b-e44bb4547e3c │
│ 3a00aeb8-2605-4eec-8215-08c0ecb51112 │
│ 3fda7c49-282e-421a-85ab-c5684ef1d350 │
│ 16ab55a7-45f6-44a8-873c-7a0b44346b3e │
│ e3776711-6359-4f22-878d-bf290d052c85 │
│ 1be30226-57b2-4739-88ec-5e3d490090f2 │
│ f65853a9-4375-4f0e-8b96-906ff622ed3c │
│ d5a0c7a6-79c6-4107-8bb8-df85915edcb7 │
│ 258e6068-17d1-4a1a-8be3-ed2ceb21815c │
│ 04b0f6a9-1f7b-4a42-8bfc-62f37b8a32b8 │
│ 9924f0d9-9c16-43a9-8f08-0944ab495aed │
│ 6720dc14-4eab-4e3e-8f0c-10c4ae8d2673 │
│ 5ddadb52-0452-4f5d-9030-c3f969af93a4 │
│                [...]                 │
│ 2dde30e6-59a1-48f8-b260-eb37921185b6 │
│ d5402a1b-77b3-4897-b288-29edf5c3ed12 │
│ 01843939-3ba7-4fea-b2aa-45f9a6f1e057 │
│ 9eceda2f-6946-40e3-b725-16f2709ca41a │
│ 03644f74-47ba-4020-b865-be5fd4c8c7ff │
│ ce3bc93d-ab19-4c74-b8cc-737cb9212099 │
│ b7ad6c91-23d6-4b5e-b8e4-a52297490b56 │
│ 06892f64-cc2d-45f3-bf86-f5c5af5768a9 │
└──────────────────────────────────────┘
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
