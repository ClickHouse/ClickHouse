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

Due to historical reasons, UUIDs are sorted by their second half.
UUIDs should therefore not be used directly in a primary key, sorting key, or partition key of a table.

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
│                [...]                 │
│ 9eceda2f-6946-40e3-b725-16f2709ca41a │
│ 03644f74-47ba-4020-b865-be5fd4c8c7ff │
│ ce3bc93d-ab19-4c74-b8cc-737cb9212099 │
│ b7ad6c91-23d6-4b5e-b8e4-a52297490b56 │
│ 06892f64-cc2d-45f3-bf86-f5c5af5768a9 │
└──────────────────────────────────────┘
```

As a workaround, the UUID can be converted to a type with an intuitive sort order.

Example using conversion to UInt128:

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;
INSERT INTO tab SELECT generateUUIDv4() FROM numbers(50);
SELECT * FROM tab ORDER BY toUInt128(uuid);
```

Result:

```sql
┌─uuid─────────────────────────────────┐
│ 018b81cd-aca1-4e9c-9e56-a84a074dc1a8 │
│ 02380033-c96a-438e-913f-a2c67e341def │
│ 057cf435-7044-456a-893b-9183a4475cea │
│ 0a3c1d4c-f57d-44cc-8567-60cb0c46f76e │
│ 0c15bf1c-8633-4414-a084-7017eead9e41 │
│                [...]                 │
│ f808cf05-ea57-4e81-8add-29a195bde63d │
│ f859fb5d-764b-4a33-81e6-9e4239dae083 │
│ fb1b7e37-ab7b-421a-910b-80e60e2bf9eb │
│ fc3174ff-517b-49b5-bfe2-9b369a5c506d │
│ fece9bf6-3832-449a-b058-cd1d70a02c8b │
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
