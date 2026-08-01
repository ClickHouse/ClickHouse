---
description: 'Documentation for the UUID data type in ClickHouse'
sidebar_label: 'UUID'
sidebar_position: 24
slug: /sql-reference/data-types/uuid
title: 'UUID'
doc_type: 'reference'
---

# UUID

A Universally Unique Identifier (UUID) is a 16-byte value used to identify records. For detailed information about UUIDs, see [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

While different UUID variants exist, e.g. UUIDv4 and UUIDv7 (see [here](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis)), ClickHouse does not validate that inserted UUIDs conform to a particular variant.
UUIDs are internally treated as a sequence of 16 random bytes with [8-4-4-4-12 representation](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation) at SQL level.

Example UUID value:

```text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

The default UUID is all-zero. It is used, for example, when a new record is inserted but no value for a UUID column is specified:

```text
00000000-0000-0000-0000-000000000000
```

:::warning
Due to historical reasons, UUIDs are sorted by their second half.

While this is fine for UUIDv4 values, this can deteriorate performance with UUIDv7 columns used in primary index definitions (usage in ordering keys or partition keys is fine).
More specifically, UUIDv7 values consist of a timestamp in the first half and a counter in the second half.
UUIDv7 sorting in sparse primary key indexes (i.e., the first values of each index granule) will therefore be by counter field.
Assuming UUIDs were sorted by the first half (timestamp), then the primary key index analysis step at the beginning of queries is expected to prune all marks in all but one part.
However, with sorting by the second half (counter), at least one mark is expected to be returned for all parts, leading to unnecessary unnecessary disk accesses.
:::

Example:

```sql
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(50);
SELECT * FROM tab;
```

Result:

```text
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

As a workaround, the UUID can be converted to a timestamp extracted from the second half:

```sql
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (UUIDv7ToDateTime(uuid));
-- Or alternatively:                      [...] PRIMARY KEY (toStartOfHour(UUIDv7ToDateTime(uuid)));

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(50);
SELECT * FROM tab;
```

ORDER BY (UUIDv7ToDateTime(uuid), uuid)

## Generating UUIDs {#generating-uuids}

ClickHouse provides the [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) function to generate random UUID version 4 values.

## Usage Example {#usage-example}

**Example 1**

This example demonstrates the creation of a table with a UUID column and the insertion of a value into the table.

```sql
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

Result:

```text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Example 2**

In this example, no UUID column value is specified when the record is inserted, i.e. the default UUID value is inserted:

```sql
INSERT INTO t_uuid (y) VALUES ('Example 2')

SELECT * FROM t_uuid
```

```text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## Restrictions {#restrictions}

The UUID data type only supports functions which [String](../../sql-reference/data-types/string.md) data type also supports (for example, [min](/sql-reference/aggregate-functions/reference/min), [max](/sql-reference/aggregate-functions/reference/max), and [count](/sql-reference/aggregate-functions/reference/count)).

The UUID data type is not supported by arithmetic operations (for example, [abs](/sql-reference/functions/arithmetic-functions#abs)) or aggregate functions, such as [sum](/sql-reference/aggregate-functions/reference/sum) and [avg](/sql-reference/aggregate-functions/reference/avg).
