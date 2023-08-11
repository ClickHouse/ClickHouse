---
slug: /en/sql-reference/data-types/uuid
sidebar_position: 46
sidebar_label: UUID
---

# UUID

A universally unique identifier (UUID) is a 16-byte number used to identify records. For detailed information about the UUID, see [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

The example of UUID type value is represented below:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

If you do not specify the UUID column value when inserting a new record, the UUID value is filled with zero:

``` text
00000000-0000-0000-0000-000000000000
```

## How to Generate

To generate the UUID value, ClickHouse provides the [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) function.

## Usage Example

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

## Implementation Details

The underlying implementation for a UUID has it represented as a 128-bit unsigned integer. Underlying this, a wide integer with a 64-bit unsigned integer as its base is utilized. This wide integer can be interfaced with as an array to access different components of the base. For example, on a Little Endian platform, accessing at index 0 will give you the 8 lower bytes, and index 1 will give you the 8 higher bytes. On a Big Endian platform, this is reversed where index 0 will give you the 8 higher bytes, and index 1 will give you the 8 lower bytes

``` cpp
uuid.toUnderType().items[0] 

//  uint64_t   uint64_t
// [xxxxxxxx] [        ]

uuid.toUnderType().items[1]

//  uint64_t   uint64_t
// [        ] [xxxxxxxx]
```

### Previous Implementation

Originally, the way that data was stored in the underlying wide integer did not treat the data as one contiguous 128-bit number, but instead as two 64-bit chunks sequenced in an array. On a Little Endian platform, this resulted in the following layout

``` cpp
// Suppose uuid contains 61f0c404-5cb3-11e7-907b-a6006ad3dba0

uuid.toUnderType().items[0]

//  uint64_t as HEX
// [E7 11 B3 5C 04 C4 F0 61] [A0 DB D3 6A 00 A6 7B 90]
//  ^^^^^^^^^^^^^^^^^^^^^^^

uuid.toUnderType().items[1]

//  uint64_t as HEX
// [E7 11 B3 5C 04 C4 F0 61] [A0 DB D3 6A 00 A6 7B 90]
//                            ^^^^^^^^^^^^^^^^^^^^^^^
```

While storing the data in this way shouldn't matter too much, it has some unfortunate side effects when treated as one contiguous number. One example of where this could be seen is sorting data by the UUID. The following result is due to this mixed layout

``` sql
CREATE TABLE test (uuid UUID) engine = Memory;
INSERT INTO test VALUES
  ('3f5ffba3-19ff-4f3d-8861-60ae6e1fc1aa'), 
  ('a72dc048-f72f-470e-b0f9-60cfad6e1157'), 
  ('40634f4f-37bf-44e4-ac7c-6f024ad19990'), 
  ('cb5818ab-83b5-48a8-8861-60ae6e1fc1aa'), 
  ('ffffffff-ffff-ffff-0000-000000000000'), 
  ('00000000-0000-0000-ffff-ffffffffffff');
SELECT * FROM test ORDER BY uuid;

-- Output:
--   ffffffff-ffff-ffff-0000-000000000000
--   3f5ffba3-19ff-4f3d-8861-60ae6e1fc1aa
--   cb5818ab-83b5-48a8-8861-60ae6e1fc1aa
--   40634f4f-37bf-44e4-ac7c-6f024ad19990
--   a72dc048-f72f-470e-b0f9-60cfad6e1157
--   00000000-0000-0000-ffff-ffffffffffff
```

This introduced some difficult to remedy inconsistencies between how a Little Endian and Big Endian platform represent the data.

### Current Implementation

Changing the organization of the data to be one contiguous 128-bit number allows for an easier to maintain implementation compatible with both Little and Big Endian systems. On a Little Endian platform, this organization can be visualized as 

``` cpp
// Suppose uuid contains 61f0c404-5cb3-11e7-907b-a6006ad3dba0

uuid.toUnderType().items[0]

//  uint64_t as HEX
// [A0 DB D3 6A 00 A6 7B 90] [E7 11 B3 5C 04 C4 F0 61]
//  ^^^^^^^^^^^^^^^^^^^^^^^

uuid.toUnderType().items[1]

//  uint64_t as HEX
// [A0 DB D3 6A 00 A6 7B 90] [E7 11 B3 5C 04 C4 F0 61]
//                            ^^^^^^^^^^^^^^^^^^^^^^^
```

while on a Big Endian platform this would be

``` cpp
// Suppose uuid contains 61f0c404-5cb3-11e7-907b-a6006ad3dba0

uuid.toUnderType().items[0]

//  uint64_t as HEX
// [61 F0 C4 04 5C B3 11 E7] [90 7B A6 00 6A D3 DB A0]
//  ^^^^^^^^^^^^^^^^^^^^^^^

uuid.toUnderType().items[1]

//  uint64_t as HEX
// [61 F0 C4 04 5C B3 11 E7] [90 7B A6 00 6A D3 DB A0]
//       
```

:::warning
To maintain backwards compatibility, during various forms of processing a UUID, such as serialization, deserialization, reinterpretation, hashing, etc., the method `toLegacyFormat` should be called to swap the high and low sets of bytes to keep the data in the expected ordering.
:::

## Restrictions

The UUID data type only supports functions which [String](../../sql-reference/data-types/string.md) data type also supports (for example, [min](../../sql-reference/aggregate-functions/reference/min.md#agg_function-min), [max](../../sql-reference/aggregate-functions/reference/max.md#agg_function-max), and [count](../../sql-reference/aggregate-functions/reference/count.md#agg_function-count)).

The UUID data type is not supported by arithmetic operations (for example, [abs](../../sql-reference/functions/arithmetic-functions.md#arithm_func-abs)) or aggregate functions, such as [sum](../../sql-reference/aggregate-functions/reference/sum.md#agg_function-sum) and [avg](../../sql-reference/aggregate-functions/reference/avg.md#agg_function-avg).
