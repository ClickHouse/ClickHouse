---
alias: []
description: 'Documentation for the Native format'
input_format: true
keywords: ['Native']
output_format: true
slug: /interfaces/formats/Native
title: 'Native'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `Native` format is ClickHouse's most efficient format because it is truly "columnar" 
in that it does not convert columns to rows.  

In this format data is written and read by [blocks](/development/architecture#block) in a binary format. 
For each block, the number of rows, number of columns, column names and types, and parts of columns in the block are recorded one after another. 

This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

:::tip
You can use this format to quickly generate dumps that can only be read by the ClickHouse DBMS.
It might not be practical to work with this format yourself.
:::

## Data types wire format {#data-types-wire-format}

Data is sent over the wire in a columnar format, which means that each column is sent separately,
and all values of a column are sent together as a single array.

Each column in a block contains a header similar to [RowBinaryWithNamesAndTypes](../formats/RowBinary/RowBinaryWithNamesAndTypes.md).

:::note
When using the native TCP binary protocol (or when the HTTP endpoint receives `?client_protocol_version=<n>`),
a `BlockInfo` structure is written before the column and row counts. The examples in this section use
the plain HTTP interface without a protocol version, which omits `BlockInfo`.
:::

### Block structure {#block-structure}

The following query returns two columns, `number` and `str`, with three rows:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str FROM system.numbers LIMIT 3" > out.bin
```

The output data fits into a single ClickHouse block, and it will look like this:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x02,                   // 2 columns
  0x03,                   // 3 rows
  // -- Column 1 Header --
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6e, 0x75, 0x6d,       
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6e,
  0x74, 0x36, 0x34,       // 'UInt64'
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x01, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x02, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 2 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6e, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x32,                   // '2' as String
])
```

### Multiple blocks {#multiple-blocks}

However, in many cases, the data will not fit into a single block, and ClickHouse will send the data as multiple blocks.
Consider the following query that fetches two rows with reduced block size to force splitting the data as one row per block:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str                FROM system.numbers LIMIT 2                 SETTINGS max_block_size=1" \  > out.bin
```

The output:

```js
const data = new Uint8Array([
 
  // ----- Block 1 ----- 
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D, 
  0x62, 0x65, 0x72,       // column name: 'number' 
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34,       // 'UInt64' 
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  
  // ----- Block 2 -----
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D,  
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E,  
  0x74, 0x36, 0x34,       // 'UInt64'
  0x01, 0x00, 0x00, 0x00,  
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72,  
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
]);
```

### Simple data types {#simple-data-types}

The wire format for an individual value of one of the simpler data types is similar to `RowBinary`/`RowBinaryWithNamesAndTypes`.
The full list of types that match this description includes:

- (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
- Float32, Float64
- Bool
- String
- FixedString(N)
- Date
- Date32
- DateTime
- DateTime64
- IPv4
- IPv6
- UUID

Refer to the descriptions of the types above in ["RowBinary data types wire format"](/interfaces/formats/RowBinary#data-types-wire-format) for more details.

### Complex data types {#complex-data-types}

The encoding of the following types differs from `RowBinary` and `RowBinaryWithNamesAndTypes`.

- Nullable
- LowCardinality
- Array
- Map
- Variant
- Dynamic
- JSON

#### Nullable {#nullable}

In the `Native` format, a nullable column will have a number of bytes equal to the number of rows in the block before the actual data. Each of these bytes indicates whether the value is `NULL` or not. For example, with this query, each odd number will be `NULL` instead:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, number, NULL) :: Nullable(UInt64) AS maybe_null                 FROM system.numbers LIMIT 5" \  > out.bin
```

The output will look like this:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01,                         // LEB128 - 1 column
  0x05,                         // LEB128 - 5 rows
  
  // -- Column Header --
  0x0A,                         // LEB128 - column name has 10 bytes
  0x6D, 0x61, 0x79, 0x62, 0x65, 
  0x5F, 0x6E, 0x75, 0x6C, 0x6C, // column name: 'maybe_null'
  
  0x10,                         // LEB128 - column type has 16 bytes
  0x4E, 0x75, 0x6C, 0x6C, 
  0x61, 0x62, 0x6C, 0x65, 
  0x28, 0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34, 0x29,       // column type: 'Nullable(UInt64)'
  
  // -- Nullable mask --
  0x00,                         // Row 0 is NOT NULL
  0x01,                         // Row 1 is NULL
  0x00,                         // Row 2 is NOT NULL
  0x01,                         // Row 3 is NULL
  0x00,                         // Row 4 is NOT NULL
  
  // -- UInt64 values --
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row 0: 0 as UInt64

  // even though we still might have a proper value for this number 
  // in the block, it should be still returned as NULL to the user!
  0x01, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #1: NULL
  
  0x02, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #2: 2 as UInt64
  
  0x03, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #3: NULL, similar to Row #1
  
  0x04, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #4: 4 as UInt64
]);
```

It works similarly with `Nullable(String)`. The null indicator always comes from the nullable mask byte —
a mask value of `0x01` means the row is `NULL` regardless of the string content. For `NULL` rows,
the underlying string is stored as an empty string (LEB128 length `0`). Note that a non-`NULL` empty
string also has LEB128 length `0`, so only the mask byte distinguishes the two cases. For example, the following query:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, toString(number), NULL) :: Nullable(String) AS maybe_str                 FROM system.numbers LIMIT 5" \  > out.bin
```

The output will look like this:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01, // LEB128 - 1 column
  0x05, // LEB128 - 5 rows

  // -- Column Header --
  0x09, // LEB128 - column name has 9 bytes
  0x6d,
  0x61,
  0x79,
  0x62,
  0x65,
  0x5f,
  0x73,
  0x74,
  0x72, // column name: 'maybe_str'

  0x10, // LEB128 - column type has 16 bytes
  0x4e,
  0x75,
  0x6c,
  0x6c,
  0x61,
  0x62,
  0x6c,
  0x65,
  0x28,
  0x53,
  0x74,
  0x72,
  0x69,
  0x6e,
  0x67,
  0x29, // column type: 'Nullable(String)'

  // -- Nullable mask --
  0x00, // Row 0 is NOT NULL
  0x01, // Row 1 is NULL
  0x00, // Row 2 is NOT NULL
  0x01, // Row 3 is NULL
  0x00, // Row 4 is NOT NULL

  // -- String values --
  0x01,
  0x30, // Row 0: LEB128 == 1, '0' as String
  0x00, // Row 1: LEB128 == 0, NULL
  0x01,
  0x32, // Row 2: LEB128 == 1, '2' as String
  0x00, // Row 3: LEB128 == 0, NULL
  0x01,
  0x34, // Row 4: LEB128 == 1, '4' as String
])
```

#### LowCardinality {#lowcardinality}

Unlike [RowBinary](RowBinary/RowBinary.md#lowcardinality) where `LowCardinality` is transparent, the Native format uses a dictionary-based columnar encoding. A column is encoded as a version prefix, then a dictionary of unique values, and an array of integer indexes into that dictionary.

:::note
A column can be defined as `LowCardinality(Nullable(T))`, but it is not possible to define it as `Nullable(LowCardinality(T))` — it will always result in an error from the server.
:::

The version prefix is a `UInt64(LE)` with value `1`, written once per column. Then, per block, the following is written:

- `UInt64(LE)` — `IndexesSerializationType` bitfield. Bits 0–7 encode the index width (0 = UInt8, 1 = UInt16, 2 = UInt32, 3 = UInt64). Bit 8 (`NeedGlobalDictionaryBit`) is never set in Native format (the server throws an exception if it is encountered). Bit 9 indicates additional dictionary keys are present. Bit 10 indicates the dictionary should be reset.
- `UInt64(LE)` — number of dictionary keys, followed by the keys bulk-serialized using the inner type encoding.
- `UInt64(LE)` — number of rows, followed by index values bulk-serialized using the appropriate UInt width.

The dictionary always contains a default value at index 0 (e.g. empty string for `String`, 0 for numeric types). For `LowCardinality(Nullable(T))`, index 0 represents `NULL`, and the keys are serialized without the `Nullable` wrapper.

For example, `LowCardinality(String)` with 5 rows `['foo', 'bar', 'baz', 'foo', 'bar']`:

```text
// Version prefix
01 00 00 00 00 00 00 00    // UInt64(LE) = 1

// IndexesSerializationType: UInt8 indexes, has keys, update dictionary
00 06 00 00 00 00 00 00    // UInt64(LE) = 0x0600

04 00 00 00 00 00 00 00    // 4 dictionary keys
00                          // key 0: "" (default)
03 66 6f 6f                 // key 1: "foo"
03 62 61 72                 // key 2: "bar"
03 62 61 7a                 // key 3: "baz"

05 00 00 00 00 00 00 00    // 5 rows
01 02 03 01 02              // indexes → "foo", "bar", "baz", "foo", "bar"
```

With `LowCardinality(Nullable(String))`, index 0 is `NULL`:

```text
01 00 00 00 00 00 00 00    // version
00 06 00 00 00 00 00 00    // IndexesSerializationType
03 00 00 00 00 00 00 00    // 3 keys
00                          // key 0: NULL
00                          // key 1: "" (default)
03 79 65 73                 // key 2: "yes"
05 00 00 00 00 00 00 00    // 5 rows
02 00 02 00 02              // indexes → "yes", NULL, "yes", NULL, "yes"
```

#### Array {#array}

Unlike [RowBinary](RowBinary/RowBinary.md#array) where each array is prefixed with a LEB128 element count, the Native format encodes arrays as two columnar sub-streams:

- N cumulative `UInt64` offsets (little-endian, 8 bytes each). Row `i` has `offset[i] - offset[i-1]` elements, with `offset[-1]` implicitly 0.
- All nested elements across all rows, bulk-serialized contiguously.

For example, `Array(UInt32)` with 3 rows `[[0, 10], [1, 11], [2, 12]]`:

```text
// Offsets
02 00 00 00 00 00 00 00    // 2 (row 0: 2 elements)
04 00 00 00 00 00 00 00    // 4 (row 1: 2 elements)
06 00 00 00 00 00 00 00    // 6 (row 2: 2 elements)

// Nested UInt32 values (6 total)
00 00 00 00                 // 0
0a 00 00 00                 // 10
01 00 00 00                 // 1
0b 00 00 00                 // 11
02 00 00 00                 // 2
0c 00 00 00                 // 12
```

An empty array has the same offset as the previous row. For example, `Array(String)` with 4 rows `[[], ['0'], ['0','1'], ['0','1','2']]`:

```text
00 00 00 00 00 00 00 00    // 0 (empty)
01 00 00 00 00 00 00 00    // 1
03 00 00 00 00 00 00 00    // 3
06 00 00 00 00 00 00 00    // 6
01 30                       // "0"
01 30                       // "0"
01 31                       // "1"
01 30                       // "0"
01 31                       // "1"
01 32                       // "2"
```

#### Map {#map}

A `Map(K, V)` is encoded as `Array(Tuple(K, V))` — array offsets followed by all keys, then all values. This differs from [RowBinary](RowBinary/RowBinary.md#map) where keys and values are interleaved per entry.

For example, `Map(String, UInt64)` with 3 rows `[{'a':0,'b':10}, {'a':1,'b':11}, {'a':2,'b':12}]`:

```text
// Array offsets
02 00 00 00 00 00 00 00    // 2
04 00 00 00 00 00 00 00    // 4
06 00 00 00 00 00 00 00    // 6

// All keys (6 Strings)
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"

// All values (6 UInt64s)
00 00 00 00 00 00 00 00    // 0
0a 00 00 00 00 00 00 00    // 10
01 00 00 00 00 00 00 00    // 1
0b 00 00 00 00 00 00 00    // 11
02 00 00 00 00 00 00 00    // 2
0c 00 00 00 00 00 00 00    // 12
```

#### Variant {#variant}

Unlike [RowBinary](RowBinary/RowBinary.md#variant) where each row carries its own discriminant byte followed by the value inline, the Native format separates discriminators from data.

:::warning
As with RowBinary, the types in the definition are always sorted alphabetically, and the discriminant is the index in that sorted list. `0xFF` (255) represents `NULL`.
:::

A `Variant` column is encoded as:

- `UInt64(LE)` discriminators mode prefix (`0` = BASIC, `1` = COMPACT). Native format output typically uses BASIC (`0`); COMPACT mode may appear when reading data stored with `use_compact_variant_discriminators_serialization` enabled.
- N `UInt8` discriminators, one per row.
- Each variant type's data as a separate bulk column containing only the matching rows, in discriminant order.

For example, `Variant(String, UInt32)` with 5 rows `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` (sorted: `String` = 0, `UInt32` = 1):

```text
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
01 00 ff 01 00              // UInt32, String, NULL, UInt32, String

// String (2 values, rows 1 and 4)
05 68 65 6c 6c 6f          // "hello"
05 68 65 6c 6c 6f          // "hello"

// UInt32 (2 values, rows 0 and 3)
00 00 00 00                 // 0
03 00 00 00                 // 3
```

#### Dynamic {#dynamic}

Unlike [RowBinary](RowBinary/RowBinary.md#dynamic) where each value is self-describing (type prefix + value), the Native format serializes `Dynamic` as a structure prefix followed by a [Variant](#variant) column.

The structure prefix contains a `UInt64(LE)` serialization version, then the number of dynamic types (as VarUInt), then the type names as strings. In version V1 the type count is written twice for compatibility. The data that follows is a `Variant` column whose type list is the dynamic types plus an internal `SharedVariant` type, sorted alphabetically.

For example, `Dynamic` with 5 rows `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']`:

```text
// Structure prefix (V1)
01 00 00 00 00 00 00 00    // version = V1
02                          // num types (V1 writes twice)
02                          // num types
06 53 74 72 69 6e 67       // "String"
06 55 49 6e 74 33 32       // "UInt32"

// Variant data: Variant(SharedVariant, String, UInt32)
// discriminants: SharedVariant=0, String=1, UInt32=2
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
02 01 ff 02 01              // UInt32, String, NULL, UInt32, String
// SharedVariant: 0 values
05 68 65 6c 6c 6f          // String: "hello"
05 68 65 6c 6c 6f          // String: "hello"
00 00 00 00                 // UInt32: 0
03 00 00 00                 // UInt32: 3
```

#### JSON {#json}

Unlike [RowBinary](RowBinary/RowBinary.md#json) where each row is self-describing with path names and values, the Native format serializes `JSON` in a columnar structure. The encoding is complex and version-dependent: it consists of a structure prefix with the serialization version, dynamic path names, and shared data layout, followed by typed paths (each as a bulk column), dynamic paths (each as a [Dynamic](#dynamic) column), and shared data for overflow paths.

For simpler interoperability, consider using the setting `output_format_native_write_json_as_string=1`, which serializes JSON columns as plain JSON text strings (one `String` per row).
