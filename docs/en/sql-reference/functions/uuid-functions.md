---
sidebar_position: 53
sidebar_label: UUID
---

# Functions for Working with UUID

The functions for working with UUID are listed below.

## generateUUIDv4

Generates the [UUID](../data-types/uuid.md) of [version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

**Syntax**

``` sql
generateUUIDv4([x])
```

**Arguments**

-   `x` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in any of the [supported data types](../../sql-reference/data-types/index.md#data_types). The resulting value is discarded, but the expression itself if used for bypassing [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in one query. Optional parameter.

**Returned value**

The UUID type value.

**Usage example**

This example demonstrates creating a table with the UUID type column and inserting a value into the table.

``` sql
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4()

SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

**Usage example if it is needed to generate multiple values in one row**

```sql
SELECT generateUUIDv4(1), generateUUIDv4(2)
┌─generateUUIDv4(1)────────────────────┬─generateUUIDv4(2)────────────────────┐
│ 2d49dc6e-ddce-4cd0-afb8-790956df54c1 │ 8abf8c13-7dea-4fdf-af3e-0e18767770e6 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## empty

Checks whether the input UUID is empty.

**Syntax**

```sql
empty(UUID)
```

The UUID is considered empty if it contains all zeros (zero UUID).

The function also works for [arrays](array-functions.md#function-empty) or [strings](string-functions.md#empty).

**Arguments**

-   `x` — Input UUID. [UUID](../data-types/uuid.md).

**Returned value**

-   Returns `1` for an empty UUID or `0` for a non-empty UUID. 

Type: [UInt8](../data-types/int-uint.md).

**Example**

To generate the UUID value, ClickHouse provides the [generateUUIDv4](#uuid-function-generate) function.

Query:

```sql
SELECT empty(generateUUIDv4());
```

Result:

```text
┌─empty(generateUUIDv4())─┐
│                       0 │
└─────────────────────────┘
```

## notEmpty

Checks whether the input UUID is non-empty.

**Syntax**

```sql
notEmpty(UUID)
```

The UUID is considered empty if it contains all zeros (zero UUID).

The function also works for [arrays](array-functions.md#function-notempty) or [strings](string-functions.md#notempty).

**Arguments**

-   `x` — Input UUID. [UUID](../data-types/uuid.md).

**Returned value**

-   Returns `1` for a non-empty UUID or `0` for an empty UUID. 

Type: [UInt8](../data-types/int-uint.md).

**Example**

To generate the UUID value, ClickHouse provides the [generateUUIDv4](#uuid-function-generate) function.

Query:

```sql
SELECT notEmpty(generateUUIDv4());
```

Result:

```text
┌─notEmpty(generateUUIDv4())─┐
│                          1 │
└────────────────────────────┘
```

## toUUID (x)

Converts String type value to UUID type.

``` sql
toUUID(String)
```

**Returned value**

The UUID type value.

**Usage example**

``` sql
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
```

``` text
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## toUUIDOrNull (x)

It takes an argument of type String and tries to parse it into UUID. If failed, returns NULL.

``` sql
toUUIDOrNull(String)
```

**Returned value**

The Nullable(UUID) type value.

**Usage example**

``` sql
SELECT toUUIDOrNull('61f0c404-5cb3-11e7-907b-a6006ad3dba0T') AS uuid
```

``` text
┌─uuid─┐
│ ᴺᵁᴸᴸ │
└──────┘
```

## toUUIDOrZero (x)

It takes an argument of type String and tries to parse it into UUID. If failed, returns zero UUID.

``` sql
toUUIDOrZero(String)
```

**Returned value**

The UUID type value.

**Usage example**

``` sql
SELECT toUUIDOrZero('61f0c404-5cb3-11e7-907b-a6006ad3dba0T') AS uuid
```

``` text
┌─────────────────────────────────uuid─┐
│ 00000000-0000-0000-0000-000000000000 │
└──────────────────────────────────────┘
```

## UUIDStringToNum

Accepts a string containing 36 characters in the format `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, and returns it as a set of bytes in a [FixedString(16)](../../sql-reference/data-types/fixedstring.md).

``` sql
UUIDStringToNum(String)
```

**Returned value**

FixedString(16)

**Usage examples**

``` sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes
```

``` text
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDNumToString

Accepts a [FixedString(16)](../../sql-reference/data-types/fixedstring.md) value, and returns a string containing 36 characters in text format.

``` sql
UUIDNumToString(FixedString(16))
```

**Returned value**

String.

**Usage example**

``` sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```

``` text
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## serverUUID()

Returns the random and unique UUID, which is generated when the server is first started and stored forever. The result writes to the file `uuid` created in the ClickHouse server directory `/var/lib/clickhouse/`. 

**Syntax**

```sql
serverUUID()
```

**Returned value**

-   The UUID of the server. 

Type: [UUID](../data-types/uuid.md).

## See Also

-   [dictGetUUID](../../sql-reference/functions/ext-dict-functions.md#ext_dict_functions-other)
