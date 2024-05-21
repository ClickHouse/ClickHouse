---
slug: /en/sql-reference/functions/uuid-functions
sidebar_position: 205
sidebar_label: UUIDs
---

# Functions for Working with UUIDs

## generateUUIDv4

Generates the [UUID](../data-types/uuid.md) of [version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

**Syntax**

``` sql
generateUUIDv4([x])
```

**Arguments**

- `x` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in any of the [supported data types](../../sql-reference/data-types/index.md#data_types). The resulting value is discarded, but the expression itself if used for bypassing [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in one query. Optional parameter.

**Returned value**

The UUID type value.

**Usage example**

This example demonstrates creating a table with the UUID type column and inserting a value into the table.

``` sql
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4()

SELECT * FROM t_uuid
```

```response
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

- `x` — Input UUID. [UUID](../data-types/uuid.md).

**Returned value**

- Returns `1` for an empty UUID or `0` for a non-empty UUID. 

Type: [UInt8](../data-types/int-uint.md).

**Example**

To generate the UUID value, ClickHouse provides the [generateUUIDv4](#uuid-function-generate) function.

Query:

```sql
SELECT empty(generateUUIDv4());
```

Result:

```response
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

- `x` — Input UUID. [UUID](../data-types/uuid.md).

**Returned value**

- Returns `1` for a non-empty UUID or `0` for an empty UUID. 

Type: [UInt8](../data-types/int-uint.md).

**Example**

To generate the UUID value, ClickHouse provides the [generateUUIDv4](#uuid-function-generate) function.

Query:

```sql
SELECT notEmpty(generateUUIDv4());
```

Result:

```response
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

```response
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## toUUIDOrDefault (x,y)

**Arguments**

- `string` — String of 36 characters or FixedString(36). [String](../../sql-reference/syntax.md#string).
- `default` — UUID to be used as the default if the first argument cannot be converted to a UUID type. [UUID](/docs/en/sql-reference/data-types/uuid.md).

**Returned value**

UUID

``` sql
toUUIDOrDefault(String, UUID)
```

**Returned value**

The UUID type value.

**Usage examples**

This first example returns the first argument converted to a UUID type as it can be converted:

``` sql
SELECT toUUIDOrDefault('61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as UUID));
```
```response
┌─toUUIDOrDefault('61f0c404-5cb3-11e7-907b-a6006ad3dba0', CAST('59f0c404-5cb3-11e7-907b-a6006ad3dba0', 'UUID'))─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0                                                                          │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

This second example returns the second argument (the provided default UUID) as the first argument cannot be converted to a UUID type:

```sql
SELECT toUUIDOrDefault('-----61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as UUID));
```

```response
┌─toUUIDOrDefault('-----61f0c404-5cb3-11e7-907b-a6006ad3dba0', CAST('59f0c404-5cb3-11e7-907b-a6006ad3dba0', 'UUID'))─┐
│ 59f0c404-5cb3-11e7-907b-a6006ad3dba0                                                                               │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
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

```response
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

```response
┌─────────────────────────────────uuid─┐
│ 00000000-0000-0000-0000-000000000000 │
└──────────────────────────────────────┘
```

## UUIDStringToNum

Accepts `string` containing 36 characters in the format `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, and returns a [FixedString(16)](../../sql-reference/data-types/fixedstring.md) as its binary representation, with its format optionally specified by `variant` (`Big-endian` by default).

**Syntax**

``` sql
UUIDStringToNum(string[, variant = 1])
```

**Arguments**

- `string` — String of 36 characters or FixedString(36). [String](../../sql-reference/syntax.md#syntax-string-literal).
- `variant` — Integer, representing a variant as specified by [RFC4122](https://datatracker.ietf.org/doc/html/rfc4122#section-4.1.1). 1 = `Big-endian` (default), 2 = `Microsoft`.

**Returned value**

FixedString(16)

**Usage examples**

``` sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes
```

```response
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

``` sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid, 2) AS bytes
```

```response
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ @</a;]~!p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDNumToString

Accepts `binary` containing a binary representation of a UUID, with its format optionally specified by `variant` (`Big-endian` by default), and returns a string containing 36 characters in text format.

**Syntax**

``` sql
UUIDNumToString(binary[, variant = 1])
```

**Arguments**

- `binary` — [FixedString(16)](../../sql-reference/data-types/fixedstring.md) as a binary representation of a UUID.
- `variant` — Integer, representing a variant as specified by [RFC4122](https://datatracker.ietf.org/doc/html/rfc4122#section-4.1.1). 1 = `Big-endian` (default), 2 = `Microsoft`.

**Returned value**

String.

**Usage example**

``` sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```

```response
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

``` sql
SELECT
    '@</a;]~!p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16), 2) AS uuid
```

```response
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ @</a;]~!p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## serverUUID()

Returns the random and unique UUID, which is generated when the server is first started and stored forever. The result writes to the file `uuid` created in the ClickHouse server directory `/var/lib/clickhouse/`. 

**Syntax**

```sql
serverUUID()
```

**Returned value**

- The UUID of the server. 

Type: [UUID](../data-types/uuid.md).

## See Also

- [dictGetUUID](../../sql-reference/functions/ext-dict-functions.md#ext_dict_functions-other)
