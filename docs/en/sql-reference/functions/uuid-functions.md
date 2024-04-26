---
slug: /en/sql-reference/functions/uuid-functions
sidebar_position: 205
sidebar_label: UUIDs
---

# Functions for Working with UUIDs

## generateUUIDv4

Generates a [UUID](../data-types/uuid.md) of [version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

**Syntax**

``` sql
generateUUIDv4([x])
```

**Arguments**

- `x` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in any of the [supported data types](../../sql-reference/data-types/index.md#data_types). The expression is used to bypass [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in a query but otherwise ignored. Optional.

**Returned value**

A value of type UUIDv4.

**Example**

First, create a table with a column of type UUID, then insert a generated UUIDv4 into the table.

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;

INSERT INTO tab SELECT generateUUIDv4();

SELECT * FROM tab;
```

```response
┌─────────────────────────────────uuid─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

**Example where multiple UUIDs are generated per row**

```sql
SELECT generateUUIDv4(1), generateUUIDv4(2);

┌─generateUUIDv4(1)────────────────────┬─generateUUIDv4(2)────────────────────┐
│ 2d49dc6e-ddce-4cd0-afb8-790956df54c1 │ 8abf8c13-7dea-4fdf-af3e-0e18767770e6 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## generateUUIDv7

Generates a [UUID](../data-types/uuid.md) of [version 7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format-04).

The generated UUID contains the current Unix timestamp in milliseconds (48 bits), followed by version "7" (4 bits), a counter (42 bit) to distinguish UUIDs within a millisecond (including a variant field "2", 2 bit), and a random field (32 bits).

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                           unix_ts_ms                          |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|          unix_ts_ms           |  ver  |       rand_a          |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|var|                        rand_b                             |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                            rand_b                             |
└─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘
```

**Syntax**

``` sql
generateUUIDv7([x])
```

**Arguments**

- `x` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in any of the [supported data types](../../sql-reference/data-types/index.md#data_types). The expression is used to bypass [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in a query but otherwise ignored. Optional.

**Returned value**

A value of type UUIDv7.

**Example**

First, create a table with a column of type UUID, then insert a generated UUIDv7 into the table.

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;

INSERT INTO tab SELECT generateUUIDv7();

SELECT * FROM tab;
```

```response
┌─────────────────────────────────uuid─┐
│ 018f05af-f4a8-778f-beee-1bedbc95c93b │
└──────────────────────────────────────┘
```

**Example where multiple UUIDs are generated per row**

```sql
SELECT generateUUIDv7(1), generateUUIDv7(2);

┌─generateUUIDv7(1)────────────────────┬─generateUUIDv7(2)────────────────────┐
│ 018f05b1-8c2e-7567-a988-48d09606ae8c │ 018f05b1-8c2e-7946-895b-fcd7635da9a0 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## generateUUIDv7WithCounter

Generates a [UUID](../data-types/uuid.md) of [version 7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format-04).

The generated UUID contains the current Unix timestamp in milliseconds (48 bits), followed by version "7" (4 bits), a counter (42 bit) to distinguish UUIDs within a millisecond (including a variant field "2", 2 bit), and a random field (32 bits).
At any given new timestamp in unix_ts_ms, the counter starts from some random value and then it's being increased by 1 on each new UUID v7 with counter generation until current timestamp changes.
The counter overflow causes unix_ts_ms field increment by 1 and the counter restart from a random value. Counter increment monotony at one timestamp is guaranteed across all `generateUUIDv7WithCounter` functions running simultaneously.
```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                           unix_ts_ms                          |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|          unix_ts_ms           |  ver  |   counter_high_bits   |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|var|                   counter_low_bits                        |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                            rand_b                             |
└─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘
```

**Syntax**

``` sql
generateUUIDv7WithCounter([x])
```

**Arguments**

- `x` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in any of the [supported data types](../../sql-reference/data-types/index.md#data_types). The expression is used to bypass [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in a query but otherwise ignored. Optional.

**Returned value**

A value of type UUIDv7.

**Usage example**

First, create a table with a column of type UUID, then insert a generated UUIDv7 into the table.

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;

INSERT INTO tab SELECT generateUUIDv7WithCounter();

SELECT * FROM tab;
```

```response
┌─────────────────────────────────uuid─┐
│ 018f05c7-56e3-7ac3-93e9-1d93c4218e0e │
└──────────────────────────────────────┘
```

**Example where multiple UUIDs are generated per row**

```sql
SELECT generateUUIDv7WithCounter(1), generateUUIDv7WithCounter(2);

┌─generateUUIDv7WithCounter(1)─────────┬─generateUUIDv7WithCounter(2)─────────┐
│ 018f05c9-4ab8-7b86-b64e-c9f03fbd45d1 │ 018f05c9-4ab8-7b86-b64e-c9f12efb7e16 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## generateUUIDv7WithFastCounter

Generates a [UUID](../data-types/uuid.md) of [version 7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format-04).

This function behaves like `generateUUIDv7WithCounter` but gives no guarantee on counter monotony across different requests running simultaneously. Counter increment monotony at one timestamp is guaranteed only within one thread calling this function to generate many UUIDs.

**Syntax**

``` sql
generateUUIDv7WithFastCounter([x])
```

**Arguments**

- `x` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in any of the [supported data types](../../sql-reference/data-types/index.md#data_types). The resulting value is discarded, but the expression itself if used for bypassing [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in one query. Optional parameter.

**Returned value**

A value of type UUIDv7.

**Usage example**

First, create a table with a column of type UUID, then insert a generated UUIDv7 into the table.

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;

INSERT INTO tab SELECT generateUUIDv7WithFastCounter();

SELECT * FROM tab;
```

```response
┌─────────────────────────────────uuid─┐
│ 018f05e2-e3b2-70cb-b8be-64b09b626d32 │
└──────────────────────────────────────┘
```

**Example where multiple UUIDs are generated per row**

```sql
SELECT generateUUIDv7WithFastCounter(1), generateUUIDv7WithFastCounter(2);

┌─generateUUIDv7WithFastCounter(1)─────┬─generateUUIDv7WithFastCounter(2)─────┐
│ 018f05e1-14ee-7bc5-9906-207153b400b1 │ 018f05e1-14ee-7bc5-9906-2072b8e96758 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## empty

Checks whether the input UUID is empty.

**Syntax**

```sql
empty(UUID)
```

The UUID is considered empty if it contains all zeros (zero UUID).

The function also works for [Arrays](array-functions.md#function-empty) and [Strings](string-functions.md#empty).

**Arguments**

- `x` — A UUID. [UUID](../data-types/uuid.md).

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

The function also works for [Arrays](array-functions.md#function-notempty) or [Strings](string-functions.md#notempty).

**Arguments**

- `x` — A UUID. [UUID](../data-types/uuid.md).

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

## toUUID

Converts a value of type String to a UUID.

``` sql
toUUID(string)
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

## toUUIDOrDefault

**Arguments**

- `string` — String of 36 characters or FixedString(36). [String](../../sql-reference/syntax.md#string).
- `default` — UUID to be used as the default if the first argument cannot be converted to a UUID type. [UUID](/docs/en/sql-reference/data-types/uuid.md).

**Returned value**

UUID

``` sql
toUUIDOrDefault(string, default)
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

## toUUIDOrNull

Takes an argument of type String and tries to parse it into UUID. If failed, returns NULL.

``` sql
toUUIDOrNull(string)
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

## toUUIDOrZero

It takes an argument of type String and tries to parse it into UUID. If failed, returns zero UUID.

``` sql
toUUIDOrZero(string)
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

- `string` — A [String](../../sql-reference/syntax.md#syntax-string-literal) of 36 characters or [FixedString](../../sql-reference/syntax.md#syntax-string-literal)
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

## UUIDToNum

Accepts `UUID` and returns a [FixedString(16)](../../sql-reference/data-types/fixedstring.md) as its binary representation, with its format optionally specified by `variant` (`Big-endian` by default). This function replaces calls to two separate functions `UUIDStringToNum(toString(uuid))` so intermediate conversion from UUID to string is not required to extract bytes from a UUID.

**Syntax**

``` sql
UUIDToNum(uuid[, variant = 1])
```

**Arguments**

- `uuid` — [UUID](../data-types/uuid.md).
- `variant` — Integer, representing a variant as specified by [RFC4122](https://datatracker.ietf.org/doc/html/rfc4122#section-4.1.1). 1 = `Big-endian` (default), 2 = `Microsoft`.

**Returned value**

FixedString(16)

**Usage examples**

``` sql
SELECT
    toUUID('612f3c40-5d3b-217e-707b-6a546a3d7b29') AS uuid,
    UUIDToNum(uuid) AS bytes
```

```response
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

``` sql
SELECT
    toUUID('612f3c40-5d3b-217e-707b-6a546a3d7b29') AS uuid,
    UUIDToNum(uuid, 2) AS bytes
```

```response
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ @</a;]~!p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDv7ToDateTime

Accepts `UUID` having version 7 and extracts the timestamp from it. 

**Syntax**

``` sql
UUIDv7ToDateTime(uuid[, timezone])
```

**Arguments**

- `uuid` — [UUID](../data-types/uuid.md) of version 7.
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional). [String](../../sql-reference/data-types/string.md).

**Returned value**

- Timestamp with milliseconds precision (1970-01-01 00:00:00.000 in case of non version 7 UUID).

Type: [DateTime64(3)](/docs/en/sql-reference/data-types/datetime64.md).

**Usage examples**

``` sql
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'))
```

```response
┌─UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'))─┐
│                                          2024-04-22 15:30:29.048 │
└──────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York')
```

```response
┌─UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York')─┐
│                                                              2024-04-22 08:30:29.048 │
└──────────────────────────────────────────────────────────────────────────────────────┘
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
