---
slug: /en/sql-reference/functions/uuid-functions
sidebar_position: 205
sidebar_label: UUIDs
---

# Functions for Working with UUIDs

## generateUUIDv4

Generates a [version 4](https://tools.ietf.org/html/rfc4122#section-4.4) [UUID](../data-types/uuid.md).

**Syntax**

``` sql
generateUUIDv4([expr])
```

**Arguments**

- `expr` — An arbitrary [expression](../../sql-reference/syntax.md#syntax-expressions) used to bypass [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in a query. The value of the expression has no effect on the returned UUID. Optional.

**Returned value**

A value of type UUIDv4.

**Example**

First, create a table with a column of type UUID, then insert a generated UUIDv4 into the table.

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;

INSERT INTO tab SELECT generateUUIDv4();

SELECT * FROM tab;
```

Result:

```response
┌─────────────────────────────────uuid─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

**Example with multiple UUIDs generated per row**

```sql
SELECT generateUUIDv4(1), generateUUIDv4(2);

┌─generateUUIDv4(1)────────────────────┬─generateUUIDv4(2)────────────────────┐
│ 2d49dc6e-ddce-4cd0-afb8-790956df54c1 │ 8abf8c13-7dea-4fdf-af3e-0e18767770e6 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## generateUUIDv7 {#generateUUIDv7}

Generates a [version 7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format-04) [UUID](../data-types/uuid.md).

The generated UUID contains the current Unix timestamp in milliseconds (48 bits), followed by version "7" (4 bits), a counter (42 bit) to distinguish UUIDs within a millisecond (including a variant field "2", 2 bit), and a random field (32 bits).
For any given timestamp (unix_ts_ms), the counter starts at a random value and is incremented by 1 for each new UUID until the timestamp changes.
In case the counter overflows, the timestamp field is incremented by 1 and the counter is reset to a random new start value.

Function `generateUUIDv7` guarantees that the counter field within a timestamp increments monotonically across all function invocations in concurrently running threads and queries.

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

:::note
As of April 2024, version 7 UUIDs are in draft status and their layout may change in future.
:::

**Syntax**

``` sql
generateUUIDv7([expr])
```

**Arguments**

- `expr` — An arbitrary [expression](../../sql-reference/syntax.md#syntax-expressions) used to bypass [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in a query. The value of the expression has no effect on the returned UUID. Optional.

**Returned value**

A value of type UUIDv7.

**Example**

First, create a table with a column of type UUID, then insert a generated UUIDv7 into the table.

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;

INSERT INTO tab SELECT generateUUIDv7();

SELECT * FROM tab;
```

Result:

```response
┌─────────────────────────────────uuid─┐
│ 018f05af-f4a8-778f-beee-1bedbc95c93b │
└──────────────────────────────────────┘
```

**Example with multiple UUIDs generated per row**

```sql
SELECT generateUUIDv7(1), generateUUIDv7(2);

┌─generateUUIDv7(1)────────────────────┬─generateUUIDv7(2)────────────────────┐
│ 018f05c9-4ab8-7b86-b64e-c9f03fbd45d1 │ 018f05c9-4ab8-7b86-b64e-c9f12efb7e16 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## generateUUIDv7ThreadMonotonic

Generates a [UUID](../data-types/uuid.md) of [version 7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format-04).

The generated UUID contains the current Unix timestamp in milliseconds (48 bits), followed by version "7" (4 bits), a counter (42 bit) to distinguish UUIDs within a millisecond (including a variant field "2", 2 bit), and a random field (32 bits).
For any given timestamp (unix_ts_ms), the counter starts at a random value and is incremented by 1 for each new UUID until the timestamp changes.
In case the counter overflows, the timestamp field is incremented by 1 and the counter is reset to a random new start value.

This function behaves like [generateUUIDv7](#generateUUIDv7) but gives no guarantee on counter monotony across different simultaneous requests.
Monotonicity within one timestamp is guaranteed only within the same thread calling this function to generate UUIDs.

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

:::note
As of April 2024, version 7 UUIDs are in draft status and their layout may change in future.
:::

**Syntax**

``` sql
generateUUIDv7ThreadMonotonic([expr])
```

**Arguments**

- `expr` — An arbitrary [expression](../../sql-reference/syntax.md#syntax-expressions) used to bypass [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in a query. The value of the expression has no effect on the returned UUID. Optional.

**Returned value**

A value of type UUIDv7.

**Usage example**

First, create a table with a column of type UUID, then insert a generated UUIDv7 into the table.

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;

INSERT INTO tab SELECT generateUUIDv7ThreadMonotonic();

SELECT * FROM tab;
```

Result:

```response
┌─────────────────────────────────uuid─┐
│ 018f05e2-e3b2-70cb-b8be-64b09b626d32 │
└──────────────────────────────────────┘
```

**Example with multiple UUIDs generated per row**

```sql
SELECT generateUUIDv7ThreadMonotonic(1), generateUUIDv7ThreadMonotonic(2);

┌─generateUUIDv7ThreadMonotonic(1)─────┬─generateUUIDv7ThreadMonotonic(2)─────┐
│ 018f05e1-14ee-7bc5-9906-207153b400b1 │ 018f05e1-14ee-7bc5-9906-2072b8e96758 │
└──────────────────────────────────────┴──────────────────────────────────────┘
```

## generateUUIDv7NonMonotonic

Generates a [UUID](../data-types/uuid.md) of [version 7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format-04).

The generated UUID contains the current Unix timestamp in milliseconds (48 bits), followed by version "7" (4 bits) and a random field (76 bits, including a 2-bit variant field "2").

This function is the fastest `generateUUIDv7*` function but it gives no monotonicity guarantees within a timestamp.

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

:::note
As of April 2024, version 7 UUIDs are in draft status and their layout may change in future.
:::

**Syntax**

``` sql
generateUUIDv7NonMonotonic([expr])
```

**Arguments**

- `expr` — An arbitrary [expression](../../sql-reference/syntax.md#syntax-expressions) used to bypass [common subexpression elimination](../../sql-reference/functions/index.md#common-subexpression-elimination) if the function is called multiple times in a query. The value of the expression has no effect on the returned UUID. Optional.

**Returned value**

A value of type UUIDv7.

**Example**

First, create a table with a column of type UUID, then insert a generated UUIDv7 into the table.

``` sql
CREATE TABLE tab (uuid UUID) ENGINE = Memory;

INSERT INTO tab SELECT generateUUIDv7NonMonotonic();

SELECT * FROM tab;
```

Result:

```response
┌─────────────────────────────────uuid─┐
│ 018f05af-f4a8-778f-beee-1bedbc95c93b │
└──────────────────────────────────────┘
```

**Example with multiple UUIDs generated per row**

```sql
SELECT generateUUIDv7NonMonotonic(1), generateUUIDv7NonMonotonic(2);

┌─generateUUIDv7NonMonotonic(1) ───────┬─generateUUIDv7(2)NonMonotonic────────┐
│ 018f05b1-8c2e-7567-a988-48d09606ae8c │ 018f05b1-8c2e-7946-895b-fcd7635da9a0 │
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

Result:

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

Result:

```response
┌─toUUIDOrDefault('61f0c404-5cb3-11e7-907b-a6006ad3dba0', CAST('59f0c404-5cb3-11e7-907b-a6006ad3dba0', 'UUID'))─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0                                                                          │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

This second example returns the second argument (the provided default UUID) as the first argument cannot be converted to a UUID type:

```sql
SELECT toUUIDOrDefault('-----61f0c404-5cb3-11e7-907b-a6006ad3dba0', cast('59f0c404-5cb3-11e7-907b-a6006ad3dba0' as UUID));
```

Result:

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

Result:

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

Result:

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

Result:

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

Result:

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

Result:

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

Result:

```response
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ @</a;]~!p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## UUIDToNum

Accepts a [UUID](../../sql-reference/data-types/uuid.md) and returns its binary representation as a [FixedString(16)](../../sql-reference/data-types/fixedstring.md), with its format optionally specified by `variant` (`Big-endian` by default). This function replaces calls to two separate functions `UUIDStringToNum(toString(uuid))` so no intermediate conversion from UUID to string is required to extract bytes from a UUID.

**Syntax**

``` sql
UUIDToNum(uuid[, variant = 1])
```

**Arguments**

- `uuid` — [UUID](../data-types/uuid.md).
- `variant` — Integer, representing a variant as specified by [RFC4122](https://datatracker.ietf.org/doc/html/rfc4122#section-4.1.1). 1 = `Big-endian` (default), 2 = `Microsoft`.

**Returned value**

The binary representation of the UUID.

**Usage examples**

``` sql
SELECT
    toUUID('612f3c40-5d3b-217e-707b-6a546a3d7b29') AS uuid,
    UUIDToNum(uuid) AS bytes
```

Result:

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

Result:

```response
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ @</a;]~!p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDv7ToDateTime

Returns the timestamp component of a UUID version 7.

**Syntax**

``` sql
UUIDv7ToDateTime(uuid[, timezone])
```

**Arguments**

- `uuid` — [UUID](../data-types/uuid.md) of version 7.
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional). [String](../../sql-reference/data-types/string.md).

**Returned value**

- Timestamp with milliseconds precision. If the UUID is not a valid version 7 UUID, it returns 1970-01-01 00:00:00.000.

Type: [DateTime64(3)](/docs/en/sql-reference/data-types/datetime64.md).

**Usage examples**

``` sql
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'))
```

Result:

```response
┌─UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'))─┐
│                                          2024-04-22 15:30:29.048 │
└──────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York')
```

Result:

```response
┌─UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York')─┐
│                                                              2024-04-22 08:30:29.048 │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

## serverUUID()

Returns the random UUID generated during the first start of the ClickHouse server. The UUID is stored in file `uuid` in the ClickHouse server directory (e.g. `/var/lib/clickhouse/`) and retained between server restarts.

**Syntax**

```sql
serverUUID()
```

**Returned value**

- The UUID of the server.

Type: [UUID](../data-types/uuid.md).

## See also

- [dictGetUUID](../../sql-reference/functions/ext-dict-functions.md#ext_dict_functions-other)
