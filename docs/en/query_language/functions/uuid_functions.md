# Functions for working with UUID

The functions for working with UUID are listed below.

## generateUUIDv4 {#uuid_function-generate}

Generates the [UUID](../../data_types/uuid.md) of [version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

```sql
generateUUIDv4()
```

**Returned value**

The UUID type value.

**Usage example**

This example demonstrates creating a table with the UUID type column and inserting a value into the table.

```sql
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4()

SELECT * FROM t_uuid
```
```text
┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

## toUUID (x)

Converts String type value to UUID type.

```sql
toUUID(String)
```

**Returned value**

The UUID type value.

**Usage example**

```sql
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
```
```text
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## UUIDStringToNum

Accepts a string containing 36 characters in the format `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, and returns it as a set of bytes in a [FixedString(16)](../../data_types/fixedstring.md).

```sql
UUIDStringToNum(String)
```

**Returned value**

FixedString(16)

**Usage examples**

```sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes
```
```text

┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDNumToString

Accepts a [FixedString(16)](../../data_types/fixedstring.md) value, and returns a string containing 36 characters in text format.

```sql
UUIDNumToString(FixedString(16))
```

**Returned value**

String.

**Usage example**

```sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```
```text
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## See also

- [dictGetUUID](ext_dict_functions.md#ext_dict_functions-other)

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/uuid_function/) <!--hide-->
