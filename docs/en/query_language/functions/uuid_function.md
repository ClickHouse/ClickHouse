# Functions for working with UUID

## generateUUIDv4 {#uuid_function-generate}

Generates [UUID](../../data_types/uuid.md) of [version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

```sql
generateUUIDv4()
```

**Returned value**

The UUID value.

**Usage example**

This example demonstrates creating a table with the UUID type column and inserting a value into the table.

``` sql
:) CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

:) INSERT INTO t_uuid SELECT generateUUIDv4()

:) SELECT * FROM t_uuid

┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

## toUUID (x)	
Converts String type value to UUID. 

**Returned value**
UUID.

**Usage example**
``` sql
:) SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid

┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

Note that the String value has to correspond to the UUID format.

``` sql
:) SELECT toUUID('61f0c4045cb3-11e7907ba6006ad3dba0') AS uuid

Received exception from server (version 18.16.0):
Code: 376. DB::Exception: Received from clickhouse-server:9000, 172.17.0.2. DB::Exception: Cannot parse uuid 61f0c4045cb3-11e7907b-a6006ad3dba0: Cannot parse UUID from String. 
```

## UUIDStringToNum

Accepts a string containing 36 characters in the format `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, and returns it as a set of bytes in a FixedString(16).

**Returned value**

FixedString(16)

## UUIDNumToString

## dictGetUUID

## dictGetUUIDOrDefault	

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/uuid_function/) <!--hide-->
