#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationUUID.h>


namespace DB
{

bool DataTypeUUID::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeUUID::doGetSerialization(const SerializationInfoSettings &) const
{
    return SerializationUUID::create();
}

Field DataTypeUUID::getDefault() const
{
    return UUID{};
}

MutableColumnPtr DataTypeUUID::createColumn() const
{
    return ColumnVector<UUID>::create();
}

void registerDataTypeUUID(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("UUID", [] { return DataTypePtr(std::make_shared<DataTypeUUID>()); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
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
However, with sorting by the second half (counter), at least one mark is expected to be returned for all parts, leading to unnecessary disk accesses.
:::

Example:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
└──────────────────────────────────────┘

```

As a workaround, the UUID can be converted to a timestamp extracted from the second half:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (UUIDv7ToDateTime(uuid));
-- Or alternatively:                      [...] PRIMARY KEY (toStartOfHour(UUIDv7ToDateTime(uuid)));

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

Result (assuming same data is inserted):

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
└──────────────────────────────────────┘

```

ORDER BY (UUIDv7ToDateTime(uuid), uuid)

## Generating UUIDs {#generating-uuids}

ClickHouse provides the [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) function to generate random UUID version 4 values.

## Usage Example {#usage-example}

**Example 1**

This example demonstrates the creation of a table with a UUID column and the insertion of a value into the table.

```sql title="Query"
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

```text title="Response"
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
)DOCS_MD",
            .syntax = "UUID",
            .related = {},
        });

    /// `UUID1` is an explicit alias for the current `UUID` type (which sorts by the second half of the value).
    /// The correctly-sorting variant is the separate `UUID2` type. The name `UUID` resolves to one of them
    /// depending on the `uuid_type_version` setting (see `InterpreterCreateQuery`).
    factory.registerAlias("UUID1", "UUID", DataTypeFactory::Case::Insensitive);
}

}
