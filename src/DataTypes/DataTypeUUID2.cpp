#include <DataTypes/DataTypeUUID2.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationUUID2.h>


namespace DB
{

bool DataTypeUUID2::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeUUID2::doGetSerialization(const SerializationInfoSettings &) const
{
    return SerializationUUID2::create();
}

Field DataTypeUUID2::getDefault() const
{
    return UUID{};
}

MutableColumnPtr DataTypeUUID2::createColumn() const
{
    return ColumnVector<UUID>::create();
}

void registerDataTypeUUID2(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("UUID2", [] { return DataTypePtr(std::make_shared<DataTypeUUID2>()); }, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
`UUID2` is a variant of the [UUID](/sql-reference/data-types/uuid) data type with correct sorting.

The `UUID` data type sorts by the second half of the value for historical reasons, which is unexpected and, in particular, hurts the performance of primary indexes built on UUIDv7 columns (see the note in the `UUID` documentation).

`UUID2` stores the value so that it sorts by its textual (lexicographic) representation, matching the canonical byte order used by most other systems. It is otherwise fully compatible with `UUID`: it accepts the same textual representation and occupies the same 16 bytes.

The name `UUID` resolves to either the `UUID` (version 1) or the `UUID2` (version 2) type depending on the `uuid_type_version` setting. The resolved concrete type is materialized in the table definition, so reading a table does not depend on the setting.

```sql title="Query"
CREATE TABLE tab (uuid UUID2) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(10);
SELECT * FROM tab ORDER BY uuid;
```

The values are returned in the order of their textual representation, unlike the `UUID` type.
)DOCS_MD",
            .syntax = "UUID2",
            .related = {"UUID"},
        });
}

}
