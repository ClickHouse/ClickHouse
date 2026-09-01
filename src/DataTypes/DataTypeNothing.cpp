#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/Serializations/SerializationNothing.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnNothing.h>


namespace DB
{

MutableColumnPtr DataTypeNothing::createColumn() const
{
    return ColumnNothing::create(0);
}

bool DataTypeNothing::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeNothing::doGetSerialization(const SerializationInfoSettings &) const
{
    return SerializationNothing::create();
}


void registerDataTypeNothing(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("Nothing", [] { return DataTypePtr(std::make_shared<DataTypeNothing>()); }, DataTypeFactory::Case::Sensitive, Documentation{
            .description = R"DOCS_MD(
The only purpose of this data type is to represent cases where a value is not expected. So you can't create a `Nothing` type value.

For example, literal [NULL](/sql-reference/syntax#null) has type of `Nullable(Nothing)`. See more about [Nullable](../../../sql-reference/data-types/nullable.md).

The `Nothing` type can also used to denote empty arrays:

```sql
SELECT toTypeName(array())
```

```text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```
)DOCS_MD",
            .syntax = "Nothing",
            .examples = {},
            .related = {},
        });
}

}
