#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationBool.h>

namespace DB
{

void registerDataTypeDomainBool(DataTypeFactory & factory)
{
    factory.registerSimpleDataTypeCustom("Bool", []
    {
        auto type = DataTypeFactory::instance().get("UInt8");
        return std::make_pair(type, std::make_unique<DataTypeCustomDesc>(
                std::make_unique<DataTypeCustomFixedName>("Bool"), SerializationBool::create(type->getDefaultSerialization())));
    }, DataTypeFactory::Case::Sensitive, Documentation{
        .description = R"DOCS_MD(
Type `bool` is internally stored as UInt8. Possible values are `true` (1), `false` (0).

```sql
SELECT true AS col, toTypeName(col);
┌─col──┬─toTypeName(true)─┐
│ true │ Bool             │
└──────┴──────────────────┘

select true == 1 as col, toTypeName(col);
┌─col─┬─toTypeName(equals(true, 1))─┐
│   1 │ UInt8                       │
└─────┴─────────────────────────────┘
```

```sql
CREATE TABLE test_bool
(
    `A` Int64,
    `B` Bool
)
ENGINE = Memory;

INSERT INTO test_bool VALUES (1, true),(2,0);

SELECT * FROM test_bool;
┌─A─┬─B─────┐
│ 1 │ true  │
│ 2 │ false │
└───┴───────┘
```
)DOCS_MD",
        .syntax = "Bool",
        .examples = {},
        .related = {},
    });

    factory.registerAlias("bool", "Bool", DataTypeFactory::Case::Insensitive);
    factory.registerAlias("boolean", "Bool", DataTypeFactory::Case::Insensitive);
}

}
