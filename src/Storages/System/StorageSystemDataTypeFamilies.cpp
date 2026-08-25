#include <Columns/IColumn.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemDataTypeFamilies.h>

namespace DB
{

ColumnsDescription StorageSystemDataTypeFamilies::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Data type name."},
        {"case_insensitive", std::make_shared<DataTypeUInt8>(), "Property that shows whether you can use a data type name in a query in case insensitive manner or not. For example, `Date` and `date` are both valid."},
        {"alias_to", std::make_shared<DataTypeString>(), "Data type name for which `name` is an alias."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description of what the data type is."},
        {"syntax", std::make_shared<DataTypeString>(), "How the data type is spelled in a query."},
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"introduced_in", std::make_shared<DataTypeString>(), "The ClickHouse version in which the data type was first introduced, in the form major.minor."},
        {"related", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related data types."},
    };
}

void StorageSystemDataTypeFamilies::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = DataTypeFactory::instance();
    auto names = factory.getAllRegisteredNames();
    for (const auto & dtf_name : names)
    {
        const auto documentation = factory.getDocumentation(dtf_name);

        size_t i = 0;
        res_columns[i++]->insert(dtf_name);
        res_columns[i++]->insert(factory.isCaseInsensitive(dtf_name));

        if (factory.isAlias(dtf_name))
            res_columns[i++]->insert(factory.aliasTo(dtf_name));
        else
            res_columns[i++]->insertDefault();

        res_columns[i++]->insert(documentation.description);
        res_columns[i++]->insert(documentation.syntaxAsString());
        res_columns[i++]->insert(documentation.examplesAsString());
        res_columns[i++]->insert(documentation.introducedInAsString());

        Array related;
        for (const auto & name : documentation.related)
            related.push_back(name);
        res_columns[i++]->insert(related);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDataTypeFamilies) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "data_type_families",
    .description = R"DOCS_MD(
Contains information about supported [data types](/reference/data-types/index).
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT name, case_insensitive, alias_to FROM system.data_type_families WHERE alias_to = 'String'
```

```text
┌─name───────┬─case_insensitive─┬─alias_to─┐
│ LONGBLOB   │                1 │ String   │
│ LONGTEXT   │                1 │ String   │
│ TINYTEXT   │                1 │ String   │
│ TEXT       │                1 │ String   │
│ VARCHAR    │                1 │ String   │
│ MEDIUMBLOB │                1 │ String   │
│ BLOB       │                1 │ String   │
│ TINYBLOB   │                1 │ String   │
│ CHAR       │                1 │ String   │
│ MEDIUMTEXT │                1 │ String   │
└────────────┴──────────────────┴──────────┘
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [Syntax](/reference/syntax) — Information about supported syntax.
)DOCS_MD")

}
