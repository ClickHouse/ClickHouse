#include <Storages/System/StorageSystemDataSkippingIndexTypes.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

ColumnsDescription StorageSystemDataSkippingIndexTypes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the data skipping index type, as specified in the TYPE of an INDEX declaration."},
        {"description", std::make_shared<DataTypeString>(), "A high-level description of what the data skipping index type does."},
        {"syntax", std::make_shared<DataTypeString>(), "How the index is declared in an INDEX clause of a CREATE TABLE query."},
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"introduced_in", std::make_shared<DataTypeString>(), "The ClickHouse version in which the index type was first introduced, in the form major.minor."},
        {"related", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related data skipping index types."},
    };
}

void StorageSystemDataSkippingIndexTypes::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = MergeTreeIndexFactory::instance();
    for (const auto & name : factory.getAllRegisteredNames())
    {
        const auto documentation = factory.getDocumentation(name);

        size_t i = 0;
        res_columns[i++]->insert(name);
        res_columns[i++]->insert(documentation.description);
        res_columns[i++]->insert(documentation.syntaxAsString());
        res_columns[i++]->insert(documentation.examplesAsString());
        res_columns[i++]->insert(documentation.introducedInAsString());

        Array related;
        for (const auto & related_name : documentation.related)
            related.push_back(related_name);
        res_columns[i++]->insert(related);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDataSkippingIndexTypes) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "data_skipping_index_types",
    .description = R"DOCS_MD(
Contains the list of data skipping index types supported by the server, along with embedded documentation for each type. A data skipping index type is specified in the `TYPE` of an `INDEX` declaration in a `CREATE TABLE` query and lets ClickHouse skip granules that cannot match a query's condition.

Note that this table lists the available index *types*, whereas [`system.data_skipping_indices`](/reference/system-tables/data_skipping_indices) lists the index instances defined on existing tables.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql title="Query"
SELECT name, syntax
FROM system.data_skipping_index_types
WHERE name IN ('minmax', 'set')
ORDER BY name
```

```text title="Response"
┌─name───┬─syntax───────────────────────────────────────┐
│ minmax │ INDEX name expr TYPE minmax GRANULARITY n     │
│ set    │ INDEX name expr TYPE set(max_rows) GRANULARITY n │
└────────┴──────────────────────────────────────────────┘
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [Data skipping indices](/concepts/features/performance/skip-indexes/skipping-indexes) — Information about data skipping indexes.
- [`system.data_skipping_indices`](/reference/system-tables/data_skipping_indices) — The index instances defined on existing tables.
)DOCS_MD")

}
