#include <Storages/System/StorageSystemDictionarySources.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Dictionaries/DictionarySourceFactory.h>

namespace DB
{

ColumnsDescription StorageSystemDictionarySources::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of the dictionary source, as specified in the SOURCE clause."},
        {"description", std::make_shared<DataTypeString>(),
            "A description of what the dictionary source does. "
            "For sources that have a dedicated documentation page, this contains the full Markdown body of that page; "
            "for the remaining sources it is a concise summary."},
        {"syntax", std::make_shared<DataTypeString>(), "The structure of the SOURCE clause used to specify the source. Note that some sources are subject to access control when a dictionary is created from a DDL query (as opposed to a server configuration file); see the `description` of the individual source for details."},
        {"examples", std::make_shared<DataTypeString>(), "Usage examples."},
        {"introduced_in", std::make_shared<DataTypeString>(), "The ClickHouse version in which the source was first introduced, in the form major.minor."},
        {"related", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The names of related dictionary sources."},
    };
}

void StorageSystemDictionarySources::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = DictionarySourceFactory::instance();
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
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDictionarySources) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "dictionary_sources",
    .description = R"DOCS_MD(
Contains the list of dictionary sources supported by the server, along with embedded documentation for each source. A dictionary source determines where the dictionary data is loaded from; it is specified in the `SOURCE` clause of a `CREATE DICTIONARY` query.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql title="Query"
SELECT name, syntax
FROM system.dictionary_sources
WHERE name IN ('clickhouse', 'file')
ORDER BY name
```

```text title="Response"
┌─name───────┬─syntax─────────────────────────────────────────────────────────────────────────┐
│ clickhouse │ SOURCE(CLICKHOUSE(host 'host' port 9000 user 'default' password '' db 'db' table 'table')) │
│ file       │ SOURCE(FILE(path '/path/to/file' format 'CSV'))                                  │
└────────────┴────────────────────────────────────────────────────────────────────────────────┘
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [Dictionary sources](/reference/statements/create/dictionary/sources/overview) — Information about dictionaries and their sources.
)DOCS_MD")

}
