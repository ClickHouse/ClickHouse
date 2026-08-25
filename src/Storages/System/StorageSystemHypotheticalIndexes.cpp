#include <Storages/System/StorageSystemHypotheticalIndexes.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/HypotheticalObjectStore.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIndexDeclaration.h>

namespace DB
{

ColumnsDescription StorageSystemHypotheticalIndexes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database",    std::make_shared<DataTypeString>(), "Database name"},
        {"table",       std::make_shared<DataTypeString>(), "Table name"},
        {"name",        std::make_shared<DataTypeString>(), "Index name"},
        {"type",        std::make_shared<DataTypeString>(), "Index type (minmax, set, bloom_filter, etc)"},
        {"type_full",   std::make_shared<DataTypeString>(), "Index type expression with arguments, e.g. bloom_filter(0.01)"},
        {"expression",  std::make_shared<DataTypeString>(), "Index expression"},
        {"granularity", std::make_shared<DataTypeUInt64>(), "Index granularity"},
    };
}

void StorageSystemHypotheticalIndexes::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & store = context->getHypotheticalObjectStore();
    auto entries = store.getAll();

    for (const auto & entry : entries)
    {
        /// Hide entries whose table no longer exists (DROP TABLE).
        String database_name = entry.table_id.getDatabaseName();
        String table_name = entry.table_id.getTableName();
        if (entry.table_id.uuid != UUIDHelpers::Nil)
        {
            auto [db, storage] = DatabaseCatalog::instance().tryGetByUUID(entry.table_id.uuid);
            if (!db || !storage)
                continue;
            database_name = db->getDatabaseName();
            table_name = storage->getStorageID().getTableName();
        }

        size_t col = 0;
        res_columns[col++]->insert(database_name);
        res_columns[col++]->insert(table_name);
        res_columns[col++]->insert(entry.index.name);
        res_columns[col++]->insert(entry.index.type);

        auto * declaration = entry.index.definition_ast ? entry.index.definition_ast->as<ASTIndexDeclaration>() : nullptr;
        auto index_type_ast = declaration ? declaration->getType() : nullptr;
        if (index_type_ast)
            res_columns[col++]->insert(index_type_ast->formatForLogging());
        else
            res_columns[col++]->insertDefault();

        if (auto expression = entry.index.expression_list_ast)
            res_columns[col++]->insert(expression->formatForLogging());
        else
            res_columns[col++]->insertDefault();

        res_columns[col++]->insert(entry.index.granularity);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemHypotheticalIndexes) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "hypothetical_indexes",
    .description = R"DOCS_MD(
Lists every hypothetical (what-if) skip index defined in the current session. See [`CREATE HYPOTHETICAL INDEX`](/reference/statements/hypothetical-index#create-hypothetical-index) and [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif).

The contents are session-scoped: each connection sees only its own hypothetical indexes, and the table is empty when no indexes have been created in the current session.

The current `(database, table)` are resolved by UUID at query time, so they reflect `RENAME TABLE` and entries for dropped tables are hidden automatically.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
CREATE HYPOTHETICAL INDEX i1 ON t (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
CREATE HYPOTHETICAL INDEX i2 ON t (b) TYPE bloom_filter(0.001) GRANULARITY 1;

SELECT database, table, name, type, type_full, expression, granularity
FROM system.hypothetical_indexes;
```

```text
┌─database─┬─table─┬─name─┬─type─────────┬─type_full───────────┬─expression─┬─granularity─┐
│ default  │ t     │ i1   │ bloom_filter │ bloom_filter(0.01)  │ b          │           1 │
│ default  │ t     │ i2   │ bloom_filter │ bloom_filter(0.001) │ b          │           1 │
└──────────┴───────┴──────┴──────────────┴─────────────────────┴────────────┴─────────────┘
```

`type` is the base type name and `type_full` includes the arguments, so users can distinguish between parametrized variants like `bloom_filter(0.01)` and `bloom_filter(0.001)`.
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [`CREATE HYPOTHETICAL INDEX`](/reference/statements/hypothetical-index#create-hypothetical-index)
- [`EXPLAIN WHATIF`](/reference/statements/explain#explain-whatif)
)DOCS_MD")

}
