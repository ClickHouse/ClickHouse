#include <Storages/System/StorageSystemHypotheticalIndexes.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/HypotheticalIndexStore.h>
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
    const auto & store = context->getHypotheticalIndexStore();
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
