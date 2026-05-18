#include <Storages/System/StorageSystemHypotheticalIndexes.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

ColumnsDescription StorageSystemHypotheticalIndexes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Index name"},
        {"database", std::make_shared<DataTypeString>(), "Database name"},
        {"table", std::make_shared<DataTypeString>(), "Table name"},
        {"type", std::make_shared<DataTypeString>(), "Index type (minmax, set, bloom_filter, etc)"},
        {"expression", std::make_shared<DataTypeString>(), "Index expression"},
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
        /// Resolve current (database, table) via UUID so the row reflects the live
        /// name after RENAME, stored names are only used for dropped tables
        String database_name = entry.table_id.getDatabaseName();
        String table_name = entry.table_id.getTableName();
        if (entry.table_id.uuid != UUIDHelpers::Nil)
        {
            auto [db, storage] = DatabaseCatalog::instance().tryGetByUUID(entry.table_id.uuid);
            if (db && storage)
            {
                database_name = db->getDatabaseName();
                table_name = storage->getStorageID().getTableName();
            }
        }

        size_t col = 0;
        res_columns[col++]->insert(entry.index.name);
        res_columns[col++]->insert(database_name);
        res_columns[col++]->insert(table_name);
        res_columns[col++]->insert(entry.index.type);

        String expr_str;
        if (entry.index.expression_list_ast)
        {
            WriteBufferFromString buf(expr_str);
            IAST::FormatSettings fmt_settings(/* one_line = */ true);
            entry.index.expression_list_ast->format(buf, fmt_settings);
        }
        res_columns[col++]->insert(expr_str);

        res_columns[col++]->insert(entry.index.granularity);
    }
}

}
