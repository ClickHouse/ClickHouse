#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/RocksDB/StorageSystemRocksDB.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <rocksdb/statistics.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}

namespace DB
{


NamesAndTypesList StorageSystemRocksDB::getNamesAndTypes()
{
    return {
        { "database",              std::make_shared<DataTypeString>() },
        { "table",                 std::make_shared<DataTypeString>() },
        { "name",                  std::make_shared<DataTypeString>() },
        { "value",                 std::make_shared<DataTypeUInt64>() },
    };
}


void StorageSystemRocksDB::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    using RocksDBStoragePtr = std::shared_ptr<StorageEmbeddedRocksDB>;
    std::map<String, std::map<String, RocksDBStoragePtr>> tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            StoragePtr table = iterator->table();
            RocksDBStoragePtr rocksdb_table = table ? std::dynamic_pointer_cast<StorageEmbeddedRocksDB>(table) : nullptr;
            if (!rocksdb_table)
                continue;

            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;

            tables[db.first][iterator->name()] = rocksdb_table;
        }
    }

    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();

    for (auto & db : tables)
    {
        for (auto & table : db.second)
        {
            col_database_mut->insert(db.first);
            col_table_mut->insert(table.first);
        }
    }

    ColumnPtr col_database_to_filter = std::move(col_database_mut);
    ColumnPtr col_table_to_filter = std::move(col_table_mut);

    /// Determine what tables are needed by the conditions in the query.
    {
        Block filtered_block
        {
            { col_database_to_filter, std::make_shared<DataTypeString>(), "database" },
            { col_table_to_filter, std::make_shared<DataTypeString>(), "table" },
        };

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return;

        col_database_to_filter = filtered_block.getByName("database").column;
        col_table_to_filter = filtered_block.getByName("table").column;
    }

    bool show_zeros = context->getSettingsRef().system_events_show_zero_values;
    for (size_t i = 0, tables_size = col_database_to_filter->size(); i < tables_size; ++i)
    {
        String database = (*col_database_to_filter)[i].safeGet<const String &>();
        String table = (*col_table_to_filter)[i].safeGet<const String &>();

        auto statistics = tables[database][table]->getRocksDBStatistics();
        if (!statistics)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "RocksDB statistics are not available");

        for (auto [tick, name] : rocksdb::TickersNameMap)
        {
            UInt64 value = statistics->getTickerCount(tick);
            if (!value && !show_zeros)
                continue;

            /// trim "rocksdb."
            if (startsWith(name, "rocksdb."))
                name = name.substr(strlen("rocksdb."));

            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);

            res_columns[col_num++]->insert(name);
            res_columns[col_num++]->insert(value);
        }
    }
}

}
