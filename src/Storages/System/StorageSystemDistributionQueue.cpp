#include <Columns/ColumnString.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemDistributionQueue.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDistributed.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>

namespace DB
{


ColumnsDescription StorageSystemDistributionQueue::getColumnsDescription()
{
    return ColumnsDescription
    {
        { "database",              std::make_shared<DataTypeString>(), "Name of the database."},
        { "table",                 std::make_shared<DataTypeString>(), "Name of the table."},
        { "data_path",             std::make_shared<DataTypeString>(), "Path to the folder with local files."},
        { "is_blocked",            std::make_shared<DataTypeUInt8>(), "Flag indicates whether sending local files to the server is blocked."},
        { "error_count",           std::make_shared<DataTypeUInt64>(), "Number of errors."},
        { "data_files",            std::make_shared<DataTypeUInt64>(), "Number of local files in a folder."},
        { "data_compressed_bytes", std::make_shared<DataTypeUInt64>(), "Size of compressed data in local files, in bytes."},
        { "broken_data_files",            std::make_shared<DataTypeUInt64>(), "Number of files that has been marked as broken (due to an error)."},
        { "broken_data_compressed_bytes", std::make_shared<DataTypeUInt64>(), "Size of compressed data in broken files, in bytes."},
        { "last_exception",        std::make_shared<DataTypeString>(), "Text message about the last error that occurred (if any)."},
        { "last_exception_time",        std::make_shared<DataTypeDateTime>(), "Time when last exception occurred."},
    };
}

Block StorageSystemDistributionQueue::getFilterSampleBlock() const
{
    return {
        { {}, std::make_shared<DataTypeString>(), "database" },
        { {}, std::make_shared<DataTypeString>(), "table" },
    };
}

void StorageSystemDistributionQueue::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    std::map<String, std::map<String, StoragePtr>> tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        /// Check if database can contain distributed tables
        if (db.second->isExternal())
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            StoragePtr table = iterator->table();
            if (!table)
                continue;

            if (!dynamic_cast<const StorageDistributed *>(table.get()))
                continue;
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;
            tables[db.first][iterator->name()] = table;
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

        VirtualColumnUtils::filterBlockWithPredicate(predicate, filtered_block, context);

        if (!filtered_block.rows())
            return;

        col_database_to_filter = filtered_block.getByName("database").column;
        col_table_to_filter = filtered_block.getByName("table").column;
    }

    for (size_t i = 0, tables_size = col_database_to_filter->size(); i < tables_size; ++i)
    {
        String database = (*col_database_to_filter)[i].safeGet<String>();
        String table = (*col_table_to_filter)[i].safeGet<String>();

        auto & distributed_table = dynamic_cast<StorageDistributed &>(*tables[database][table]);

        for (const auto & status : distributed_table.getDirectoryQueueStatuses())
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);
            res_columns[col_num++]->insert(status.path);
            res_columns[col_num++]->insert(status.is_blocked);
            res_columns[col_num++]->insert(status.error_count);
            res_columns[col_num++]->insert(status.files_count);
            res_columns[col_num++]->insert(status.bytes_count);
            res_columns[col_num++]->insert(status.broken_files_count);
            res_columns[col_num++]->insert(status.broken_bytes_count);

            if (status.last_exception)
                res_columns[col_num++]->insert(getExceptionMessage(status.last_exception, false));
            else
                res_columns[col_num++]->insertDefault();
            res_columns[col_num++]->insert(static_cast<UInt32>(std::chrono::system_clock::to_time_t(status.last_exception_time)));
        }
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDistributionQueue) }
