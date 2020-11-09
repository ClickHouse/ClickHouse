#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemDistributionQueue.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/StorageDistributed.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Common/typeid_cast.h>
#include <Databases/IDatabase.h>


namespace DB
{


NamesAndTypesList StorageSystemDistributionQueue::getNamesAndTypes()
{
    return {
        { "database",              std::make_shared<DataTypeString>() },
        { "table",                 std::make_shared<DataTypeString>() },
        { "data_path",             std::make_shared<DataTypeString>() },
        { "is_blocked",            std::make_shared<DataTypeUInt8>()  },
        { "error_count",           std::make_shared<DataTypeUInt64>() },
        { "data_files",            std::make_shared<DataTypeUInt64>() },
        { "data_compressed_bytes", std::make_shared<DataTypeUInt64>() },
        { "last_exception",        std::make_shared<DataTypeString>() },
    };
}


void StorageSystemDistributionQueue::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const
{
    const auto access = context.getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    std::map<String, std::map<String, StoragePtr>> tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        /// Lazy database can not contain distributed tables
        if (db.second->getEngineName() == "Lazy")
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

        VirtualColumnUtils::filterBlockWithQuery(query_info.query, filtered_block, context);

        if (!filtered_block.rows())
            return;

        col_database_to_filter = filtered_block.getByName("database").column;
        col_table_to_filter = filtered_block.getByName("table").column;
    }

    for (size_t i = 0, tables_size = col_database_to_filter->size(); i < tables_size; ++i)
    {
        String database = (*col_database_to_filter)[i].safeGet<const String &>();
        String table = (*col_table_to_filter)[i].safeGet<const String &>();

        auto & distributed_table = dynamic_cast<StorageDistributed &>(*tables[database][table]);

        for (const auto & status : distributed_table.getDirectoryMonitorsStatuses())
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);
            res_columns[col_num++]->insert(status.path);
            res_columns[col_num++]->insert(status.is_blocked);
            res_columns[col_num++]->insert(status.error_count);
            res_columns[col_num++]->insert(status.files_count);
            res_columns[col_num++]->insert(status.bytes_count);

            if (status.last_exception)
                res_columns[col_num++]->insert(getExceptionMessage(status.last_exception, false));
            else
                res_columns[col_num++]->insertDefault();
        }
    }
}

}
