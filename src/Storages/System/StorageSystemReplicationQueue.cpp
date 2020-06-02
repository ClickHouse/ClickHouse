#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/System/StorageSystemReplicationQueue.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Common/typeid_cast.h>
#include <Databases/IDatabase.h>


namespace DB
{


NamesAndTypesList StorageSystemReplicationQueue::getNamesAndTypes()
{
    return {
        /// Table properties.
        { "database",                std::make_shared<DataTypeString>() },
        { "table",                   std::make_shared<DataTypeString>() },
        { "replica_name",            std::make_shared<DataTypeString>() },
        /// Constant element properties.
        { "position",                std::make_shared<DataTypeUInt32>() },
        { "node_name",               std::make_shared<DataTypeString>() },
        { "type",                    std::make_shared<DataTypeString>() },
        { "create_time",             std::make_shared<DataTypeDateTime>() },
        { "required_quorum",         std::make_shared<DataTypeUInt32>() },
        { "source_replica",          std::make_shared<DataTypeString>() },
        { "new_part_name",           std::make_shared<DataTypeString>() },
        { "parts_to_merge",          std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "is_detach",               std::make_shared<DataTypeUInt8>() },
        /// Processing status of item.
        { "is_currently_executing",  std::make_shared<DataTypeUInt8>() },
        { "num_tries",               std::make_shared<DataTypeUInt32>() },
        { "last_exception",          std::make_shared<DataTypeString>() },
        { "last_attempt_time",       std::make_shared<DataTypeDateTime>() },
        { "num_postponed",           std::make_shared<DataTypeUInt32>() },
        { "postpone_reason",         std::make_shared<DataTypeString>() },
        { "last_postpone_time",      std::make_shared<DataTypeDateTime>() },
    };
}


void StorageSystemReplicationQueue::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const
{
    const auto access = context.getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    std::map<String, std::map<String, StoragePtr>> replicated_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        /// Lazy database can not contain replicated tables
        if (db.second->getEngineName() == "Lazy")
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const auto & table = iterator->table();
            if (!table)
                continue;
            if (!dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
                continue;
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, db.first, iterator->name()))
                continue;
            replicated_tables[db.first][iterator->name()] = table;
        }
    }


    MutableColumnPtr col_database_mut = ColumnString::create();
    MutableColumnPtr col_table_mut = ColumnString::create();

    for (auto & db : replicated_tables)
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

    StorageReplicatedMergeTree::LogEntriesData queue;
    String replica_name;

    for (size_t i = 0, tables_size = col_database_to_filter->size(); i < tables_size; ++i)
    {
        String database = (*col_database_to_filter)[i].safeGet<const String &>();
        String table = (*col_table_to_filter)[i].safeGet<const String &>();

        dynamic_cast<StorageReplicatedMergeTree &>(*replicated_tables[database][table]).getQueue(queue, replica_name);

        for (size_t j = 0, queue_size = queue.size(); j < queue_size; ++j)
        {
            const auto & entry = queue[j];

            Array parts_to_merge;
            parts_to_merge.reserve(entry.source_parts.size());
            for (const auto & part_name : entry.source_parts)
                parts_to_merge.push_back(part_name);

            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);
            res_columns[col_num++]->insert(replica_name);
            res_columns[col_num++]->insert(j);
            res_columns[col_num++]->insert(entry.znode_name);
            res_columns[col_num++]->insert(entry.typeToString());
            res_columns[col_num++]->insert(entry.create_time);
            res_columns[col_num++]->insert(entry.quorum);
            res_columns[col_num++]->insert(entry.source_replica);
            res_columns[col_num++]->insert(entry.new_part_name);
            res_columns[col_num++]->insert(parts_to_merge);
            res_columns[col_num++]->insert(entry.detach);
            res_columns[col_num++]->insert(entry.currently_executing);
            res_columns[col_num++]->insert(entry.num_tries);
            res_columns[col_num++]->insert(entry.exception ? getExceptionMessage(entry.exception, false) : "");
            res_columns[col_num++]->insert(UInt64(entry.last_attempt_time));
            res_columns[col_num++]->insert(entry.num_postponed);
            res_columns[col_num++]->insert(entry.postpone_reason);
            res_columns[col_num++]->insert(UInt64(entry.last_postpone_time));
        }
    }
}

}
