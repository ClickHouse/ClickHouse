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


ColumnsDescription StorageSystemReplicationQueue::getColumnsDescription()
{
    return ColumnsDescription
    {
        /// Table properties.
        { "database",                std::make_shared<DataTypeString>(), "Name of the database."},
        { "table",                   std::make_shared<DataTypeString>(), "Name of the table."},
        { "replica_name",            std::make_shared<DataTypeString>(),
            "Replica name in ClickHouse Keeper. Different replicas of the same table have different names."},
        /// Constant element properties.
        { "position",                std::make_shared<DataTypeUInt32>(), "Position of the task in the queue."},
        { "node_name",               std::make_shared<DataTypeString>(), "Node name in ClickHouse Keeper."},
        { "type",                    std::make_shared<DataTypeString>(),
            "Type of the task in the queue, one of: "
            "• GET_PART — Get the part from another replica, "
            "• ATTACH_PART — Attach the part, possibly from our own replica (if found in the detached folder). "
            "You may think of it as a GET_PART with some optimizations as they're nearly identical, "
            "• MERGE_PARTS — Merge the parts, "
            "• DROP_RANGE — Delete the parts in the specified partition in the specified number range. "
            "• CLEAR_COLUMN — NOTE: Deprecated. Drop specific column from specified partition. "
            "• CLEAR_INDEX — NOTE: Deprecated. Drop specific index from specified partition. "
            "• REPLACE_RANGE — Drop a certain range of parts and replace them with new ones. "
            "• MUTATE_PART — Apply one or several mutations to the part. "
            "• ALTER_METADATA — Apply alter modification according to global /metadata and /columns paths."
        },
        { "create_time",             std::make_shared<DataTypeDateTime>(), "Date and time when the task was submitted for execution."},
        { "required_quorum",         std::make_shared<DataTypeUInt32>(), "The number of replicas waiting for the task to complete with confirmation of completion. This column is only relevant for the GET_PARTS task."},
        { "source_replica",          std::make_shared<DataTypeString>(), "Name of the source replica."},
        { "new_part_name",           std::make_shared<DataTypeString>(), "Name of the new part."},
        { "parts_to_merge",          std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Names of parts to merge or update."},
        { "is_detach",               std::make_shared<DataTypeUInt8>(), "The flag indicates whether the DETACH_PARTS task is in the queue."},
        /// Processing status of item.
        { "is_currently_executing",  std::make_shared<DataTypeUInt8>(), "The flag indicates whether a specific task is being performed right now."},
        { "num_tries",               std::make_shared<DataTypeUInt32>(), "The number of failed attempts to complete the task."},
        { "last_exception",          std::make_shared<DataTypeString>(), "Text message about the last error that occurred (if any)."},
        { "last_exception_time",     std::make_shared<DataTypeDateTime>(), "Date and time when the last error occurred."},
        { "last_attempt_time",       std::make_shared<DataTypeDateTime>(), "Date and time when the task was last attempted."},
        { "num_postponed",           std::make_shared<DataTypeUInt32>(), "The number of postponed tasks."},
        { "postpone_reason",         std::make_shared<DataTypeString>(), "The reason why the task was postponed."},
        { "last_postpone_time",      std::make_shared<DataTypeDateTime>(), "Date and time when the task was last postponed."},
        { "merge_type",              std::make_shared<DataTypeString>(), "Type of the current merge. Empty if it's a mutation."},
    };
}


Block StorageSystemReplicationQueue::getFilterSampleBlock() const
{
    return {
        { {}, std::make_shared<DataTypeString>(), "database" },
        { {}, std::make_shared<DataTypeString>(), "table" },
    };
}

void StorageSystemReplicationQueue::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    std::map<String, std::map<String, StoragePtr>> replicated_tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        /// Check if database can contain replicated tables
        if (!db.second->canContainMergeTreeTables())
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

        VirtualColumnUtils::filterBlockWithPredicate(predicate, filtered_block, context);

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
            res_columns[col_num++]->insert(entry.exception ? getExceptionMessage(entry.exception, true) : "");
            res_columns[col_num++]->insert(UInt64(entry.last_exception_time));
            res_columns[col_num++]->insert(UInt64(entry.last_attempt_time));
            res_columns[col_num++]->insert(entry.num_postponed);
            res_columns[col_num++]->insert(entry.postpone_reason);
            res_columns[col_num++]->insert(UInt64(entry.last_postpone_time));

            if (entry.type == ReplicatedMergeTreeLogEntryData::Type::MERGE_PARTS)
                res_columns[col_num++]->insert(toString(entry.merge_type));
            else
                res_columns[col_num++]->insertDefault();
        }
    }
}

}
