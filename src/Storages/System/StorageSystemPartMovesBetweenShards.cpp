#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/System/StorageSystemPartMovesBetweenShards.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/typeid_cast.h>


namespace DB
{


ColumnsDescription StorageSystemPartMovesBetweenShards::getColumnsDescription()
{
    return ColumnsDescription
    {
        /// Table properties.
        { "database",                std::make_shared<DataTypeString>(), "The name of the database where move is performed."},
        { "table",                   std::make_shared<DataTypeString>(), "The name of the table where move is performed."},

        /// Constant element properties.
        { "task_name",               std::make_shared<DataTypeString>(), "The name of the moving task."},
        { "task_uuid",               std::make_shared<DataTypeUUID>(), "The identifier of the moving task."},
        { "create_time",             std::make_shared<DataTypeDateTime>(), "The time when the task was created."},
        { "part_name",               std::make_shared<DataTypeString>(), "The name of the part which is in a process of moving."},
        { "part_uuid",               std::make_shared<DataTypeUUID>(), "The UUID of the part which is in a process of moving."},
        { "to_shard",                std::make_shared<DataTypeString>(), "The name of the destination shard."},
        { "dst_part_name",           std::make_shared<DataTypeString>(), "The result part name."},

        /// Processing status of item.
        { "update_time",             std::make_shared<DataTypeDateTime>(), "The last time update was performed."},
        { "state",                   std::make_shared<DataTypeString>(), "The current state of the move."},
        { "rollback",                std::make_shared<DataTypeUInt8>(), "The flag which indicated whether the operation was rolled back."},
        { "num_tries",               std::make_shared<DataTypeUInt32>(), "The number of tries to complete the operation."},
        { "last_exception",          std::make_shared<DataTypeString>(), "The last exception name if any."},
    };
}


Block StorageSystemPartMovesBetweenShards::getFilterSampleBlock() const
{
    return {
        { {}, std::make_shared<DataTypeString>(), "database" },
        { {}, std::make_shared<DataTypeString>(), "table" },
    };
}

void StorageSystemPartMovesBetweenShards::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
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

    for (size_t i = 0, tables_size = col_database_to_filter->size(); i < tables_size; ++i)
    {
        String database = (*col_database_to_filter)[i].safeGet<const String &>();
        String table = (*col_table_to_filter)[i].safeGet<const String &>();

        auto moves = dynamic_cast<StorageReplicatedMergeTree &>(*replicated_tables[database][table]).getPartMovesBetweenShardsEntries();

        for (auto & entry : moves)
        {
            size_t col_num = 0;

            /// Table properties.
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);

            /// Constant element properties.
            res_columns[col_num++]->insert(entry.znode_name);
            res_columns[col_num++]->insert(entry.task_uuid);
            res_columns[col_num++]->insert(entry.create_time);
            res_columns[col_num++]->insert(entry.part_name);
            res_columns[col_num++]->insert(entry.part_uuid);
            res_columns[col_num++]->insert(entry.to_shard);
            res_columns[col_num++]->insert(entry.dst_part_name);

            /// Processing status of item.
            res_columns[col_num++]->insert(entry.update_time);
            res_columns[col_num++]->insert(entry.state.toString());
            res_columns[col_num++]->insert(entry.rollback);
            res_columns[col_num++]->insert(entry.num_tries);
            res_columns[col_num++]->insert(entry.last_exception_msg);
        }
    }
}

}
