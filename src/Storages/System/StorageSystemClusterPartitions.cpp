#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/System/StorageSystemClusterPartitions.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Access/ContextAccess.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>

namespace DB
{

ColumnsDescription StorageSystemClusterPartitions::getColumnsDescription()
{
    return ColumnsDescription
    {
        { "database",        std::make_shared<DataTypeString>(), "Database name" },
        { "table",           std::make_shared<DataTypeString>(), "Table name" },
        { "partition",       std::make_shared<DataTypeString>(), "Partition name" },
        { "all_replicas",    std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "All replicas that contains this partition" },
        { "active_replicas", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Active replicas that contains this partition" },
    };
}


void StorageSystemClusterPartitions::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    std::map<String, std::map<String, StoragePtr>> tables;
    for (const auto & db : DatabaseCatalog::instance().getDatabases())
    {
        if (!db.second->canContainMergeTreeTables())
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, db.first);

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            StoragePtr table = iterator->table();
            if (!table)
                continue;

            if (!dynamic_cast<const StorageReplicatedMergeTree *>(table.get()))
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

    auto get_replicas_array = [](const auto & replicas)
    {
        Array replicas_field;
        replicas_field.reserve(replicas.size());
        for (const auto & replica : replicas)
            replicas_field.emplace_back(replica);
        return replicas_field;
    };

    for (size_t i = 0, tables_size = col_database_to_filter->size(); i < tables_size; ++i)
    {
        String database = (*col_database_to_filter)[i].safeGet<const String &>();
        String table = (*col_table_to_filter)[i].safeGet<const String &>();

        auto & replicated_table = dynamic_cast<StorageReplicatedMergeTree &>(*tables[database][table]);
        const auto & cluster_partitions = replicated_table.getClusterPartitions();

        for (const auto & cluster_partition : cluster_partitions)
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(database);
            res_columns[col_num++]->insert(table);
            res_columns[col_num++]->insert(cluster_partition.getPartitionId());
            res_columns[col_num++]->insert(get_replicas_array(cluster_partition.getAllReplicas()));
            res_columns[col_num++]->insert(get_replicas_array(cluster_partition.getActiveReplicas()));
        }
    }
}

}
