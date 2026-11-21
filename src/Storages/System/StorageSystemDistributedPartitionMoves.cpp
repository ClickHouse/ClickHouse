#include <Storages/System/StorageSystemDistributedPartitionMoves.h>

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Distributed/DistributedPartitionMoves.h>
#include <Storages/StorageDistributed.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/extractTablesFilter.h>


namespace DB
{

ColumnsDescription StorageSystemDistributedPartitionMoves::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Name of the database."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table."},
        {"zookeeper_path", std::make_shared<DataTypeString>(), "Path of the partition moves in ZooKeeper (the `partition_moves_zookeeper_path` setting)."},
        {"version", std::make_shared<DataTypeInt32>(), "Version of the ZooKeeper node of the loaded moves, or -1 if they have not been loaded."},
        {"moves", std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeString>()},
            Names{"partition_id", "source_shard_num", "destination_shard_num", "read_from"})),
            "The loaded moves: the partition ID, the source and the destination shard numbers, and the copy that queries read, `source` or `destination`."},
        {"last_exception", std::make_shared<DataTypeString>(), "Error of the last attempt to load the moves, or an empty string if it succeeded."},
    };
}

Block StorageSystemDistributedPartitionMoves::getFilterSampleBlock() const
{
    return {
        { {}, std::make_shared<DataTypeString>(), "database" },
        { {}, std::make_shared<DataTypeString>(), "table" },
    };
}

void StorageSystemDistributedPartitionMoves::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    const auto database_name_filter = extractNameFilter(predicate, "database", context);
    const auto table_name_filter = extractNameFilter(predicate, "table", context);

    for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        if (database_name_filter && !database_name_filter(database_name))
            continue;

        if (database->isExternal())
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

        for (auto iterator = database->getTablesIterator(context, table_name_filter, /* skip_not_loaded */ false); iterator->isValid(); iterator->next())
        {
            const auto * distributed_table = dynamic_cast<const StorageDistributed *>(iterator->table().get());
            if (!distributed_table || !distributed_table->getPartitionMovesWatcher())
                continue;
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, iterator->name()))
                continue;

            const auto * watcher = distributed_table->getPartitionMovesWatcher();
            const auto status = watcher->getStatus();

            Array moves;
            if (status.moves)
            {
                for (const auto & move : status.moves->moves)
                {
                    String read_from = move.read_from == DistributedPartitionMoves::ReadFrom::Source ? "source" : "destination";
                    moves.push_back(Tuple{move.partition_id, move.source_shard_num, move.destination_shard_num, read_from});
                }
            }

            size_t col_num = 0;
            res_columns[col_num++]->insert(database_name);
            res_columns[col_num++]->insert(iterator->name());
            res_columns[col_num++]->insert(watcher->getFullZooKeeperPath());
            res_columns[col_num++]->insert(status.moves ? status.moves->version : -1);
            res_columns[col_num++]->insert(moves);
            if (status.last_exception)
                res_columns[col_num++]->insert(getExceptionMessage(status.last_exception, false));
            else
                res_columns[col_num++]->insertDefault();
        }
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDistributedPartitionMoves) }
