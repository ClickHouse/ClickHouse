#include <Storages/System/StorageSystemDistributedBucketMaps.h>

#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Distributed/DistributedBucketMap.h>
#include <Storages/StorageDistributed.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/extractTablesFilter.h>


namespace DB
{

ColumnsDescription StorageSystemDistributedBucketMaps::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Name of the database."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table."},
        {"zookeeper_path", std::make_shared<DataTypeString>(), "Path of the bucket map in ZooKeeper (the `bucket_map_zookeeper_path` setting)."},
        {"version", std::make_shared<DataTypeInt32>(), "Version of the ZooKeeper node of the loaded map, or -1 if the map has not been loaded."},
        {"buckets", std::make_shared<DataTypeUInt64>(), "Number of buckets of the loaded map."},
        {"ranges", std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt32>()},
            Names{"first", "last", "shard_num"})),
            "Ranges of buckets of the loaded map: the first and the last bucket of every range and the shard number it is assigned to."},
        {"last_exception", std::make_shared<DataTypeString>(), "Error of the last attempt to load the map, or an empty string if it succeeded."},
    };
}

Block StorageSystemDistributedBucketMaps::getFilterSampleBlock() const
{
    return {
        { {}, std::make_shared<DataTypeString>(), "database" },
        { {}, std::make_shared<DataTypeString>(), "table" },
    };
}

void StorageSystemDistributedBucketMaps::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
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
            if (!distributed_table || !distributed_table->getBucketMapWatcher())
                continue;
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, iterator->name()))
                continue;

            const auto * watcher = distributed_table->getBucketMapWatcher();
            const auto status = watcher->getStatus();

            Array ranges;
            if (status.map)
                for (const auto & range : status.map->ranges)
                    ranges.push_back(Tuple{range.first, range.last, range.shard_num});

            size_t col_num = 0;
            res_columns[col_num++]->insert(database_name);
            res_columns[col_num++]->insert(iterator->name());
            res_columns[col_num++]->insert(watcher->getFullZooKeeperPath());
            res_columns[col_num++]->insert(status.map ? status.map->version : -1);
            res_columns[col_num++]->insert(status.map ? status.map->buckets : 0);
            res_columns[col_num++]->insert(ranges);
            if (status.last_exception)
                res_columns[col_num++]->insert(getExceptionMessage(status.last_exception, false));
            else
                res_columns[col_num++]->insertDefault();
        }
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemDistributedBucketMaps) }
