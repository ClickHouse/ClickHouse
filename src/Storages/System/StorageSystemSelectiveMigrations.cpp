#include <Storages/System/StorageSystemSelectiveMigrations.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Databases/IDatabase.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/SelectiveReplication/MigrationCoordinator.h>
#include <Storages/MergeTree/SelectiveReplication/Constants.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 replication_factor;
}

ColumnsDescription StorageSystemSelectiveMigrations::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database name"},
        {"table", std::make_shared<DataTypeString>(), "Table name"},
        {"migration_id", std::make_shared<DataTypeString>(), "UUID of the migration"},
        {"partition_id", std::make_shared<DataTypeString>(), "Partition being migrated"},
        {"source_replica", std::make_shared<DataTypeString>(), "Source replica name"},
        {"target_replica", std::make_shared<DataTypeString>(), "Target replica name"},
        {"state", std::make_shared<DataTypeString>(), "Current state: CLONE, SWITCH, CLEANUP, DONE, FAILED"},
        {"coordinator", std::make_shared<DataTypeString>(), "Replica coordinating the migration"},
        {"snapshot_parts", std::make_shared<DataTypeUInt64>(), "Number of parts in the source snapshot"},
        {"created_at", std::make_shared<DataTypeDateTime>(), "Timestamp when migration was created"},
    };
}

void StorageSystemSelectiveMigrations::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * /*predicate*/, std::vector<UInt8> /*columns_mask*/) const
{
    auto component_guard = Coordination::setCurrentComponent("StorageSystemSelectiveMigrations::fillData");
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});

    for (const auto & [database_name, database] : databases)
    {
        for (auto it = database->getTablesIterator(context); it->isValid(); it->next())
        {
            const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, it->name()))
                continue;
            auto * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(it->table().get());
            if (!replicated_table)
                continue;

            const auto settings = replicated_table->getSettings();
            UInt64 rf = (*settings)[MergeTreeSetting::replication_factor];
            if (rf == 0)
                continue;

            zkutil::ZooKeeperPtr zk;
            try
            {
                zk = context->getDefaultOrAuxiliaryZooKeeper(replicated_table->getZooKeeperName());
            }
            catch (const Coordination::Exception &)
            {
                continue;
            }

            String zk_path = replicated_table->getZooKeeperPath();

            String migrations_path = zk_path + "/selective/" + SelectiveReplication::MIGRATIONS_SUBPATH;

            Coordination::Stat stat;
            Strings migration_ids;
            auto rc = zk->tryGetChildren(migrations_path, migration_ids, &stat);
            if (rc != Coordination::Error::ZOK)
                continue;

            for (const auto & migration_id : migration_ids)
            {
                String migration_path = migrations_path + "/" + migration_id;

                /// Migration data is stored as a single JSON node, not sub-nodes.
                MigrationMetadata meta;
                if (!MigrationMetadata::read(zk, migration_path, meta))
                    continue;

                UInt64 snapshot_parts = meta.source_parts_snapshot.size();

                size_t col = 0;
                res_columns[col++]->insert(database_name);
                res_columns[col++]->insert(it->name());
                res_columns[col++]->insert(migration_id);
                res_columns[col++]->insert(meta.partition_id);
                res_columns[col++]->insert(meta.source_replica);
                res_columns[col++]->insert(meta.target_replica);
                res_columns[col++]->insert(meta.state);
                res_columns[col++]->insert(meta.coordinator);
                res_columns[col++]->insert(snapshot_parts);

                /// Parse created_at as unix timestamp for DataTypeDateTime.
                UInt32 created_at_ts = 0;
                if (!meta.created_at.empty())
                {
                    try { created_at_ts = static_cast<UInt32>(std::stoul(meta.created_at)); }
                    catch (const std::exception &)
                    {
                        tryLogCurrentException("StorageSystemSelectiveMigrations", "Failed to parse created_at: " + meta.created_at);
                    }
                }
                res_columns[col++]->insert(created_at_ts);
            }
        }
    }
}

}
