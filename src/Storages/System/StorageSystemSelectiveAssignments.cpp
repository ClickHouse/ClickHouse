#include <Storages/System/StorageSystemSelectiveAssignments.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Databases/IDatabase.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/SelectiveReplication/KeeperAssignment.h>
#include <Storages/MergeTree/SelectiveReplication/MigrationCoordinator.h>
#include <Storages/MergeTree/SelectiveReplication/Constants.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 replication_factor;
}

ColumnsDescription StorageSystemSelectiveAssignments::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database name"},
        {"table", std::make_shared<DataTypeString>(), "Table name"},
        {"partition_id", std::make_shared<DataTypeString>(), "Partition ID"},
        {"assigned_replicas", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Assigned replicas for this partition"},
        {"replication_factor", std::make_shared<DataTypeUInt64>(), "Configured replication factor"},
        {"is_local", std::make_shared<DataTypeUInt8>(), "Whether this partition is assigned to the current replica"},
        {"is_migrating", std::make_shared<DataTypeUInt8>(), "Whether this partition has an active migration (CLONE or SWITCH state)"},
    };
}

void StorageSystemSelectiveAssignments::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * /*predicate*/, std::vector<UInt8> /*columns_mask*/) const
{
    auto component_guard = Coordination::setCurrentComponent("StorageSystemSelectiveAssignments::fillData");
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});

    for (const auto & [database_name, database] : databases)
    {
        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);

        for (auto it = database->getTablesIterator(context); it->isValid(); it->next())
        {
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, it->name()))
                continue;
            auto * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(it->table().get());
            if (!replicated_table)
                continue;

            const auto settings = replicated_table->getSettings();
            UInt64 rf = (*settings)[MergeTreeSetting::replication_factor];
            if (rf == 0)
                continue;

            auto keeper_assign = replicated_table->getReplicaAssignment();
            if (!keeper_assign)
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

            auto assignment_map = keeper_assign->getAssignments(zk, {}, /*force_refresh=*/true);

            /// Build set of partition_ids with active migrations.
            String migrations_path = replicated_table->getZooKeeperPath() + "/selective/" + SelectiveReplication::MIGRATIONS_SUBPATH;
            auto migrating_partitions = PartitionMigrationCoordinator::getActiveMigrationPartitions(zk, migrations_path);

            for (const auto & [partition_id, entry] : assignment_map)
            {
                size_t col = 0;
                res_columns[col++]->insert(database_name);
                res_columns[col++]->insert(it->name());
                res_columns[col++]->insert(partition_id);

                Array replicas_array;
                replicas_array.reserve(entry.replicas.size());
                for (const auto & r : entry.replicas)
                    replicas_array.push_back(r);
                res_columns[col++]->insert(replicas_array);

                res_columns[col++]->insert(rf);

                bool is_local = std::find(
                    entry.replicas.begin(), entry.replicas.end(),
                    replicated_table->getReplicaName()) != entry.replicas.end();
                res_columns[col++]->insert(UInt8(is_local));

                bool is_migrating = migrating_partitions.contains(partition_id);
                res_columns[col++]->insert(UInt8(is_migrating));
            }
        }
    }
}

}
