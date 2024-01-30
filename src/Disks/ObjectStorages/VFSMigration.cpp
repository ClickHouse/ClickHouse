#include "VFSMigration.h"
#include "DiskObjectStorageVFS.h"
#include "Interpreters/ActionLocksManager.h"
#include "Interpreters/DatabaseCatalog.h"
#include "Storages/MergeTree/MergeTreeData.h"
//#include "Storages/StorageReplicatedMergeTree.h"

namespace DB
{
namespace ActionLocks
{
extern const StorageActionBlockType PartsMerge;
extern const StorageActionBlockType PartsFetch;
extern const StorageActionBlockType PartsSend;
extern const StorageActionBlockType ReplicationQueue;
extern const StorageActionBlockType DistributedSend;
extern const StorageActionBlockType PartsTTLMerge;
extern const StorageActionBlockType PartsMove;
extern const StorageActionBlockType PullReplicationLog;
extern const StorageActionBlockType Cleanup;
extern const StorageActionBlockType ViewRefresh;

};
using namespace ActionLocks;

static const auto ACTIONS = {
    PartsMerge,
    PartsFetch,
    PartsSend,
    ReplicationQueue,
    DistributedSend,
    PartsTTLMerge,
    PartsMove,
    PullReplicationLog,
    Cleanup,
    ViewRefresh,
};

VFSMigration::VFSMigration(DiskObjectStorageVFS & disk_, ContextWeakPtr ctx)
    : WithContext(std::move(ctx)), disk(disk_), log(getLogger(fmt::format("VFSMigration({})", disk.getName())))
{
}

void VFSMigration::migrate() const
{
    LOG_INFO(log, "Migrating {}", disk.getName());
    auto ctx = getContext();

    for (const auto & [_, db] : DatabaseCatalog::instance().getDatabases())
        for (auto it = db->getTablesIterator(ctx); it->isValid(); it->next())
            if (StoragePtr table_ptr = it->table()) // Lazy tables may return nullptr
                migrateTable(std::move(table_ptr), *ctx);

    LOG_INFO(log, "Migrated {}", disk.getName());
}

void VFSMigration::migrateTable(StoragePtr table_ptr, const Context & ctx) const
{
    LOG_INFO(log, "Migrating {}", table_ptr->getName());

    auto * const table_casted_ptr = dynamic_cast<MergeTreeData *>(table_ptr.get());
    if (!table_casted_ptr)
    {
        LOG_INFO(log, "Only MergeTree-derived tables are eligible for migration, skipping");
        return;
    }
    MergeTreeData & table = *table_casted_ptr;

    if (auto disks = table.getDisks(); std::ranges::find_if(disks, [&](DiskPtr d) { return d.get() == &disk; }) == disks.end())
    {
        LOG_INFO(log, "Table {} doesn't store data on disk {}, skipping", table.getName(), disk.getName());
        return;
    }

    ActionLocksManagerPtr manager = ctx.getActionLocksManager();
    manager->cleanExpired();

    // We don't need to stop mutations if we temporarily ban replicated fetches
    // TODO ban user to connect to replica while migrating OR ban requests to
    // specific table

    // TODO myrrc possibly better STOP ON VOLUME, research
    // TODO stop only needed actions
    // TODO myrrc respect settings set before migration. E.g. if moves were stopped for table / volume / globally, we shouldn't turn them off
    auto perform = [&](bool start)
    {
        for (const StorageActionBlockType action : ACTIONS)
            if (start)
            {
                manager->remove(table_ptr, action);
                table.onActionLockRemove(action);
            }
            else
                manager->add(table_ptr, action);
    };

    perform(false);
    SCOPE_EXIT(perform(true));

    MetadataStoragePtr metadata_storage = disk.getMetadataStorage();
    VFSLogItem item; // TODO myrrc batch in 1MB sizes (may be done on VFSTransaction side by @mkmkme)

    // TODO myrrc replace with streaming mode, parallelize
    const auto path = fs::path(disk.getPath()) / table.getRelativeDataPath();
    LOG_DEBUG(log, "Will iterate {}", path);
    for (const auto & entry : fs::recursive_directory_iterator{path})
        if (entry.is_regular_file())
            for (const StoredObject & elem : metadata_storage->getStorageObjects(entry.path()))
            {
                const size_t links = fs::hard_link_count(entry);
                if (auto it = item.find(elem.remote_path); it == item.end())
                    item.emplace(elem.remote_path, links);
                else
                    it->second += links;
            }

    disk.zookeeper()->create(disk.traits.log_item, item.serialize(), zkutil::CreateMode::PersistentSequential);
    LOG_INFO(log, "Migrated {}", table_ptr->getName());
}

bool VFSMigration::fromZeroCopy(IStorage & table, zkutil::ZooKeeper & zookeeper) const
{
    (void)table;
    (void)zookeeper;
    return false;
    //auto * const ptr = dynamic_cast<StorageReplicatedMergeTree *>(&table);
    //if (!ptr)
    //    return false;
    //const auto & settings = ptr->getSettings();
    //const DataSourceType disk_type = disk.getDataSourceDescription().type;
    //chassert(!settings->remote_fs_zero_copy_path_compatible_mode);
    //const Strings paths_for_dummy_part
    //    = StorageReplicatedMergeTree::getZeroCopyPartPath(*settings, toString(disk_type), ptr->getTableSharedID(), "dummy_part", "");
    //const String zk_table_path = fs::path(paths_for_dummy_part[0]).parent_path();
    //return zookeeper.exists(zk_table_path);
}
}
