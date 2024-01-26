#include "VFSMigration.h"
#include "DiskObjectStorageVFS.h"
#include "Interpreters/ActionLocksManager.h"
#include "Interpreters/DatabaseCatalog.h"
#include "Storages/StorageReplicatedMergeTree.h"

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

void VFSMigration::migrate() const
{
    LOG_INFO(log, "Migrating {}", disk.getName());

    for (const auto & [name, db] : DatabaseCatalog::instance().getDatabases())
    {
        LOG_INFO(log, "Migrating {}", name);
        for (auto it = db->getTablesIterator(ctx); it->isValid(); it->next())
        {
            StoragePtr table_ptr = it->table();
            if (!table_ptr) // Lazy table
                continue;
            migrateTable(std::move(table_ptr));
        }
    }
}

void VFSMigration::migrateTable(StoragePtr table_ptr) const
{
    LOG_INFO(log, "Migrating {}", table_ptr->getName());

    auto * table_casted_ptr = typeid_cast<StorageReplicatedMergeTree *>(table_ptr.get());
    if (!table_casted_ptr)
    {
        LOG_INFO(log, "Only StorageReplicatedMergeTree is eligible for migration, skipping");
        return;
    }
    StorageReplicatedMergeTree & table = *table_casted_ptr;

    if (auto disks = table.getDisks(); std::ranges::find_if(disks, [&](DiskPtr d) { return d.get() == &disk; }) == disks.end())
    {
        LOG_INFO(log, "Table {} doesn't store data on disk {}, skipping", table.getName(), disk.getName());
        return;
    }

    ActionLocksManagerPtr manager = ctx->getActionLocksManager();
    manager->cleanExpired();
    // TODO support 0copy

    // TODO myrrc possibly better STOP ON VOLUME, research
    // TODO myrrc stop mutations
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

    VFSLogItem item;

    Strings paths; // TODO myrrc replace with streaming mode, parallelize
    const String path = disk.getPath() + table.getRelativeDataPath();
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
}
}
