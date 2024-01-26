#include "VFSMigration.h"
#include "DiskObjectStorageVFS.h"
#include "Interpreters/ActionLocksManager.h"
#include "Interpreters/Context.h"
#include "Interpreters/DatabaseCatalog.h"
#include "Interpreters/InterpreterSystemQuery.h"

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

void VFSMigration::migrateTable(StoragePtr table) const
{
    LOG_INFO(log, "Migrating {}", table->getName());
    auto manager = ctx->getActionLocksManager();
    manager->cleanExpired();

    // TODO myrrc possibly better STOP ON VOLUME, research
    // TODO myrrc stop mutations
    // TODO stop only needed actions
    // TODO myrrc respect settings set before migration. E.g. if moves were stopped for table / volume / globally, we shouldn't turn them off
    auto perform = [&](bool start)
    {
        for (const StorageActionBlockType action : ACTIONS)
            if (start)
            {
                manager->remove(table, action);
                table->onActionLockRemove(action);
            }
            else
                manager->add(table, action);
    };

    perform(false);
    SCOPE_EXIT(perform(true));
}
}
