#include "VFSMigration.h"
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
    if (!disk.garbage_collector->trySetLock()) {

    }
    LOG_INFO(log, "Starting migration for {}", disk.getName());

    // TODO myrrc better granularity: e.g. stop only for table currently processed (e.g. STOP ON VOLUME / STOP table)
    // need to match tables storing data on this disk
    // TODO myrrc stop mutations
    InterpreterSystemQuery interpreter({}, ctx);

    // TODO myrrc respect settings set before migration. E.g. if moves were stopped for table / volume / globally, we
    // shouldn't turn them off
    auto perform = [&](bool start)
    {
        for (const auto & [name, db] : DatabaseCatalog::instance().getDatabases())
            for (const StorageActionBlockType action : ACTIONS)
                interpreter.startStopActionInDatabase(action, start, name, db, ctx, log);
    };
    perform(false);
    SCOPE_EXIT(perform(true));
}
}
