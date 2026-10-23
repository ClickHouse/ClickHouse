#include <Interpreters/ActionLocksManager.h>
#include <Storages/IStorage.h>
#include <Common/ActionBlocker.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <utility>
#include <vector>

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
    extern const StorageActionBlockType VirtualPartsUpdate;
    extern const StorageActionBlockType ReduceBlockingParts;
    extern const StorageActionBlockType ViewRefreshPause;
    extern const StorageActionBlockType StreamConsume;
}

namespace
{
class StoppedActionsTestStorage final : public IStorage
{
public:
    explicit StoppedActionsTestStorage(ActionBlocker & blocker_)
        : IStorage(StorageID("test", "stopped_actions")), blocker(blocker_)
    {
    }

    String getName() const override { return "StoppedActionsTestStorage"; }
    ActionLock getActionLock(StorageActionBlockType) override { return blocker.cancel(); }

private:
    ActionBlocker & blocker;
};
}

TEST(StoppedActions, DisplayNamesAndReadOnlyObservation)
{
    /// Some action types have no SQL-accessible implementation in this build. Exercise the
    /// display mapping directly without changing which locks real engines register.
    const std::vector<std::pair<StorageActionBlockType, String>> action_names = {
        {ActionLocks::PartsMerge, "merges"},
        {ActionLocks::PartsFetch, "fetches"},
        {ActionLocks::PartsSend, "replicated_sends"},
        {ActionLocks::ReplicationQueue, "replication_queue"},
        {ActionLocks::DistributedSend, "distributed_sends"},
        {ActionLocks::PartsTTLMerge, "ttl_merges"},
        {ActionLocks::PartsMove, "moves"},
        {ActionLocks::PullReplicationLog, "pull_replication_log"},
        {ActionLocks::Cleanup, "cleanup"},
        {ActionLocks::ViewRefresh, "view_refresh"},
        {ActionLocks::VirtualPartsUpdate, "virtual_parts_update"},
        {ActionLocks::ReduceBlockingParts, "reduce_blocking_parts"},
        {ActionLocks::ViewRefreshPause, "view_refresh_pause"},
        {ActionLocks::StreamConsume, "streaming_consumption"},
    };
    ActionLocksManager manager(getContext().context);
    ActionBlocker blocker;
    auto storage = std::make_shared<StoppedActionsTestStorage>(blocker);
    auto other = std::make_shared<StoppedActionsTestStorage>(blocker);
    EXPECT_TRUE(manager.getStoppedActions(storage).empty());

    Names expected;
    for (const auto & [type, name] : action_names)
    {
        manager.add(storage, type);
        expected.push_back(name);
    }
    std::ranges::sort(expected);
    manager.add(storage, ActionLocks::PartsMerge);
    EXPECT_EQ(manager.getStoppedActions(storage), expected);
    EXPECT_EQ(manager.getStoppedActions(storage), expected);
    EXPECT_TRUE(blocker.isCancelled());
    /// Sharing a blocker or a table name does not create a registry entry for another instance.
    EXPECT_TRUE(manager.getStoppedActions(other).empty());

    for (const auto & [type, _] : action_names)
        manager.remove(storage, type);
    EXPECT_TRUE(manager.getStoppedActions(storage).empty());
    EXPECT_FALSE(blocker.isCancelled());
}

TEST(StoppedActions, ExpiredLocksBeforeCleanup)
{
    ActionLocksManager manager(getContext().context);
    auto blocker = std::make_unique<ActionBlocker>();
    auto storage = std::make_shared<StoppedActionsTestStorage>(*blocker);
    manager.add(storage, ActionLocks::PartsMerge);
    EXPECT_EQ(manager.getStoppedActions(storage), Names{"merges"});

    /// Keep the registry entry and table alive while the blocker expires. SQL cannot directly
    /// arrange this lifetime boundary without also detaching or replacing the table instance.
    blocker.reset();
    EXPECT_TRUE(manager.getStoppedActions(storage).empty());
    manager.cleanExpired();
    EXPECT_TRUE(manager.getStoppedActions(storage).empty());
}
}
