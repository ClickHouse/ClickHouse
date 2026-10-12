#include <Interpreters/ActionLocksManager.h>
#include <Storages/IStorage.h>
#include <Common/ActionBlocker.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <new>
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

class ActionLocksTestStorage final : public IStorage
{
public:
    explicit ActionLocksTestStorage(ActionBlocker & blocker_)
        : IStorage(StorageID("test", "action_locks")), blocker(blocker_)
    {
    }

    String getName() const override
    {
        return "ActionLocksTestStorage";
    }

    ActionLock getActionLock(StorageActionBlockType) override
    {
        return blocker.cancel();
    }

private:
    ActionBlocker & blocker;
};

}

TEST(ActionLocksManager, GetStoppedActionsReturnsRegisteredControls)
{
    ActionLocksManager manager(getContext().context);
    ActionBlocker blocker;
    auto storage = std::make_shared<ActionLocksTestStorage>(blocker);

    manager.add(storage, ActionLocks::PartsMerge);
    manager.add(storage, ActionLocks::PartsMerge);
    manager.add(storage, ActionLocks::PartsMove);
    EXPECT_EQ(manager.getStoppedActions(storage), (Names{"merges", "moves"}));

    /// Reading does not disturb the registry.
    EXPECT_EQ(manager.getStoppedActions(storage), (Names{"merges", "moves"}));
    EXPECT_TRUE(blocker.isCancelled());

    manager.remove(storage, ActionLocks::PartsMerge);
    EXPECT_EQ(manager.getStoppedActions(storage), Names{"moves"});
    manager.remove(storage, ActionLocks::PartsMove);
    EXPECT_TRUE(manager.getStoppedActions(storage).empty());
    EXPECT_FALSE(blocker.isCancelled());
}

TEST(ActionLocksManager, StorageOwnershipSurvivesAddressReuse)
{
    ActionLocksManager manager(getContext().context);
    alignas(ActionLocksTestStorage) std::byte memory[sizeof(ActionLocksTestStorage)];
    auto make_storage = [&](ActionBlocker & blocker)
    {
        return StoragePtr(new (memory) ActionLocksTestStorage(blocker), [](IStorage * storage)
        {
            static_cast<ActionLocksTestStorage *>(storage)->~ActionLocksTestStorage();
        });
    };

    ActionBlocker first_blocker;
    auto first = make_storage(first_blocker);
    manager.add(first, ActionLocks::PartsMerge);
    EXPECT_EQ(manager.getStoppedActions(first), Names{"merges"});
    first.reset();
    EXPECT_TRUE(first_blocker.isCancelled());

    /// A new storage at the recycled address must not observe the previous owner's controls.
    auto second = make_storage(first_blocker);
    EXPECT_TRUE(manager.getStoppedActions(second).empty());

    /// A control registered by the new owner replaces the stale entry, not merges with it.
    manager.add(second, ActionLocks::PartsMove);
    EXPECT_EQ(manager.getStoppedActions(second), Names{"moves"});
    manager.remove(second, ActionLocks::PartsMove);
    /// Removing the last control reclaims the entry; the recycled address stays clean
    /// even if this storage lives on and its address is never reused.
    second.reset();

    /// Destroying the blocker expires the previous owner's locks; the next sweep reclaims the entry.
    {
        ActionBlocker expiring_blocker = std::move(first_blocker);
    }
    manager.cleanExpired();

    ActionBlocker third_blocker;
    auto third = make_storage(third_blocker);
    manager.add(third, ActionLocks::ViewRefresh);
    EXPECT_EQ(manager.getStoppedActions(third), Names{"view_refresh"});
    third.reset();
    manager.cleanExpired();
    EXPECT_TRUE(manager.getStoppedActions(third).empty());
}

TEST(ActionLocksManager, AllActionTypesHaveDisplayNames)
{
    /// A new `StorageActionBlockType` that is registered here but missed in
    /// `getActionName` would make every `system.tables` scan fail with
    /// LOGICAL_ERROR instead of just showing an incomplete list. Guard against
    /// that by registering all known action types and expecting their full,
    /// sorted set of display names back.
    const std::vector<std::pair<StorageActionBlockType, String>> expected_names = {
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
    auto storage = std::make_shared<ActionLocksTestStorage>(blocker);

    Names expected;
    for (const auto & [action_type, name] : expected_names)
    {
        manager.add(storage, action_type);
        expected.push_back(name);
    }
    std::ranges::sort(expected);

    EXPECT_EQ(manager.getStoppedActions(storage), expected);

    /// Every name must be unique: a copy-pasted case in `getActionName` would
    /// silently collapse two action types into one display name.
    const auto duplicate = std::adjacent_find(expected.begin(), expected.end());
    EXPECT_EQ(duplicate, expected.end()) << "Duplicated display name: " << *duplicate;
}

TEST(ActionLocksManager, ExpiredLocksAreNotReportedUntilSwept)
{
    ActionLocksManager manager(getContext().context);
    auto blocker = std::make_unique<ActionBlocker>();
    auto storage = std::make_shared<ActionLocksTestStorage>(*blocker);

    manager.add(storage, ActionLocks::PartsMerge);
    manager.add(storage, ActionLocks::ViewRefreshPause);
    EXPECT_EQ(manager.getStoppedActions(storage), (Names{"merges", "view_refresh_pause"}));

    /// Destroying the blocker expires the locks: they must not be reported anymore,
    /// even before the next sweep removes their entries from the registry.
    blocker.reset();
    EXPECT_TRUE(manager.getStoppedActions(storage).empty());

    /// After the sweep the registry must accept new controls for the same storage again.
    manager.cleanExpired();
    ActionBlocker new_blocker;
    auto renewed = std::make_shared<ActionLocksTestStorage>(new_blocker);
    manager.add(renewed, ActionLocks::PartsMerge);
    EXPECT_EQ(manager.getStoppedActions(renewed), Names{"merges"});
}

}
