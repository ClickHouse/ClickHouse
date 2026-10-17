#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/ActionLockTypes.h>
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
ACTION_LOCK_TYPES(ACTION_LOCK_DECLARE)
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
    /// The expected names are generated from the same `ACTION_LOCK_TYPES` list that
    /// declares the constants and drives `getActionName`, so a new `StorageActionBlockType`
    /// added without a display name breaks the build instead of making every
    /// `system.tables` scan fail with LOGICAL_ERROR at runtime.
    ActionLocksManager manager(getContext().context);
    ActionBlocker blocker;
    auto storage = std::make_shared<ActionLocksTestStorage>(blocker);

    Names expected;
#define ADD_ACTION_LOCK(ID, VALUE, NAME) \
    manager.add(storage, ActionLocks::ID); \
    expected.push_back(NAME);
    ACTION_LOCK_TYPES(ADD_ACTION_LOCK)
#undef ADD_ACTION_LOCK
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
