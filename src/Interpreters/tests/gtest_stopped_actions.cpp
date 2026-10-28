#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/ActionLockTypes.h>
#include <Storages/IStorage.h>
#include <Common/ActionBlocker.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <new>
#include <utility>
#include <vector>

namespace DB
{
namespace
{
/// Owns its blocker, like every storage that implements `getActionLock` itself, so the registered
/// lock lives exactly as long as this instance.
class StoppedActionsTestStorage final : public IStorage
{
public:
    explicit StoppedActionsTestStorage(StorageID id = StorageID("test", "stopped_actions"))
        : IStorage(std::move(id))
    {
    }

    String getName() const override { return "StoppedActionsTestStorage"; }
    ActionLock getActionLock(StorageActionBlockType) override { return blocker.cancel(); }

    ActionBlocker & getBlocker() { return blocker; }

private:
    ActionBlocker blocker;
};

/// Forwards the lock to another storage, like an `Alias`, a materialized view or a lazy proxy.
class StoppedActionsForwardingTestStorage final : public IStorage
{
public:
    StoppedActionsForwardingTestStorage(StorageID id, StoragePtr target_)
        : IStorage(std::move(id)), target(std::move(target_))
    {
    }

    String getName() const override { return "StoppedActionsForwardingTestStorage"; }
    ActionLock getActionLock(StorageActionBlockType type) override { return target->getActionLock(type); }
    StoragePtr getActionLockOwner(StorageActionBlockType /* action_type */) const override { return target; }

private:
    StoragePtr target;
};
}

TEST(StoppedActions, DisplayNamesAndReadOnlyObservation)
{
    /// Some action types have no SQL-accessible implementation in this build. Exercise the
    /// display mapping directly without changing which locks real engines register.
    /// Generate the test inputs from the same list as the constants and display mapping.
    /// SQL reference files independently pin the names exposed by real engines.
    const std::vector<std::pair<StorageActionBlockType, String>> action_names = {
#define ACTION_NAME_PAIR(ID, VALUE, NAME) {ActionLocks::ID, NAME},
        ACTION_LOCK_TYPES(ACTION_NAME_PAIR)
#undef ACTION_NAME_PAIR
    };
    ActionLocksManager manager(getContext().context);
    auto storage = std::make_shared<StoppedActionsTestStorage>();
    auto other = std::make_shared<StoppedActionsTestStorage>();
    EXPECT_TRUE(manager.getStoppedActions(storage).empty());

    Names expected;
    for (const auto & [type, name] : action_names)
    {
        EXPECT_FALSE(name.empty()) << "Empty display name for action type " << type;
        manager.add(storage, type);
        expected.push_back(name);
    }
    std::ranges::sort(expected);
    EXPECT_EQ(std::adjacent_find(expected.begin(), expected.end()), expected.end()) << "Action display names must be unique";
    manager.add(storage, ActionLocks::PartsMerge);
    EXPECT_EQ(manager.getStoppedActions(storage), expected);
    EXPECT_EQ(manager.getStoppedActions(storage), expected);
    EXPECT_TRUE(storage->getBlocker().isCancelled());
    /// Sharing a table name does not create a registry entry for another instance.
    EXPECT_TRUE(manager.getStoppedActions(other).empty());

    for (const auto & [type, _] : action_names)
        manager.remove(storage, type);
    EXPECT_TRUE(manager.getStoppedActions(storage).empty());
    EXPECT_FALSE(storage->getBlocker().isCancelled());
}

TEST(StoppedActions, ReplacementInstanceAtTheSameAddress)
{
    /// The registry is keyed by the storage that owns the blocker, so an entry lives exactly as long
    /// as that storage. A replacement allocated at the same address therefore cannot inherit it,
    /// without any bookkeeping beyond `ActionLock::expired`.
    ActionLocksManager manager(getContext().context);

    alignas(StoppedActionsTestStorage) unsigned char storage_memory[sizeof(StoppedActionsTestStorage)];
    auto emplace_instance = [&] { return StoragePtr(new (storage_memory) StoppedActionsTestStorage(), [](IStorage *) {}); };
    auto destroy_instance = [&] { std::destroy_at(reinterpret_cast<StoppedActionsTestStorage *>(storage_memory)); };
    auto blocker_of = [](const StoragePtr & storage) -> ActionBlocker & { return static_cast<StoppedActionsTestStorage &>(*storage).getBlocker(); };

    StoragePtr first = emplace_instance();
    const auto * const address = first.get();
    manager.add(first, ActionLocks::PartsMerge);
    ASSERT_EQ(manager.getStoppedActions(first), Names{"merges"});
    ASSERT_TRUE(blocker_of(first).isCancelled());

    first.reset();
    destroy_instance();

    StoragePtr replacement = emplace_instance();
    ASSERT_EQ(replacement.get(), address);
    /// The previous owner's lock expired with it, so the entry is not reported here.
    EXPECT_TRUE(manager.getStoppedActions(replacement).empty());

    manager.add(replacement, ActionLocks::PartsMove);
    EXPECT_EQ(manager.getStoppedActions(replacement), Names{"moves"});
    EXPECT_TRUE(blocker_of(replacement).isCancelled());

    manager.remove(replacement, ActionLocks::PartsMove);
    EXPECT_TRUE(manager.getStoppedActions(replacement).empty());
    EXPECT_FALSE(blocker_of(replacement).isCancelled());

    /// `cleanExpired` reclaims the entry of the instance that is gone.
    replacement.reset();
    destroy_instance();
    manager.cleanExpired();

    StoragePtr third = emplace_instance();
    EXPECT_TRUE(manager.getStoppedActions(third).empty());
    third.reset();
    destroy_instance();
}

TEST(StoppedActions, ForwardedLockIsAttributedToTheOwner)
{
    /// A forwarding storage registers its lock on the table that owns the blocker, so the action is
    /// reported where it actually blocks background work, and any forwarding storage can release it.
    ActionLocksManager manager(getContext().context);
    auto target = std::make_shared<StoppedActionsTestStorage>(StorageID("test", "target"));
    auto proxy = std::make_shared<StoppedActionsForwardingTestStorage>(StorageID("test", "proxy"), target);
    auto outer = std::make_shared<StoppedActionsForwardingTestStorage>(StorageID("test", "outer"), proxy);

    manager.add(outer, ActionLocks::PartsMerge);
    EXPECT_TRUE(target->getBlocker().isCancelled());
    EXPECT_EQ(manager.getStoppedActions(target), Names{"merges"});
    /// The forwarding storages own no blocker, so they report nothing.
    EXPECT_TRUE(manager.getStoppedActions(proxy).empty());
    EXPECT_TRUE(manager.getStoppedActions(outer).empty());

    /// Releasing through the outermost forwarding storage clears the owner's entry.
    manager.remove(outer, ActionLocks::PartsMerge);
    EXPECT_TRUE(manager.getStoppedActions(target).empty());
    EXPECT_FALSE(target->getBlocker().isCancelled());

    /// A stop registered through the proxy survives the proxy and can be released on the target.
    manager.add(proxy, ActionLocks::PartsMove);
    EXPECT_EQ(manager.getStoppedActions(target), Names{"moves"});
    outer.reset();
    proxy.reset();
    EXPECT_EQ(manager.getStoppedActions(target), Names{"moves"});
    manager.remove(target, ActionLocks::PartsMove);
    EXPECT_TRUE(manager.getStoppedActions(target).empty());
    EXPECT_FALSE(target->getBlocker().isCancelled());
}
}
