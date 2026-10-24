#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/ActionLockTypes.h>
#include <Storages/IStorage.h>
#include <Common/ActionBlocker.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <utility>
#include <vector>

namespace DB
{
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
    /// Generate the test inputs from the same list as the constants and display mapping.
    /// SQL reference files independently pin the names exposed by real engines.
    const std::vector<std::pair<StorageActionBlockType, String>> action_names = {
#define ACTION_NAME_PAIR(ID, VALUE, NAME) {ActionLocks::ID, NAME},
        ACTION_LOCK_TYPES(ACTION_NAME_PAIR)
#undef ACTION_NAME_PAIR
    };
    ActionLocksManager manager(getContext().context);
    ActionBlocker blocker;
    auto storage = std::make_shared<StoppedActionsTestStorage>(blocker);
    auto other = std::make_shared<StoppedActionsTestStorage>(blocker);
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
