#include <Interpreters/ActionLocksManager.h>
#include <Storages/IStorage.h>
#include <Common/ActionBlocker.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <cstddef>
#include <new>

namespace DB
{
namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsMove;
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

TEST(ActionLocksManager, CleanExpiredReleasesForwardedBlockerAfterOwnerExpires)
{
    ActionLocksManager manager(getContext().context);
    /// Like an ordinary `MaterializedView`, the owner forwards to a longer-lived target blocker.
    ActionBlocker blocker;
    auto storage = std::make_shared<ActionLocksTestStorage>(blocker);
    std::weak_ptr<IStorage> weak_storage = storage;
    manager.add(storage, ActionLocks::PartsMerge);
    manager.add(storage, ActionLocks::PartsMerge);
    EXPECT_EQ(blocker.getCounter(), 1);
    /// `cleanExpired` must preserve controls whose owner is still alive.
    manager.cleanExpired();
    EXPECT_TRUE(blocker.isCancelled());

    storage.reset();
    EXPECT_TRUE(weak_storage.expired());
    /// Destruction alone does not release the lock; the next sweep does.
    EXPECT_TRUE(blocker.isCancelled());
    manager.cleanExpired();
    EXPECT_FALSE(blocker.isCancelled());
}

TEST(ActionLocksManager, StorageOwnershipSurvivesAddressReuse)
{
    ActionLocksManager manager(getContext().context);
    ActionBlocker blocker;
    alignas(ActionLocksTestStorage) std::byte memory[sizeof(ActionLocksTestStorage)];
    auto make_storage = [&]
    {
        return StoragePtr(new (memory) ActionLocksTestStorage(blocker), [](IStorage * storage)
        {
            static_cast<ActionLocksTestStorage *>(storage)->~ActionLocksTestStorage();
        });
    };

    auto first = make_storage();
    manager.add(first, ActionLocks::PartsMerge);
    std::weak_ptr<IStorage> weak_first = first;
    first.reset();
    ASSERT_TRUE(weak_first.expired());

    /// Reusing the address must discard the old owner's lock, even for a different action.
    auto second = make_storage();
    manager.add(second, ActionLocks::PartsMove);
    EXPECT_EQ(blocker.getCounter(), 1);
    manager.remove(second, ActionLocks::PartsMerge);
    EXPECT_EQ(blocker.getCounter(), 1);
    second.reset();
    manager.cleanExpired();
    EXPECT_FALSE(blocker.isCancelled());

    auto third = make_storage();
    manager.add(third, ActionLocks::PartsMerge);
    third.reset();
    auto fourth = make_storage();
    manager.remove(fourth, ActionLocks::PartsMove);
    EXPECT_FALSE(blocker.isCancelled());
}

}
