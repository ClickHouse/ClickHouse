#include <Interpreters/ActionLocksManager.h>
#include <Storages/IStorage.h>
#include <Common/ActionBlocker.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <new>

namespace DB
{
namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsMove;
    extern const StorageActionBlockType ViewRefresh;
    extern const StorageActionBlockType ViewRefreshPause;
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
        stopped = true;
        if (on_stop)
            on_stop();
        return blocker.cancel();
    }

    void onActionLockRemove(StorageActionBlockType) override
    {
        stopped = false;
        ++start_count;
    }

    std::atomic<bool> stopped = false;
    size_t start_count = 0;
    std::function<void()> on_stop;

private:
    ActionBlocker & blocker;
};

}

TEST(ActionLocksManager, ExpiredStorageReleasesForwardedBlocker)
{
    ActionLocksManager manager(getContext().context);
    /// Like an alias, the storage forwards its lock to a blocker with a longer lifetime.
    ActionBlocker blocker;
    auto storage = std::make_shared<ActionLocksTestStorage>(blocker);
    std::weak_ptr<IStorage> weak_storage = storage;
    manager.add(storage, ActionLocks::PartsMerge);
    manager.add(storage, ActionLocks::PartsMerge);
    EXPECT_EQ(blocker.getCounter(), 1);
    manager.cleanExpired();
    EXPECT_TRUE(blocker.isCancelled());

    storage.reset();
    EXPECT_TRUE(weak_storage.expired());
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

TEST(ActionLocksManager, RefreshStartWaitsForStopRegistration)
{
    ActionLocksManager manager(getContext().context);
    ActionBlocker blocker;
    auto storage = std::make_shared<ActionLocksTestStorage>(blocker);

    for (auto action : {ActionLocks::ViewRefresh, ActionLocks::ViewRefreshPause})
    {
        std::promise<void> stopped;
        std::promise<void> release_stop;
        auto release = release_stop.get_future();
        storage->on_stop = [&]
        {
            /// Registry cleanup from a storage callback must not deadlock.
            manager.cleanExpired();
            stopped.set_value();
            release.wait();
        };
        auto stop = std::async(std::launch::async, [&]
        {
            manager.add(storage, action);
        });
        stopped.get_future().wait();
        std::promise<void> starting;
        auto start = std::async(std::launch::async, [&]
        {
            starting.set_value();
            manager.remove(storage, ActionLocks::ViewRefresh);
        });
        starting.get_future().wait();
        /// `STOP` has changed the storage state but has not returned its lock for registration.
        EXPECT_EQ(start.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
        release_stop.set_value();
        stop.get();
        start.get();
        EXPECT_FALSE(storage->stopped);
        EXPECT_FALSE(blocker.isCancelled());
    }
    EXPECT_EQ(storage->start_count, 2);

    storage->on_stop = {};
    manager.add(storage, ActionLocks::ViewRefresh);
    manager.add(storage, ActionLocks::ViewRefreshPause);
    EXPECT_EQ(blocker.getCounter(), 2);
    manager.remove(storage, ActionLocks::ViewRefresh);
    EXPECT_FALSE(storage->stopped);
    EXPECT_FALSE(blocker.isCancelled());
    EXPECT_EQ(storage->start_count, 3);
}

}
