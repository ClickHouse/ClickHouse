#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>
#include <Storages/Streaming/SubscriptionManager.h>

#include <gtest/gtest.h>

#include <memory>
#include <vector>

using namespace DB;

TEST(StreamSubscriptionManager, TakeAllReturnsLiveSubscriptions)
{
    StreamSubscriptionManager manager;

    auto first = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
    auto second = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
    manager.registerSubscription(first);
    manager.registerSubscription(second);

    ASSERT_FALSE(manager.isEmpty());
    ASSERT_EQ(manager.takeAllSubscriptions().size(), 2u);
}

TEST(StreamSubscriptionManager, TakeAllSkipsExpiredSubscriptions)
{
    StreamSubscriptionManager manager;

    auto alive = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
    manager.registerSubscription(alive);
    manager.registerSubscription(std::make_shared<MergeTreeBoundsSubscription>(1, 0));

    /// The manager holds weak pointers, so the temporary above is already gone.
    auto collected = manager.takeAllSubscriptions();
    ASSERT_EQ(collected.size(), 1u);
    ASSERT_EQ(collected.front().get(), alive.get());
}

TEST(StreamSubscriptionManager, TakeAllKeepsSubscriptionAlive)
{
    StreamSubscriptionManager manager;

    std::weak_ptr<IStreamSubscription> weak;
    std::vector<StreamSubscriptionPtr> collected;
    {
        auto subscription = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
        weak = subscription;
        manager.registerSubscription(subscription);
        collected = manager.takeAllSubscriptions();
    }

    /// The collected strong pointer must outlive the caller that registered it, so a round cannot
    /// run against a half-destroyed subscription.
    ASSERT_EQ(collected.size(), 1u);
    ASSERT_FALSE(weak.expired());

    collected.clear();
    ASSERT_TRUE(weak.expired());
}

TEST(StreamSubscriptionManager, TakeAllExcludesLaterRegistrations)
{
    StreamSubscriptionManager manager;

    auto first = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
    manager.registerSubscription(first);

    auto collected = manager.takeAllSubscriptions();
    ASSERT_EQ(collected.size(), 1u);

    /// A subscription registered after the take must not be served by this round: the caller
    /// reads the parts only after collecting, so that snapshot may predate this subscription.
    auto second = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
    manager.registerSubscription(second);

    ASSERT_EQ(collected.size(), 1u);
    ASSERT_EQ(collected.front().get(), first.get());

    /// It is picked up by the next round instead.
    ASSERT_EQ(manager.takeAllSubscriptions().size(), 2u);
}
