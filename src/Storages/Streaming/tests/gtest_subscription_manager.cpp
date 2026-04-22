#include <Storages/Streaming/QueueStreamSubscription.h>
#include <Storages/Streaming/SubscriptionManager.h>

#include <gtest/gtest.h>

#include <memory>

using namespace DB;

using IntQueueSub = QueueStreamSubscription<int>;

TEST(StreamSubscriptionManager, StartsEmpty)
{
    StreamSubscriptionManager m;
    ASSERT_TRUE(m.isEmpty());
    ASSERT_FALSE(m.hasSome());
}

TEST(StreamSubscriptionManager, RegisterOneSubscription)
{
    StreamSubscriptionManager m;
    auto sub = std::make_shared<IntQueueSub>();

    m.registerSubscription(sub);

    ASSERT_FALSE(m.isEmpty());
    ASSERT_TRUE(m.hasSome());
}

TEST(StreamSubscriptionManager, ExecuteFiresOnEachAliveSubscription)
{
    StreamSubscriptionManager m;

    auto sub1 = std::make_shared<IntQueueSub>();
    auto sub2 = std::make_shared<IntQueueSub>();
    auto sub3 = std::make_shared<IntQueueSub>();

    m.registerSubscription(sub1);
    m.registerSubscription(sub2);
    m.registerSubscription(sub3);

    int counter = 0;
    m.executeOnEachSubscription([&counter](StreamSubscriptionPtr &) { ++counter; });
    ASSERT_EQ(counter, 3);
}

TEST(StreamSubscriptionManager, ExpiredSubscriptionsAreCleanedLazily)
{
    StreamSubscriptionManager m;

    auto sub_alive = std::make_shared<IntQueueSub>();
    {
        auto sub_short = std::make_shared<IntQueueSub>();
        m.registerSubscription(sub_short);
        m.registerSubscription(sub_alive);
    }
    /// sub_short is destroyed here; count will still read 2 until executeOnEachSubscription triggers clean.

    int invocations = 0;
    m.executeOnEachSubscription([&invocations](StreamSubscriptionPtr &) { ++invocations; });
    ASSERT_EQ(invocations, 1); /// only the alive one was invoked

    /// After clean, counter reflects only the alive sub.
    ASSERT_TRUE(m.hasSome());

    /// Dropping the last subscription and re-running execute should clean it too.
    sub_alive.reset();
    m.executeOnEachSubscription([](StreamSubscriptionPtr &) {});
    ASSERT_TRUE(m.isEmpty());
}

TEST(StreamSubscriptionManager, PushThroughSubscriptionDeliversToConsumer)
{
    /// Integration sanity check: the functor receives the same object the caller
    /// registered, so pushing/extracting through it works.
    StreamSubscriptionManager m;

    auto sub = std::make_shared<IntQueueSub>();
    m.registerSubscription(sub);

    m.executeOnEachSubscription([](StreamSubscriptionPtr & s)
    {
        if (auto * typed = s->as<IntQueueSub>())
            typed->push(7);
    });

    auto items = sub->extractAll();
    ASSERT_EQ(items.size(), 1u);
    ASSERT_EQ(items.front(), 7);
}
