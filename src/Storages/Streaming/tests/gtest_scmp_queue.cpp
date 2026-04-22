#include <Storages/Streaming/SCMPQueue.h>

#include <gtest/gtest.h>

#include <list>
#include <thread>
#include <chrono>

using namespace DB;

TEST(SCMPQueue, PushExtractSingleItem)
{
    SCMPQueue<int> q;
    q.add(42);

    auto items = q.extractAll();
    ASSERT_EQ(items.size(), 1u);
    ASSERT_EQ(items.front(), 42);
}

TEST(SCMPQueue, PushExtractPreservesOrder)
{
    SCMPQueue<int> q;

    for (int i = 0; i < 5; ++i)
        q.add(i);

    auto items = q.extractAll();
    ASSERT_EQ(items.size(), 5u);

    int expected = 0;
    for (auto v : items)
        ASSERT_EQ(v, expected++);
}

TEST(SCMPQueue, IsEmptyReflectsState)
{
    SCMPQueue<int> q;
    ASSERT_TRUE(q.isEmpty());

    q.add(1);
    ASSERT_FALSE(q.isEmpty());

    (void)q.extractAll();
    ASSERT_TRUE(q.isEmpty());
}

TEST(SCMPQueue, CloseReturnsEmpty)
{
    SCMPQueue<int> q;
    q.add(1);
    q.add(2);

    q.close();
    ASSERT_TRUE(q.isClosed());

    /// Closed queue returns an empty list even if items were queued before close.
    auto items = q.extractAll();
    ASSERT_TRUE(items.empty());

    /// And ignores subsequent adds.
    q.add(3);
    items = q.extractAll();
    ASSERT_TRUE(items.empty());
}

#if defined(OS_LINUX)
TEST(SCMPQueue, FdIsExposedOnLinux)
{
    SCMPQueue<int> q;
    auto fd = q.fd();
    ASSERT_TRUE(fd.has_value());
    ASSERT_GE(fd.value(), 0);
}
#else
TEST(SCMPQueue, FdAbsentOnNonLinux)
{
    SCMPQueue<int> q;
    ASSERT_FALSE(q.fd().has_value());
}
#endif

TEST(SCMPQueue, MultipleProducersPreservePerProducerOrder)
{
    /// Producers push their own monotonic sequences; each sequence must appear in
    /// order in the drained list (even if interleaved with other producers).
    SCMPQueue<std::pair<int, int>> q;

    constexpr int num_producers = 4;
    constexpr int items_per_producer = 50;

    std::vector<std::thread> producers;
    producers.reserve(num_producers);
    for (int p = 0; p < num_producers; ++p)
    {
        producers.emplace_back([&q, p] {
            for (int i = 0; i < items_per_producer; ++i)
                q.add(std::make_pair(p, i));
        });
    }

    for (auto & t : producers)
        t.join();

    auto items = q.extractAll();
    ASSERT_EQ(items.size(), static_cast<size_t>(num_producers * items_per_producer));

    std::vector<int> last_seen(num_producers, -1);
    for (const auto & [pid, seq] : items)
    {
        ASSERT_GT(seq, last_seen[pid]);
        last_seen[pid] = seq;
    }

    for (int p = 0; p < num_producers; ++p)
        ASSERT_EQ(last_seen[p], items_per_producer - 1);
}
