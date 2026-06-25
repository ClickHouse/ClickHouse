#include <gtest/gtest.h>

#include <Access/BcryptConcurrencyLimiter.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace DB;

TEST(BcryptConcurrencyLimiter, ZeroMeansUnlimited)
{
    BcryptConcurrencyLimiter limiter; /// default limit 0
    EXPECT_EQ(limiter.getLimit(), 0u);

    std::vector<BcryptConcurrencyLimiter::Guard> guards;
    for (int i = 0; i < 1000; ++i)
    {
        auto g = limiter.tryAcquire();
        EXPECT_TRUE(g.acquired());
        guards.push_back(std::move(g));
    }
    EXPECT_EQ(limiter.getInFlight(), 1000u);
}

TEST(BcryptConcurrencyLimiter, RejectsBeyondLimit)
{
    BcryptConcurrencyLimiter limiter;
    limiter.setLimit(2);

    auto g1 = limiter.tryAcquire();
    auto g2 = limiter.tryAcquire();
    EXPECT_TRUE(g1.acquired());
    EXPECT_TRUE(g2.acquired());
    EXPECT_EQ(limiter.getInFlight(), 2u);

    /// Third concurrent acquire is rejected (distinct-password flood is capped).
    auto g3 = limiter.tryAcquire();
    EXPECT_FALSE(g3.acquired());
    EXPECT_EQ(limiter.getInFlight(), 2u);
}

TEST(BcryptConcurrencyLimiter, SlotIsReleasedOnGuardDestruction)
{
    BcryptConcurrencyLimiter limiter;
    limiter.setLimit(1);

    {
        auto g1 = limiter.tryAcquire();
        EXPECT_TRUE(g1.acquired());
        EXPECT_FALSE(limiter.tryAcquire().acquired());
    }
    /// g1 destroyed -> slot freed -> next acquire succeeds (legitimate retry under lower load).
    EXPECT_EQ(limiter.getInFlight(), 0u);
    auto g2 = limiter.tryAcquire();
    EXPECT_TRUE(g2.acquired());
}

TEST(BcryptConcurrencyLimiter, MoveTransfersOwnership)
{
    BcryptConcurrencyLimiter limiter;
    limiter.setLimit(1);

    auto g1 = limiter.tryAcquire();
    EXPECT_TRUE(g1.acquired());
    EXPECT_EQ(limiter.getInFlight(), 1u);

    auto g2 = std::move(g1);
    EXPECT_TRUE(g2.acquired());
    EXPECT_FALSE(g1.acquired()); /// NOLINT(bugprone-use-after-move)
    /// Moved-from guard releases nothing; the single slot is still held exactly once.
    EXPECT_EQ(limiter.getInFlight(), 1u);
}

TEST(BcryptConcurrencyLimiter, LimitCanBeRaisedAndLowered)
{
    BcryptConcurrencyLimiter limiter;
    limiter.setLimit(1);

    auto g1 = limiter.tryAcquire();
    EXPECT_TRUE(g1.acquired());
    EXPECT_FALSE(limiter.tryAcquire().acquired());

    /// Raising the limit immediately admits more (config reload).
    limiter.setLimit(3);
    auto g2 = limiter.tryAcquire();
    auto g3 = limiter.tryAcquire();
    EXPECT_TRUE(g2.acquired());
    EXPECT_TRUE(g3.acquired());
    EXPECT_FALSE(limiter.tryAcquire().acquired());
}

/// The bound must hold at every instant even under heavy contention: the number of slots handed
/// out concurrently never exceeds the configured limit, and nothing is under-rejected past it.
TEST(BcryptConcurrencyLimiter, ConcurrentFloodNeverExceedsLimit)
{
    constexpr UInt64 limit = 4;
    BcryptConcurrencyLimiter limiter;
    limiter.setLimit(limit);

    std::atomic<UInt64> concurrent{0};
    std::atomic<UInt64> max_seen{0};
    std::atomic<bool> stop{false};

    auto worker = [&]
    {
        while (!stop.load(std::memory_order_relaxed))
        {
            auto g = limiter.tryAcquire();
            if (!g.acquired())
                continue;
            const UInt64 now = concurrent.fetch_add(1, std::memory_order_acq_rel) + 1;
            UInt64 prev = max_seen.load(std::memory_order_relaxed);
            while (now > prev && !max_seen.compare_exchange_weak(prev, now, std::memory_order_acq_rel))
                ; /// record observed peak
            std::this_thread::yield();
            concurrent.fetch_sub(1, std::memory_order_acq_rel);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(16);
    for (int i = 0; i < 16; ++i)
        threads.emplace_back(worker);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    stop.store(true, std::memory_order_relaxed);
    for (auto & t : threads)
        t.join();

    EXPECT_LE(max_seen.load(), limit);
    EXPECT_EQ(limiter.getInFlight(), 0u);
}
