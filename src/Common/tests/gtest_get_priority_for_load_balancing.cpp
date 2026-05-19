#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <unordered_set>
#include <vector>

#include <Common/GetPriorityForLoadBalancing.h>

using namespace DB;

namespace
{
    /// For a closure produced by `getPriorityFunc(ROUND_ROBIN, 0, pool_size)` find the index
    /// `i` such that `func(i).value == pool_size - 1`. Per the rotation table in the source,
    /// that index is exactly `local_last_used` — i.e. the value of the counter the call
    /// observed before its `fetch_add`.
    size_t observedHead(const GetPriorityForLoadBalancing::Func & func, size_t pool_size)
    {
        for (size_t i = 0; i < pool_size; ++i)
            if (func(i).value == static_cast<Int64>(pool_size - 1))
                return i;
        return pool_size;
    }
}

/// Regression test for the data race in `GetPriorityForLoadBalancing::getPriorityFunc`
/// (TSan: STID 4676-580d, STID 4676-58a7). The function is `const` but mutates the
/// internal `last_used` counter for round-robin load balancing. It is invoked
/// concurrently from many threads through `ConnectionPoolWithFailover::makeGetPriorityFunc`
/// during distributed query dispatch (parallel replicas, distributed inserts).
///
/// The fix made `last_used` a `std::atomic<size_t>`. With the fix in place, this
/// test passes cleanly under ThreadSanitizer. Without the fix, TSan reports a
/// data race on `last_used` during the parallel calls. Independently of TSan,
/// after a known number of concurrent increments the counter must end up at the
/// expected value — a race that loses updates would surface as a wrong rotation
/// head on the next call.
TEST(GetPriorityForLoadBalancing, RoundRobinIsThreadSafe)
{
    constexpr size_t pool_size = 7;
    constexpr size_t num_threads = 8;
    constexpr size_t calls_per_thread = 10000;
    constexpr size_t total_parallel_calls = num_threads * calls_per_thread;

    GetPriorityForLoadBalancing lb(LoadBalancing::ROUND_ROBIN);

    std::atomic<bool> start{false};
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (size_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&]
        {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();

            for (size_t i = 0; i < calls_per_thread; ++i)
            {
                /// Concurrent invocations of `getPriorityFunc` on the same instance.
                /// Each call does `last_used.fetch_add(1)` internally — this is the
                /// hot path the data race fix targets.
                auto func = lb.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size);
                ASSERT_TRUE(static_cast<bool>(func));
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto & th : threads)
        th.join();

    /// One more call beyond the parallel block. Its observed head must equal
    /// `total_parallel_calls % pool_size` — the pre-increment counter value.
    /// If the parallel block had a race that lost increments, this assertion fails
    /// (independent of TSan).
    auto next_func = lb.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size);
    ASSERT_TRUE(static_cast<bool>(next_func));
    ASSERT_EQ(observedHead(next_func, pool_size), total_parallel_calls % pool_size);

    /// And a closure produced by a single call yields a full rotation over [0, pool_size).
    std::unordered_set<Int64> priorities;
    for (size_t j = 0; j < pool_size; ++j)
        priorities.insert(next_func(j).value);
    ASSERT_EQ(priorities.size(), pool_size);
}

/// Verifies the round-robin counter advances monotonically across single-threaded calls
/// (i.e. atomicity didn't change the observable rotation semantics).
TEST(GetPriorityForLoadBalancing, RoundRobinRotates)
{
    constexpr size_t pool_size = 5;
    GetPriorityForLoadBalancing lb(LoadBalancing::ROUND_ROBIN);

    std::vector<size_t> heads;
    heads.reserve(pool_size);
    for (size_t call = 0; call < pool_size; ++call)
    {
        auto func = lb.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size);
        heads.push_back(observedHead(func, pool_size));
    }

    /// All `pool_size` heads must be distinct (one full rotation).
    std::unordered_set<size_t> distinct_heads(heads.begin(), heads.end());
    ASSERT_EQ(distinct_heads.size(), pool_size);
}

/// Smoke-test for the explicit copy/move ctors and assignment operators that were added
/// to keep the class copyable/movable after `last_used` became a `std::atomic`.
/// (`ZooKeeperArgs` and `std::optional<GetPriorityForLoadBalancing>` rely on these.)
TEST(GetPriorityForLoadBalancing, CopyAndMovePropagateCounter)
{
    constexpr size_t pool_size = 3;

    /// Copy ctor — counter must come along.
    GetPriorityForLoadBalancing src(LoadBalancing::ROUND_ROBIN);
    for (int i = 0; i < 4; ++i)
        (void)src.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size);
    GetPriorityForLoadBalancing copy(src); // NOLINT(performance-unnecessary-copy-initialization) - intentionally testing copy constructor
    ASSERT_EQ(
        observedHead(src.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size), pool_size),
        observedHead(copy.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size), pool_size));

    /// Move ctor — same expectation.
    GetPriorityForLoadBalancing src2(LoadBalancing::ROUND_ROBIN);
    for (int i = 0; i < 2; ++i)
        (void)src2.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size);
    /// Save the head expected from a peer instance with the same counter advance.
    GetPriorityForLoadBalancing peer(LoadBalancing::ROUND_ROBIN);
    for (int i = 0; i < 2; ++i)
        (void)peer.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size);
    auto expected_head = observedHead(peer.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size), pool_size);
    GetPriorityForLoadBalancing moved(std::move(src2));
    ASSERT_EQ(observedHead(moved.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size), pool_size), expected_head);

    /// Copy assignment — same expectation.
    GetPriorityForLoadBalancing src3(LoadBalancing::ROUND_ROBIN);
    for (int i = 0; i < 5; ++i)
        (void)src3.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size);
    GetPriorityForLoadBalancing dst3;
    dst3 = src3;
    ASSERT_EQ(
        observedHead(src3.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size), pool_size),
        observedHead(dst3.getPriorityFunc(LoadBalancing::ROUND_ROBIN, 0, pool_size), pool_size));
}
