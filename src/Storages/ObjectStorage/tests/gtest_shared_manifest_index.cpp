#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

/// Tests for the shared atomic manifest-index counter contract used by
/// `SingleThreadIcebergKeysIterator` when running with
/// `iceberg_parallel_manifest_decode_threads > 1`. The iterator uses
/// `fetch_add(1, std::memory_order_relaxed)` to claim the next manifest
/// index from a counter shared across N peers; correctness depends on
/// every index in `[0, total)` being claimed exactly once across all
/// threads, with no overlap and no gaps.
///
/// We deliberately do not exercise `SingleThreadIcebergKeysIterator`
/// directly: constructing one requires a valid `IcebergDataSnapshot`,
/// `PersistentTableComponents`, AVRO catalog state, and an
/// `ObjectStorage`, none of which is reasonable to mock. The contract
/// being tested is the simple atomic counter; isolating it gives us a
/// fast, deterministic check that future changes to the iterator's claim
/// loop don't accidentally regress the underlying invariant.

namespace
{

void runStressTest(size_t total_indices, size_t num_threads)
{
    auto counter = std::make_shared<std::atomic<size_t>>(0);

    std::vector<std::vector<size_t>> per_thread_claims(num_threads);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (size_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&, t]()
        {
            auto & claims = per_thread_claims[t];
            claims.reserve(total_indices / num_threads + 1);
            while (true)
            {
                size_t idx = counter->fetch_add(1, std::memory_order_relaxed);
                if (idx >= total_indices)
                    break;
                claims.push_back(idx);
            }
        });
    }

    for (auto & th : threads)
        th.join();

    /// Aggregate and verify every index in [0, total_indices) was claimed exactly once.
    std::vector<bool> seen(total_indices, false);
    size_t total_claimed = 0;
    for (const auto & per_thread : per_thread_claims)
    {
        for (size_t idx : per_thread)
        {
            ASSERT_LT(idx, total_indices);
            ASSERT_FALSE(seen[idx]) << "index " << idx << " was claimed by more than one thread";
            seen[idx] = true;
            ++total_claimed;
        }
    }

    ASSERT_EQ(total_claimed, total_indices);
    for (size_t i = 0; i < total_indices; ++i)
        ASSERT_TRUE(seen[i]) << "index " << i << " was never claimed";
}

}

TEST(SharedManifestIndex, SingleThreadCoversAllIndices)
{
    runStressTest(/* total_indices */ 1000, /* num_threads */ 1);
}

TEST(SharedManifestIndex, ManyThreadsNoOverlapNoGaps)
{
    /// Many threads racing on a large counter: a stress test of the claim contract at a
    /// high thread count and a large index count.
    runStressTest(/* total_indices */ 60'000, /* num_threads */ 16);
}

TEST(SharedManifestIndex, MoreThreadsThanIndices)
{
    /// Edge case: more producers than there is work. Some threads should claim 0 indices
    /// and exit immediately on the first `fetch_add`. The contract still holds: every
    /// index in [0, total) is seen exactly once.
    runStressTest(/* total_indices */ 5, /* num_threads */ 32);
}

TEST(SharedManifestIndex, HighParallelism)
{
    /// 64 producers racing on a 10k-index counter. The shared-counter contract
    /// must hold under any reasonable thread count (the implementation no longer
    /// imposes an upper bound — see resolveParallelManifestDecodeThreads).
    runStressTest(/* total_indices */ 10'000, /* num_threads */ 64);
}
