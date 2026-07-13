#include <AggregateFunctions/UniqExactSet.h>
#include <Common/HashTable/HashSet.h>
#include <Common/ThreadPool.h>
#include <Common/VectorWithMemoryTracking.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
/// Instantiate the same concrete type used by AggregateFunctionUniqExact<UInt64>.
using Key = UInt64;
constexpr size_t initial_size_degree = 4;
using SingleLevelSet = HashSetWithStackMemory<Key, HashCRC32<Key>, initial_size_degree>;
using TwoLevelSet = TwoLevelHashSet<Key, HashCRC32<Key>>;
using TestSet = UniqExactSet<SingleLevelSet, TwoLevelSet>;

void fillSet(TestSet & set, size_t start, size_t count)
{
    for (size_t i = start; i < start + count; ++i)
    {
        Key key = i;
        set.insert<Key, SetLevelHint::unknown>(std::move(key));
    }
}
}

/// Test pairwise merge (the existing path) with thread pool.
TEST(UniqExactParallelMerge, PairwiseMerge)
{
    constexpr size_t N = 200'000;

    TestSet a;
    TestSet b;
    fillSet(a, 0, N);
    fillSet(b, N / 2, N);

    ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 4);
    std::atomic<bool> is_cancelled{false};

    a.merge(b, &pool, &is_cancelled);

    /// a should contain the union: [0, N + N/2)
    ASSERT_EQ(a.size(), N + N / 2);
}

/// Test batch merge (parallelizeMergeMulti) — the new path.
TEST(UniqExactParallelMerge, BatchMergeMulti)
{
    constexpr size_t NUM_SETS = 8;
    constexpr size_t ELEMENTS_PER_SET = 200'000;

    std::vector<TestSet> sets(NUM_SETS);

    /// Each set gets a range that partially overlaps with its neighbors.
    for (size_t s = 0; s < NUM_SETS; ++s)
        fillSet(sets[s], s * (ELEMENTS_PER_SET / 2), ELEMENTS_PER_SET);

    /// Convert to two-level (required for parallelizeMergeMulti fast path).
    for (auto & set : sets)
        set.convertToTwoLevel();

    /// Collect pointers.
    VectorWithMemoryTracking<TestSet *> ptrs;
    ptrs.reserve(NUM_SETS);
    for (auto & set : sets)
        ptrs.push_back(&set);

    ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 4);
    std::atomic<bool> is_cancelled{false};

    TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);

    /// Compute expected size: union of ranges [s * ELEMENTS_PER_SET/2, s * ELEMENTS_PER_SET/2 + ELEMENTS_PER_SET)
    size_t max_val = (NUM_SETS - 1) * (ELEMENTS_PER_SET / 2) + ELEMENTS_PER_SET;
    ASSERT_EQ(sets[0].size(), max_val);
}

/// Test that batch merge with a single set is a no-op.
TEST(UniqExactParallelMerge, BatchMergeSingleSet)
{
    constexpr size_t N = 1000;

    TestSet a;
    fillSet(a, 0, N);
    a.convertToTwoLevel();

    VectorWithMemoryTracking<TestSet *> ptrs = {&a};
    ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 4);
    std::atomic<bool> is_cancelled{false};

    TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);
    ASSERT_EQ(a.size(), N);
}

/// Test batch merge with mixed single-level and two-level sets (fallback path).
TEST(UniqExactParallelMerge, BatchMergeMixedLevels)
{
    constexpr size_t N = 200'000;

    TestSet a;
    TestSet b;
    fillSet(a, 0, N);
    fillSet(b, N, N);

    /// a is two-level (large), b stays at its natural level.
    a.convertToTwoLevel();

    VectorWithMemoryTracking<TestSet *> ptrs = {&a, &b};
    ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 4);
    std::atomic<bool> is_cancelled{false};

    TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);
    ASSERT_EQ(a.size(), 2 * N);
}

/// Test cancellation support.
TEST(UniqExactParallelMerge, BatchMergeCancellation)
{
    constexpr size_t N = 200'000;

    TestSet a;
    TestSet b;
    fillSet(a, 0, N);
    fillSet(b, N, N);
    a.convertToTwoLevel();
    b.convertToTwoLevel();

    VectorWithMemoryTracking<TestSet *> ptrs = {&a, &b};
    ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 4);
    std::atomic<bool> is_cancelled{true}; /// Pre-cancelled.

    TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);
    /// With cancellation, the merge may be partial or empty — just verify no crash.
    ASSERT_LE(a.size(), 2 * N);
}
