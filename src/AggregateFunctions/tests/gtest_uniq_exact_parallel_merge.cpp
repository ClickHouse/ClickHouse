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

TEST(UniqExactParallelMerge, WaveWorkersAreDistinct)
{
    constexpr size_t NUM_TASKS = 8;

    UniqExactMergeWaveStats wave_stats(NUM_TASKS, NUM_TASKS);
    ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 1);

    const auto waves_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves];
    const auto wave_states_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveInputStates];
    const auto wave_workers_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveWorkers];

    for (size_t i = 0; i < NUM_TASKS; ++i)
    {
        pool.scheduleOrThrowOnError(
            [&wave_stats]
            {
                UniqExactMergeWaveTaskTimer task_timer(wave_stats);
                task_timer.recordWorkItem();
            });
    }
    pool.wait();
    wave_stats.report();

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves] - waves_before, 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveInputStates] - wave_states_before, NUM_TASKS);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveWorkers] - wave_workers_before, 1);
}

TEST(UniqExactParallelMerge, IdleTasksDoNotCountAsWorkersOrCPUTime)
{
    UniqExactMergeWaveStats wave_stats(1, 2);
    ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 1);

    const auto waves_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves];
    const auto wave_cpu_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveCPUTimeMicroseconds];
    const auto wave_workers_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveWorkers];

    pool.scheduleOrThrowOnError([&wave_stats] { wave_stats.recordTask(2'000, 0); });
    pool.wait();
    wave_stats.recordTask(1'000, 1);
    wave_stats.report();

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves] - waves_before, 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveCPUTimeMicroseconds] - wave_cpu_before, 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveWorkers] - wave_workers_before, 1);
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

    const auto waves_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves];
    const auto wave_states_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveInputStates];
    const auto wave_wall_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveElapsedMicroseconds];
    const auto wave_cpu_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveCPUTimeMicroseconds];
    const auto wave_workers_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveWorkers];

    a.merge(b, &pool, &is_cancelled);

    /// a should contain the union: [0, N + N/2)
    ASSERT_EQ(a.size(), N + N / 2);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves] - waves_before, 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveInputStates] - wave_states_before, 2);
    EXPECT_GT(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveElapsedMicroseconds] - wave_wall_before, 0);
    EXPECT_GT(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveCPUTimeMicroseconds] - wave_cpu_before, 0);
    EXPECT_GT(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveWorkers] - wave_workers_before, 0);
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

    const auto waves_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves];
    const auto wave_states_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveInputStates];

    TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);

    /// Compute expected size: union of ranges [s * ELEMENTS_PER_SET/2, s * ELEMENTS_PER_SET/2 + ELEMENTS_PER_SET)
    size_t max_val = (NUM_SETS - 1) * (ELEMENTS_PER_SET / 2) + ELEMENTS_PER_SET;
    ASSERT_EQ(sets[0].size(), max_val);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves] - waves_before, 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveInputStates] - wave_states_before, NUM_SETS);
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
    constexpr size_t LARGE_N = 200'000;
    constexpr size_t SMALL_N = 50'000;

    TestSet a;
    TestSet b;
    fillSet(a, 0, LARGE_N);
    fillSet(b, LARGE_N, SMALL_N);

    /// a is two-level (large), b stays single-level.
    a.convertToTwoLevel();

    VectorWithMemoryTracking<TestSet *> ptrs = {&a, &b};
    ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 4);
    std::atomic<bool> is_cancelled{false};

    const auto waves_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves];

    TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);
    ASSERT_EQ(a.size(), LARGE_N + SMALL_N);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves] - waves_before, 0);
}

TEST(UniqExactParallelMerge, PrepareMixedLevelsWaveProfileEvents)
{
    constexpr size_t NUM_SETS = 4;
    constexpr size_t N = 1'000;

    std::vector<TestSet> sets(NUM_SETS);
    for (size_t i = 0; i < NUM_SETS; ++i)
        fillSet(sets[i], i * N, N);
    sets.front().convertToTwoLevel();

    VectorWithMemoryTracking<TestSet *> ptrs;
    for (auto & set : sets)
        ptrs.push_back(&set);

    ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 4);
    std::atomic<bool> is_cancelled{false};
    const auto waves_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves];
    const auto wave_states_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveInputStates];

    TestSet::parallelizeMergePrepare(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);

    for (const auto & set : sets)
        EXPECT_TRUE(set.isTwoLevel());
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves] - waves_before, 1);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveInputStates] - wave_states_before, NUM_SETS);
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

    const auto waves_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves];
    const auto wave_states_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveInputStates];
    const auto wave_wall_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveElapsedMicroseconds];
    const auto wave_cpu_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveCPUTimeMicroseconds];
    const auto wave_workers_before = ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveWorkers];

    TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);
    ASSERT_EQ(a.size(), N);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaves] - waves_before, 0);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveInputStates] - wave_states_before, 0);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveElapsedMicroseconds] - wave_wall_before, 0);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveCPUTimeMicroseconds] - wave_cpu_before, 0);
    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::UniqExactMergeWaveWorkers] - wave_workers_before, 0);
}
