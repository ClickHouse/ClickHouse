#include <AggregateFunctions/UniqExactSet.h>
#include <Common/HashTable/HashSet.h>
#include <Common/ThreadPool.h>
#include <Common/VectorWithMemoryTracking.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

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

/// Extract the exact, sorted key set through the public serialization interface. Both levels write the
/// same format (varint count, then raw keys; see `HashTable::write` and `writeAsSingleLevel`).
std::vector<Key> collectKeys(const TestSet & set)
{
    WriteBufferFromOwnString out;
    set.write(out);

    ReadBufferFromString in(out.str());
    size_t count = 0;
    readVarUInt(count, in);
    std::vector<Key> keys(count);
    for (auto & key : keys)
        readBinaryLittleEndian(key, in);

    std::sort(keys.begin(), keys.end());
    return keys;
}

/// Extract the exact, sorted key set from a raw hash set (single- or two-level) by iteration.
template <typename Table>
std::vector<Key> collectRawKeys(const Table & table)
{
    std::vector<Key> keys;
    keys.reserve(table.size());
    for (auto it = table.begin(); it != table.end(); ++it)
        keys.push_back(it->getValue());
    std::sort(keys.begin(), keys.end());
    return keys;
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

/// Direct differential for `HashSetTable::mergeInto` against `HashSetTable::merge` with prefetch forced on,
/// independent of the runtime L2 gate. The same const source merged both ways must produce identical key
/// sets — including key 0, which `merge` propagates with an explicit block while `mergeInto` relies on
/// zero-first iteration order and `emplaceIfZero` routing — and must itself stay unmodified.
TEST(HashSetMergeInto, MatchesMergeOnSingleLevelDestination)
{
    constexpr size_t DST_N = 120'000;
    constexpr size_t SRC_N = 200'000;

    for (int zero_in : {0, 1, 2})   /// 0: source only, 1: destination only, 2: both
    {
        SCOPED_TRACE(testing::Message() << "zero_in=" << zero_in);

        SingleLevelSet dst_old;
        SingleLevelSet dst_new;
        SingleLevelSet src;
        for (size_t k = 1; k <= DST_N; ++k)
        {
            dst_old.insert(k);
            dst_new.insert(k);
        }
        /// [DST_N/2, DST_N/2 + SRC_N) overlaps the destination by 60k keys. The source buffer (200k keys
        /// -> 1M cells) exceeds the destination's (120k -> 256k cells), so the old path's preemptive resize
        /// fires while the new path instead grows incrementally through a mid-merge rehash.
        for (size_t k = DST_N / 2; k < DST_N / 2 + SRC_N; ++k)
            src.insert(k);
        if (zero_in == 0 || zero_in == 2)
            src.insert(0);
        if (zero_in == 1 || zero_in == 2)
        {
            dst_old.insert(0);
            dst_new.insert(0);
        }

        const auto src_keys_before = collectRawKeys(src);

        const SingleLevelSet & const_src = src;
        dst_old.merge(const_src);                           /// the existing path, with the preemptive resize
        const_src.mergeInto</*prefetch=*/ true>(dst_new);   /// the new path; compiles only if genuinely const

        ASSERT_EQ(collectRawKeys(dst_new), collectRawKeys(dst_old));
        ASSERT_EQ(collectRawKeys(const_src), src_keys_before);   /// the const source is untouched
    }
}

/// Same differential with a two-level destination: `mergeInto` must route every key (zero included) by hash
/// to the right sub-table, matching `TwoLevelHashSetTable::merge(const HashSetTable &)` exactly.
TEST(HashSetMergeInto, MatchesMergeOnTwoLevelDestination)
{
    constexpr size_t DST_N = 150'000;
    constexpr size_t SRC_N = 100'000;

    for (int zero_in : {0, 1, 2})   /// 0: source only, 1: destination only, 2: both
    {
        SCOPED_TRACE(testing::Message() << "zero_in=" << zero_in);

        /// Heap-allocated: two TwoLevelSet locals (256 inline sub-tables each) overflow the
        /// -Wframe-larger-than=65536 budget.
        const auto dst_old_holder = std::make_unique<TwoLevelSet>();
        const auto dst_new_holder = std::make_unique<TwoLevelSet>();
        const auto src_holder = std::make_unique<SingleLevelSet>();
        TwoLevelSet & dst_old = *dst_old_holder;
        TwoLevelSet & dst_new = *dst_new_holder;
        SingleLevelSet & src = *src_holder;
        for (size_t k = 1; k <= DST_N; ++k)
        {
            dst_old.insert(k);
            dst_new.insert(k);
        }
        for (size_t k = DST_N / 2; k < DST_N / 2 + SRC_N; ++k)
            src.insert(k);
        if (zero_in == 0 || zero_in == 2)
            src.insert(0);
        if (zero_in == 1 || zero_in == 2)
        {
            dst_old.insert(0);
            dst_new.insert(0);
        }

        const auto src_keys_before = collectRawKeys(src);

        const SingleLevelSet & const_src = src;
        dst_old.merge(const_src);
        const_src.mergeInto</*prefetch=*/ true>(dst_new);

        ASSERT_EQ(collectRawKeys(dst_new), collectRawKeys(dst_old));
        ASSERT_EQ(collectRawKeys(const_src), src_keys_before);
    }
}

/// Zero-key differential for the merge routing: cover key 0 in the source only, the destination only, and
/// both, across all level combinations, at sizes on both sides of the L2 routing gate (a 1k-key destination
/// stays a 32 KiB buffer; a 90k-key single-level destination is a 2 MiB buffer at load <= 1/2).
TEST(UniqExactParallelMerge, ZeroKeyAcrossMergePaths)
{
    for (size_t n : {static_cast<size_t>(1'000), static_cast<size_t>(90'000)})
    {
        for (bool dst_two_level : {false, true})
        {
            for (bool src_two_level : {false, true})
            {
                for (int zero_in : {0, 1, 2})   /// 0: source only, 1: destination only, 2: both
                {
                    SCOPED_TRACE(testing::Message() << "n=" << n << " dst_two_level=" << dst_two_level
                                                    << " src_two_level=" << src_two_level << " zero_in=" << zero_in);

                    TestSet dst;
                    TestSet src;
                    fillSet(dst, 1, n);           /// [1, n + 1)
                    fillSet(src, n / 2 + 1, n);   /// [n/2 + 1, n/2 + n + 1), overlaps the destination by ~half
                    if (zero_in == 0 || zero_in == 2)
                    {
                        Key zero = 0;
                        src.insert<Key, SetLevelHint::unknown>(std::move(zero));
                    }
                    if (zero_in == 1 || zero_in == 2)
                    {
                        Key zero = 0;
                        dst.insert<Key, SetLevelHint::unknown>(std::move(zero));
                    }
                    if (dst_two_level)
                        dst.convertToTwoLevel();
                    if (src_two_level)
                        src.convertToTwoLevel();

                    std::vector<Key> expected;
                    expected.reserve(n / 2 + n + 1);
                    expected.push_back(0);
                    for (size_t i = 1; i < n / 2 + n + 1; ++i)
                        expected.push_back(i);

                    dst.merge(src);

                    /// The merge must not change conversion decisions: a single x single merge stays
                    /// single-level even past the two-level threshold, everything else ends up two-level.
                    ASSERT_EQ(dst.isTwoLevel(), dst_two_level || src_two_level);
                    ASSERT_EQ(dst.size(), expected.size());
                    ASSERT_EQ(collectKeys(dst), expected);
                }
            }
        }
    }
}

/// Grouped multi-way dispatch: the shape `Aggregator::mergeBucketMultiWayImpl` drives per destination
/// state of the keyed merge (`enable_multi_way_keyed_merge`) - `parallelizeMergePrepare` (parallel
/// two-level conversion, internally gated) followed by one `parallelizeMergeMulti` wave. Covers one
/// destination with several sources of mixed levels, a single-source group (the wave's 2-place
/// degenerate case), and an empty source set (a 1-place wave must be a no-op). Every result must
/// equal a serial pairwise reference merge of the same inputs in the same order.
TEST(UniqExactParallelMerge, GroupedMultiWayDispatch)
{
    ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 4);
    std::atomic<bool> is_cancelled{false};

    /// Heap-allocated sets throughout (the two-level pointee is heap-allocated anyway, and raw
    /// TwoLevelSet locals overflow the -Wframe-larger-than=65536 budget).
    auto build = [](size_t start, size_t count, bool two_level, bool with_zero)
    {
        auto set = std::make_unique<TestSet>();
        if (with_zero)
        {
            Key zero = 0;
            set->insert<Key, SetLevelHint::unknown>(std::move(zero));
        }
        fillSet(*set, start, count);
        if (two_level)
            set->convertToTwoLevel();
        return set;
    };

    /// One destination, 5 sources of mixed levels with overlapping ranges, zero key included.
    {
        struct SourceSpec
        {
            size_t start;
            size_t count;
            bool two_level;
            bool with_zero;
        };
        const std::vector<SourceSpec> specs = {
            {50'000, 200'000, true, false},
            {10'000, 30'000, false, false},
            {200'000, 150'000, true, false},
            {1, 5'000, false, true},
            {120'000, 40'000, false, false},
        };

        auto dst_wave = build(0, 60'000, false, false);
        auto dst_ref = build(0, 60'000, false, false);

        std::vector<std::unique_ptr<TestSet>> wave_sources;
        std::vector<std::unique_ptr<TestSet>> ref_sources;
        for (const auto & spec : specs)
        {
            wave_sources.push_back(build(spec.start, spec.count, spec.two_level, spec.with_zero));
            ref_sources.push_back(build(spec.start, spec.count, spec.two_level, spec.with_zero));
        }

        /// Serial pairwise reference merge, in source order.
        for (auto & src : ref_sources)
            dst_ref->merge(*src);

        VectorWithMemoryTracking<TestSet *> ptrs;
        ptrs.reserve(1 + wave_sources.size());
        ptrs.push_back(dst_wave.get());
        for (auto & src : wave_sources)
            ptrs.push_back(src.get());

        TestSet::parallelizeMergePrepare(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);
        TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);

        /// Mixed levels force the parallel conversion, so the wave ran the bucket-parallel path.
        EXPECT_TRUE(dst_wave->isTwoLevel());
        EXPECT_EQ(dst_wave->size(), dst_ref->size());
        EXPECT_EQ(collectKeys(*dst_wave), collectKeys(*dst_ref));
    }

    /// A single-source group: prepare converts the mixed pair, the wave degenerates to one merge.
    {
        auto dst_wave = build(0, 120'000, true, false);
        auto src_wave = build(60'000, 120'000, false, true);
        auto dst_ref = build(0, 120'000, true, false);
        auto src_ref = build(60'000, 120'000, false, true);

        dst_ref->merge(*src_ref);

        VectorWithMemoryTracking<TestSet *> ptrs = {dst_wave.get(), src_wave.get()};
        TestSet::parallelizeMergePrepare(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);
        TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);

        EXPECT_EQ(dst_wave->size(), dst_ref->size());
        EXPECT_EQ(collectKeys(*dst_wave), collectKeys(*dst_ref));
    }

    /// An empty source set: a wave holding only the destination must not touch it.
    {
        auto dst = build(0, 50'000, false, false);
        const auto keys_before = collectKeys(*dst);

        VectorWithMemoryTracking<TestSet *> ptrs = {dst.get()};
        TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);

        EXPECT_TRUE(dst->isSingleLevel());
        EXPECT_EQ(collectKeys(*dst), keys_before);
    }
}

/// Large-merge differential: deterministic random keys with ~75% overlap (the high-overlap shape the
/// non-resizing merge-into targets), zero key on both sides. Whatever routing branch fires, the merged
/// result must equal the reference union exactly, on the sequential, thread-pool, and multi-way paths.
TEST(UniqExactParallelMerge, LargeMergeEquivalenceHighOverlap)
{
    constexpr size_t N = 300'000;

    std::mt19937_64 rng(20260901);   /// fixed seed: deterministic
    std::vector<Key> a_keys(N);
    for (auto & key : a_keys)
        key = rng();
    std::vector<Key> b_keys(a_keys.begin(), a_keys.begin() + 3 * N / 4);
    for (size_t i = 3 * N / 4; i < N; ++i)
        b_keys.push_back(rng());
    a_keys.push_back(0);
    b_keys.push_back(0);

    std::vector<Key> expected;
    expected.reserve(a_keys.size() + b_keys.size());
    expected.insert(expected.end(), a_keys.begin(), a_keys.end());
    expected.insert(expected.end(), b_keys.begin(), b_keys.end());
    std::sort(expected.begin(), expected.end());
    expected.erase(std::unique(expected.begin(), expected.end()), expected.end());

    auto build = [](const std::vector<Key> & keys)
    {
        auto set = std::make_unique<TestSet>();
        for (auto key : keys)
        {
            Key k = key;
            set->insert<Key, SetLevelHint::unknown>(std::move(k));
        }
        return set;
    };

    /// Sequential two-level x two-level merge (the per-bucket loop).
    {
        auto dst = build(a_keys);
        auto src = build(b_keys);
        ASSERT_TRUE(dst->isTwoLevel());   /// 300k inserts auto-convert past the 100k threshold
        ASSERT_TRUE(src->isTwoLevel());
        dst->merge(*src);
        ASSERT_EQ(collectKeys(*dst), expected);
    }

    /// The same merge through the thread-pool path.
    {
        auto dst = build(a_keys);
        auto src = build(b_keys);
        ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 4);
        std::atomic<bool> is_cancelled{false};
        dst->merge(*src, &pool, &is_cancelled);
        ASSERT_EQ(collectKeys(*dst), expected);
    }

    /// The same keys through parallelizeMergeMulti (one bucket-wise wave across several sources).
    {
        auto dst = build(a_keys);
        auto src1 = build(b_keys);
        auto src2 = build(a_keys);   /// fully overlaps the destination
        auto src3 = build(b_keys);
        VectorWithMemoryTracking<TestSet *> ptrs = {dst.get(), src1.get(), src2.get(), src3.get()};
        ThreadPool pool(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), 4);
        std::atomic<bool> is_cancelled{false};
        TestSet::parallelizeMergeMulti(ptrs, [](TestSet * p) { return p; }, pool, is_cancelled);
        ASSERT_EQ(collectKeys(*dst), expected);
    }
}
