#pragma once

#include <Common/ThreadGroupSwitcher.h>
#include <Common/HashTable/HashSet.h>
#include <Common/PODArray.h>
#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/VectorWithMemoryTracking.h>

#include <base/getL2CacheSize.h>

#include <atomic>
#include <memory>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_LARGE_ARRAY_SIZE;
}

enum class SetLevelHint
{
    singleLevel,
    twoLevel,
    unknown,
};

template <typename SingleLevelSet, typename TwoLevelSet>
class UniqExactSet
{
    static_assert(std::is_same_v<typename SingleLevelSet::value_type, typename TwoLevelSet::value_type>);
    static_assert(std::is_same_v<typename SingleLevelSet::Cell::State, HashTableNoState>);

    /// Two-level set plus a flag marking whether it has been handed out to another `UniqExactSet`.
    /// `getTwoLevelSet` sets it before the pointee escapes; `doDeepCopyIfNeeded` forks when it is set instead of
    /// using `shared_ptr::use_count()`, which is not a cross-thread synchronization primitive (a holder dropping
    /// its reference 2 -> 1 establishes no happens-before, so an in-place writer could race a concurrent reader).
    struct SharedTwoLevelSet
    {
        TwoLevelSet set;
        std::atomic<bool> is_shared{false};

        SharedTwoLevelSet() = default;
        explicit SharedTwoLevelSet(size_t size_hint) : set(size_hint) {}
        template <typename Source>
        explicit SharedTwoLevelSet(const Source & src) : set(src) {}
    };

public:
    using value_type = typename SingleLevelSet::value_type;

    template <typename Arg, SetLevelHint hint>
    auto ALWAYS_INLINE insert(Arg && arg)
    {
        if constexpr (hint == SetLevelHint::singleLevel)
        {
            asSingleLevel().insert(std::forward<Arg>(arg));
        }
        else if constexpr (hint == SetLevelHint::twoLevel)
        {
            asTwoLevel().insert(std::forward<Arg>(arg));
        }
        else
        {
            if (isSingleLevel())
            {
                auto && [_, inserted] = asSingleLevel().insert(std::forward<Arg>(arg));
                if (inserted && worthConvertingToTwoLevel(asSingleLevel().size()))
                    convertToTwoLevel();
            }
            else
            {
                asTwoLevel().insert(std::forward<Arg>(arg));
            }
        }
    }

    /// Batch-inserts a run of keys.
    ///
    /// Once the set spills out of L2 the inserts become cache-miss bound and mutually independent, so
    /// while inserting the current key we software-prefetch the destination cell of a look-ahead key,
    template <SetLevelHint hint>
    void ALWAYS_INLINE insertMany(const value_type * values, size_t n)
    {
        if constexpr (hint == SetLevelHint::twoLevel)
        {
            insertManyIntoSet(asTwoLevel(), values, n, /*prefetch=*/ true);
        }
        else if constexpr (hint == SetLevelHint::singleLevel)
        {
            auto & set = asSingleLevel();
            insertManyIntoSet(set, values, n, set.getBufferSizeInBytes() > getL2CacheSize());
        }
        else
        {
            if (isTwoLevel())
            {
                insertManyIntoSet(asTwoLevel(), values, n, /*prefetch=*/ true);
            }
            else
            {
                auto & set = asSingleLevel();
                insertManyIntoSet(set, values, n, set.getBufferSizeInBytes() > getL2CacheSize());

                if (worthConvertingToTwoLevel(set.size()))
                    convertToTwoLevel();
            }
        }
    }

    /// In merge, if one of the lhs and rhs is twolevelset and the other is singlelevelset, then the singlelevelset will need to convertToTwoLevel().
    /// It's not in parallel and will cost extra large time if the thread_num is large.
    /// This method will convert all the SingleLevelSet to TwoLevelSet in parallel if the hashsets are not all singlelevel or not all twolevel.
    /// Accepts a container of places and an accessor that returns `UniqExactSet *` for each element.
    /// This avoids building an intermediate vector of pointers.
    template <typename Places, typename Accessor>
    static void parallelizeMergePrepare(const Places & places, Accessor && accessor, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled)
    {
        UInt64 single_level_set_num = 0;
        UInt64 all_single_hash_size = 0;

        for (size_t i = 0; i < places.size(); ++i)
        {
            if (accessor(places[i])->isSingleLevel())
                single_level_set_num ++;
        }

        if (single_level_set_num == places.size())
        {
            for (size_t i = 0; i < places.size(); ++i)
                all_single_hash_size += accessor(places[i])->size();
        }

        /// If all the hashtables are mixed by singleLevel and twoLevel, or all singleLevel (larger than 6000 for average value), they could be converted into
        /// twoLevel hashtables in parallel and then merge together. please refer to the following PR for more details.
        /// https://github.com/ClickHouse/ClickHouse/pull/50748
        /// https://github.com/ClickHouse/ClickHouse/pull/52973
        if ((single_level_set_num > 0 && single_level_set_num < places.size()) || ((all_single_hash_size/places.size()) > 6000))
        {
            /// The pool can be shared with concurrent callers (e.g. two-level buckets are merged in parallel),
            /// so track and wait only for our own jobs: a bare `thread_pool.wait()` would also wait for
            /// unrelated jobs and could steal their exceptions.
            ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, ThreadName::UNIQ_EXACT_CONVERT);
            try
            {
                auto data_vec_atomic_index = std::make_shared<std::atomic_uint32_t>(0);
                auto thread_func = [&places, &accessor, data_vec_atomic_index, &is_cancelled]()
                {
                    while (true)
                    {
                        if (is_cancelled.load(std::memory_order_seq_cst))
                            return;

                        const auto i = data_vec_atomic_index->fetch_add(1);
                        if (i >= places.size())
                            return;
                        if (accessor(places[i])->isSingleLevel())
                            accessor(places[i])->convertToTwoLevel();
                    }
                };
                for (size_t i = 0; i < std::min<size_t>(thread_pool.getMaxThreads(), single_level_set_num); ++i)
                    runner.enqueueAndKeepTrack(thread_func, Priority{});
            }
            catch (...)
            {
                is_cancelled.store(true);
                throw;
            }
            runner.waitForAllToFinishAndRethrowFirstError();
        }
    }

    /// Batch merge multiple UniqExactSet into the first one in parallel.
    /// Each thread processes one bucket at a time across all hash tables,
    /// reducing thread pool overhead from O(N) to O(1) compared to pairwise merge.
    /// Accepts a container of places and an accessor that returns `UniqExactSet *` for each element.
    template <typename Places, typename Accessor>
    static void parallelizeMergeMulti(const Places & places, Accessor && accessor, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled)
    {
        if (places.size() <= 1)
            return;

        auto * first = accessor(places[0]);

        /// If not all are two-level, fall back to pairwise merge with thread pool.
        bool all_two_level = true;
        for (size_t i = 0; i < places.size(); ++i)
        {
            if (!accessor(places[i])->isTwoLevel())
            {
                all_two_level = false;
                break;
            }
        }

        if (!all_two_level)
        {
            size_t total_size = 0;
            for (size_t i = 0; i < places.size(); ++i)
                total_size += accessor(places[i])->size();

            if (worthConvertingToTwoLevel(total_size))
            {
                parallelizeMergeMultiScattered(places, accessor, thread_pool, is_cancelled);
                return;
            }

            for (size_t j = 1; j < places.size(); ++j)
            {
                if (is_cancelled.load(std::memory_order_seq_cst))
                    return;
                first->merge(*accessor(places[j]), &thread_pool, &is_cancelled);
            }
            return;
        }

        /// All sets are two-level, perform parallel bucket-wise merge.
        auto & first_two_level = first->asTwoLevelChecked();
        constexpr size_t NUM_BUCKETS = TwoLevelSet::NUM_BUCKETS;

        /// Pre-fetch all two-level set pointers to avoid concurrent access to getTwoLevelSet().
        VectorWithMemoryTracking<TwoLevelSet *> two_level_ptrs;
        two_level_ptrs.reserve(places.size());
        for (size_t i = 0; i < places.size(); ++i)
            two_level_ptrs.emplace_back(&accessor(places[i])->asTwoLevelChecked());

        ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, ThreadName::UNIQ_EXACT_MERGER);
        try
        {
            auto next_bucket_to_merge = std::make_shared<std::atomic_uint32_t>(0);

            auto thread_func = [&two_level_ptrs, &first_two_level, next_bucket_to_merge, &is_cancelled]()
            {
                while (true)
                {
                    if (is_cancelled.load(std::memory_order_seq_cst))
                        return;

                    const auto bucket = next_bucket_to_merge->fetch_add(1);
                    if (bucket >= NUM_BUCKETS)
                        return;

                    for (size_t j = 1; j < two_level_ptrs.size(); ++j)
                    {
                        if (is_cancelled.load(std::memory_order_seq_cst))
                            return;

                        first_two_level.impls[bucket].merge(two_level_ptrs[j]->impls[bucket]);
                    }
                }
            };

            const size_t max_threads_to_enqueue = std::min<size_t>(thread_pool.getMaxThreads(), NUM_BUCKETS);
            for (size_t i = 0; i < max_threads_to_enqueue
                 && next_bucket_to_merge->load(std::memory_order_relaxed) < NUM_BUCKETS; ++i)
                runner.enqueueAndKeepTrack(thread_func, Priority{});
        }
        catch (...)
        {
            is_cancelled.store(true);
            throw;
        }
        runner.waitForAllToFinishAndRethrowFirstError();
    }

    auto merge(const UniqExactSet & other, ThreadPool * thread_pool = nullptr, std::atomic<bool> * is_cancelled = nullptr)
    {
        if (size() == 0 && worthConvertingToTwoLevel(other.size()))
        {
            two_level_set = other.getTwoLevelSet();
            return;
        }

        if (isSingleLevel() && other.isTwoLevel())
            convertToTwoLevel();

        if (isSingleLevel())
        {
            asSingleLevel().merge(other.asSingleLevel());
        }
        else
        {
            auto & lhs = asTwoLevelChecked();

            if (other.isSingleLevel())
                return lhs.merge(other.asSingleLevel());

            /// `getTwoLevelSet` marked the pointee shared, so no other holder mutates it in place while we read it.
            const auto rhs_ptr = other.getTwoLevelSet();
            const auto & rhs = rhs_ptr->set;
            if (!thread_pool)
            {
                for (size_t i = 0; i < rhs.NUM_BUCKETS; ++i)
                {
                    lhs.impls[i].merge(rhs.impls[i]);
                }
            }
            else
            {

                /// Usage of lhs and rhs is fine. The references belong to *this and will outlive `runner`, so the order of destruction is ok
                ThreadPoolCallbackRunnerLocal<void> runner(*thread_pool, ThreadName::UNIQ_EXACT_MERGER);
                try
                {
                    auto next_bucket_to_merge = std::make_shared<std::atomic_uint32_t>(0);

                    auto thread_func = [&lhs, &rhs, next_bucket_to_merge, is_cancelled]()
                    {
                        while (true)
                        {
                            if (is_cancelled->load())
                                return;

                            const auto bucket = next_bucket_to_merge->fetch_add(1);
                            if (bucket >= rhs.NUM_BUCKETS)
                                return;
                            lhs.impls[bucket].merge(rhs.impls[bucket]);
                        }
                    };

                    const size_t max_threads_to_enqueue = std::min<size_t>(thread_pool->getMaxThreads(), rhs.NUM_BUCKETS);
                    for (size_t i = 0; i < max_threads_to_enqueue
                         && next_bucket_to_merge->load(std::memory_order_relaxed) < rhs.NUM_BUCKETS; ++i)
                        runner.enqueueAndKeepTrack(thread_func, Priority{});
                }
                catch (...)
                {
                    is_cancelled->store(true);
                    throw;
                }
                runner.waitForAllToFinishAndRethrowFirstError();
            }
        }
    }

    void read(ReadBuffer & in)
    {
        size_t new_size = 0;
        readVarUInt(new_size, in);
        if (new_size > 100'000'000'000)
            throw DB::Exception(
                DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE, "The size of serialized hash table is suspiciously large: {}", new_size);

        if (worthConvertingToTwoLevel(new_size))
        {
            two_level_set = std::make_shared<SharedTwoLevelSet>(new_size);
            for (size_t i = 0; i < new_size; ++i)
            {
                typename SingleLevelSet::Cell x;
                x.read(in);
                asTwoLevel().insert(x.getValue());
            }
        }
        else
        {
            asSingleLevel().reserve(new_size);

            for (size_t i = 0; i < new_size; ++i)
            {
                typename SingleLevelSet::Cell x;
                x.read(in);
                asSingleLevel().insert(x.getValue());
            }
        }
    }

    void write(WriteBuffer & out) const
    {
        if (isSingleLevel())
            asSingleLevel().write(out);
        else
            /// We have to preserve compatibility with the old implementation that used only single level hash sets.
            asTwoLevel().writeAsSingleLevel(out);
    }

    size_t size() const { return isSingleLevel() ? asSingleLevel().size() : asTwoLevel().size(); }

    /// Hand out the two-level pointee for reading or merging. It is `const` and may run concurrently for the same
    /// source (ROLLUP/CUBE/GROUPING SETS merge one state into several destinations at once), so it must not mutate the
    /// buckets. Marking the pointee shared before it escapes lets `doDeepCopyIfNeeded` fork before any later in-place
    /// mutation, keeping the shared instance read-only. A freshly built pointee is solely owned by the caller, so it
    /// stays unshared and mutable in place.
    std::shared_ptr<SharedTwoLevelSet> getTwoLevelSet() const
    {
        if (two_level_set)
        {
            two_level_set->is_shared.store(true, std::memory_order_release);
            return two_level_set;
        }
        return std::make_shared<SharedTwoLevelSet>(asSingleLevel());
    }

    static bool worthConvertingToTwoLevel(size_t size) { return size > 100'000; }

    /// Whether merging `other` into this set is heavy enough to be worth handing over to a thread pool
    /// (via `parallelizeMergeMulti`) instead of merging serially in place.
    /// A two-level set is always past the threshold: it converted at `worthConvertingToTwoLevel` size.
    /// Both `size()` calls are O(1) here since they are only reached when the set is single-level.
    bool worthMergingInParallel(const UniqExactSet & other) const
    {
        /// An empty destination adopts a two-level source by pointer in `merge` (or fills from a small
        /// single-level one), which is cheaper than any parallel merge.
        if (isSingleLevel() && size() == 0)
            return false;
        /// An empty source (e.g. `uniqExactIf` where the predicate never matched, or all-`NULL` input under the
        /// `Nullable` adapter) makes `merge` a no-op; deferring it would only add the overhead of a parallel merge.
        if (other.isSingleLevel() && other.size() == 0)
            return false;
        return isTwoLevel() || other.isTwoLevel() || worthConvertingToTwoLevel(size() + other.size());
    }

    void convertToTwoLevel()
    {
        /// Already two-level: rebuilding from the cleared single-level set would drop the data.
        if (two_level_set)
            return;
        two_level_set = std::make_shared<SharedTwoLevelSet>(asSingleLevel());
        single_level_set.clear();
    }

    bool isSingleLevel() const { return !two_level_set; }
    bool isTwoLevel() const { return !!two_level_set; }

private:
    static constexpr size_t insert_prefetch_look_ahead = 16;

    /// The keys of a single-level set grouped by the bucket of the two-level set they belong to:
    /// the keys of bucket `b` are `keys[offsets[b]] .. keys[offsets[b + 1] - 1]`.
    static_assert(TwoLevelSet::NUM_BUCKETS <= 256, "Bucket indices are stored as UInt8");

    struct ScatteredSet
    {
        PODArray<value_type> keys;
        std::array<UInt32, TwoLevelSet::NUM_BUCKETS + 1> offsets{};
    };

    static void scatterByBucket(const SingleLevelSet & src, const TwoLevelSet & dst, ScatteredSet & res)
    {
        constexpr size_t NUM_BUCKETS = TwoLevelSet::NUM_BUCKETS;
        const size_t size = src.size();

        PODArray<value_type> keys(size);
        PODArray<UInt8> buckets(size);
        std::array<UInt32, NUM_BUCKETS> counts{};

        size_t i = 0;
        for (auto it = src.begin(); it != src.end(); ++it, ++i)
        {
            const auto & key = it->getValue();
            const auto bucket = TwoLevelSet::getBucketFromHash(dst.hash(key));
            keys[i] = key;
            buckets[i] = static_cast<UInt8>(bucket);
            ++counts[bucket];
        }
        chassert(i == size);

        for (size_t b = 0; b < NUM_BUCKETS; ++b)
            res.offsets[b + 1] = res.offsets[b] + counts[b];

        std::array<UInt32, NUM_BUCKETS> positions{};
        std::copy_n(res.offsets.begin(), NUM_BUCKETS, positions.begin());

        res.keys.resize(size);
        for (i = 0; i < size; ++i)
            res.keys[positions[buckets[i]]++] = keys[i];
    }

    /// Merge the sets of `places` into the first one in parallel, when some of them are single-level.
    /// Converting a single-level set to two-level allocates `NUM_BUCKETS` hash tables of the initial size,
    /// which costs far more than the set itself when it is small (e.g. per-thread partial states of one
    /// of many groups), so the single-level sets are not converted. Instead, their keys are grouped by
    /// bucket into flat arrays in parallel, and then each bucket of the destination absorbs its share of
    /// every source, with the buckets processed in parallel.
    template <typename Places, typename Accessor>
    static void parallelizeMergeMultiScattered(const Places & places, Accessor && accessor, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled)
    {
        constexpr size_t NUM_BUCKETS = TwoLevelSet::NUM_BUCKETS;
        auto * first = accessor(places[0]);

        /// The single-level sets to scatter. If the destination is single-level, it becomes an empty
        /// two-level set, and its former content is scattered as one more source.
        VectorWithMemoryTracking<const SingleLevelSet *> single_level_sources;
        VectorWithMemoryTracking<std::shared_ptr<SharedTwoLevelSet>> two_level_sources;
        single_level_sources.reserve(places.size());
        two_level_sources.reserve(places.size());

        if (first->isSingleLevel())
        {
            first->two_level_set = std::make_shared<SharedTwoLevelSet>();
            single_level_sources.push_back(&first->single_level_set);
        }

        for (size_t i = 1; i < places.size(); ++i)
        {
            const auto * src = accessor(places[i]);
            if (src->isSingleLevel())
            {
                if (src->size() != 0)
                    single_level_sources.push_back(&src->asSingleLevel());
            }
            else
            {
                /// `getTwoLevelSet` marks the pointee shared, so nobody mutates it in place while we read it.
                two_level_sources.push_back(src->getTwoLevelSet());
            }
        }

        auto & dst = first->asTwoLevelChecked();
        VectorWithMemoryTracking<ScatteredSet> scattered(single_level_sources.size());

        {
            ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, ThreadName::UNIQ_EXACT_CONVERT);
            try
            {
                auto next_source = std::make_shared<std::atomic_size_t>(0);
                auto thread_func = [&single_level_sources, &scattered, &dst, next_source, &is_cancelled]()
                {
                    while (true)
                    {
                        if (is_cancelled.load(std::memory_order_seq_cst))
                            return;

                        const size_t i = next_source->fetch_add(1);
                        if (i >= single_level_sources.size())
                            return;

                        scatterByBucket(*single_level_sources[i], dst, scattered[i]);
                    }
                };

                const size_t num_jobs = std::min<size_t>(thread_pool.getMaxThreads(), single_level_sources.size());
                for (size_t i = 0; i < num_jobs; ++i)
                    runner.enqueueAndKeepTrack(thread_func, Priority{});
            }
            catch (...)
            {
                is_cancelled.store(true);
                throw;
            }
            runner.waitForAllToFinishAndRethrowFirstError();
        }

        /// The former content of a single-level destination now lives in `scattered`.
        first->single_level_set.clear();

        if (is_cancelled.load(std::memory_order_seq_cst))
            return;

        ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, ThreadName::UNIQ_EXACT_MERGER);
        try
        {
            auto next_bucket = std::make_shared<std::atomic_uint32_t>(0);
            auto thread_func = [&two_level_sources, &scattered, &dst, next_bucket, &is_cancelled]()
            {
                while (true)
                {
                    if (is_cancelled.load(std::memory_order_seq_cst))
                        return;

                    const auto bucket = next_bucket->fetch_add(1);
                    if (bucket >= NUM_BUCKETS)
                        return;

                    auto & dst_bucket = dst.impls[bucket];
                    for (const auto & src : two_level_sources)
                        dst_bucket.merge(src->set.impls[bucket]);

                    for (const auto & src : scattered)
                        for (size_t i = src.offsets[bucket]; i < src.offsets[bucket + 1]; ++i)
                            dst_bucket.insert(src.keys[i]);
                }
            };

            const size_t num_jobs = std::min<size_t>(thread_pool.getMaxThreads(), NUM_BUCKETS);
            for (size_t i = 0; i < num_jobs && next_bucket->load(std::memory_order_relaxed) < NUM_BUCKETS; ++i)
                runner.enqueueAndKeepTrack(thread_func, Priority{});
        }
        catch (...)
        {
            is_cancelled.store(true);
            throw;
        }
        runner.waitForAllToFinishAndRethrowFirstError();
    }

    template <typename Set>
    static void ALWAYS_INLINE insertManyIntoSet(Set & set, const value_type * values, size_t n, bool prefetch)
    {
        size_t i = 0;

        if (prefetch)
        {
            for (; i + insert_prefetch_look_ahead < n; ++i)
            {
                set.prefetch(values[i + insert_prefetch_look_ahead]);
                set.insert(values[i]);
            }
        }

        for (; i < n; ++i)
            set.insert(values[i]);
    }

    SingleLevelSet & asSingleLevel() { return single_level_set; }
    const SingleLevelSet & asSingleLevel() const { return single_level_set; }

    TwoLevelSet & asTwoLevelChecked()
    {
        doDeepCopyIfNeeded();
        return two_level_set->set;
    }

    TwoLevelSet & asTwoLevel() { return two_level_set->set; }
    const TwoLevelSet & asTwoLevel() const { return two_level_set->set; }

    /// Fork a private copy before mutating a pointee that may be shared (adopted by another `UniqExactSet` via the fast
    /// path in `merge`, or handed out for reading). Forks on the pointee's `is_shared` flag, not `shared_ptr::use_count()`,
    /// which is not a cross-thread synchronization primitive. The fork is solely owned, so later mutations stay in place.
    void doDeepCopyIfNeeded()
    {
        if (two_level_set && two_level_set->is_shared.load(std::memory_order_acquire))
        {
            const auto & src = two_level_set->set;
            auto copy = std::make_shared<SharedTwoLevelSet>(src.size());
            for (size_t i = 0; i < TwoLevelSet::NUM_BUCKETS; ++i)
                copy->set.impls[i].merge(src.impls[i]);
            two_level_set = std::move(copy);
        }
    }

    SingleLevelSet single_level_set;
    std::shared_ptr<SharedTwoLevelSet> two_level_set;
};
}
