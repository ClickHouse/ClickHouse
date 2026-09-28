#pragma once

#include <AggregateFunctions/MergeWaveStats.h>

#include <Common/ThreadGroupSwitcher.h>
#include <Common/HashTable/HashSet.h>
#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/VectorWithMemoryTracking.h>

#include <base/defines.h>
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
            /// Wave diagnostics (`log_per_bucket_merge_timings`): the coordinating thread set the
            /// sink before dispatching this wave; the pooled tasks account their thread-CPU time
            /// and worker identity to it.
            MergeWaveStats * wave_stats = current_merge_wave_stats;

            /// The conversions go through a local runner that tracks its own tasks and waits only
            /// for them, same as `parallelizeMergeMulti` below: prepare is called from
            /// concurrently running bucket mergers sharing `thread_pool`, so a bare
            /// `thread_pool.wait()` would block on unrelated jobs and steal their exceptions.
            /// The runner switches its tasks to the enqueuer's thread group and names them, so the
            /// manual `ThreadGroupSwitcher` of the detached-job pattern is not needed here.
            ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, ThreadName::UNIQ_EXACT_CONVERT);
            try
            {
                auto data_vec_atomic_index = std::make_shared<std::atomic_uint32_t>(0);
                auto thread_func = [&places, &accessor, data_vec_atomic_index, &is_cancelled, wave_stats]()
                {
                    MergeWaveTaskTimer task_timer(wave_stats);

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
            for (size_t j = 1; j < places.size(); ++j)
            {
                if (is_cancelled.load(std::memory_order_seq_cst))
                    return;
                first->merge(*accessor(places[j]), &thread_pool, &is_cancelled);
            }
            return;
        }

        /// All sets are two-level, perform parallel bucket-wise merge.
        ///
        /// Every participant is materialized through `asTwoLevelChecked()` below, so a shared
        /// pointee here would silently take the serial 256-sub-table deep copy in
        /// `doDeepCopyIfNeeded` inside the wave. Callers must hand in exclusively owned states
        /// (the keyed and without-key merge paths do: their states come straight from the
        /// aggregation hash tables and never escape before the merge).
        for (size_t i = 0; i < places.size(); ++i)
            chassert(
                !accessor(places[i])->hasSharedTwoLevelPointee(),
                "UniqExactSet::parallelizeMergeMulti: a participating state's two-level pointee is shared; "
                "the deep copy in doDeepCopyIfNeeded must not fire inside the merge wave");

        auto & first_two_level = first->asTwoLevelChecked();
        constexpr size_t NUM_BUCKETS = TwoLevelSet::NUM_BUCKETS;

        /// Pre-fetch all two-level set pointers to avoid concurrent access to getTwoLevelSet().
        VectorWithMemoryTracking<TwoLevelSet *> two_level_ptrs;
        two_level_ptrs.reserve(places.size());
        for (size_t i = 0; i < places.size(); ++i)
            two_level_ptrs.emplace_back(&accessor(places[i])->asTwoLevelChecked());

        /// The same L2 gate as `insertMany` on the destination's total buffer, decided once before the
        /// workers start mutating it (summing sub-table sizes mid-merge would race them).
        const bool prefetch_merge = first_two_level.getBufferSizeInBytes() > getL2CacheSize();

        /// Wave diagnostics (`log_per_bucket_merge_timings`), same sink as in
        /// `parallelizeMergePrepare`. The pairwise fallback above needs no read of its own: it
        /// runs `merge` on this same thread, which reads the sink at its pooled section.
        MergeWaveStats * wave_stats = current_merge_wave_stats;
        ThreadPoolCallbackRunnerLocal<void> runner(thread_pool, ThreadName::UNIQ_EXACT_MERGER);
        try
        {
            auto next_bucket_to_merge = std::make_shared<std::atomic_uint32_t>(0);

            auto thread_func = [&two_level_ptrs, &first_two_level, prefetch_merge, next_bucket_to_merge, &is_cancelled, wave_stats]()
            {
                MergeWaveTaskTimer task_timer(wave_stats);

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

                        /// An empty destination sub-table keeps `merge` for its buffer-copy fast path.
                        if (prefetch_merge && !first_two_level.impls[bucket].empty())
                            two_level_ptrs[j]->impls[bucket].template mergeInto</*prefetch=*/ true>(first_two_level.impls[bucket]);
                        else
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
            auto & lhs = asSingleLevel();
            /// Once the destination spills out of L2 the merge is cache-miss bound (same gate as `insertMany`):
            /// take the prefetching `mergeInto`, which also skips `merge`'s preemptive dst+src resize - for
            /// high-overlap merges that over-allocates and forces a full rehash. Below the gate keep `merge`
            /// unchanged, including its empty-destination buffer-copy fast path (an above-gate destination
            /// cannot be empty: its buffer only grows past L2 by holding elements).
            if (lhs.getBufferSizeInBytes() > getL2CacheSize())
                other.asSingleLevel().template mergeInto</*prefetch=*/ true>(lhs);
            else
                lhs.merge(other.asSingleLevel());
        }
        else
        {
            auto & lhs = asTwoLevelChecked();

            /// The same L2 gate on the two-level destination's total buffer, decided once up front: the buffer
            /// only grows during the merge, and in the pool path summing sub-table sizes mid-merge would race
            /// the worker threads.
            const bool prefetch_merge = lhs.getBufferSizeInBytes() > getL2CacheSize();

            if (other.isSingleLevel())
            {
                if (prefetch_merge)
                    other.asSingleLevel().template mergeInto</*prefetch=*/ true>(lhs);
                else
                    lhs.merge(other.asSingleLevel());
                return;
            }

            /// `getTwoLevelSet` marked the pointee shared, so no other holder mutates it in place while we read it.
            const auto rhs_ptr = other.getTwoLevelSet();
            const auto & rhs = rhs_ptr->set;
            if (!thread_pool)
            {
                for (size_t i = 0; i < rhs.NUM_BUCKETS; ++i)
                {
                    /// An empty destination sub-table keeps `merge` for its buffer-copy fast path.
                    if (prefetch_merge && !lhs.impls[i].empty())
                        rhs.impls[i].template mergeInto</*prefetch=*/ true>(lhs.impls[i]);
                    else
                        lhs.impls[i].merge(rhs.impls[i]);
                }
            }
            else
            {

                /// Usage of lhs and rhs is fine. The references belong to *this and will outlive `runner`, so the order of destruction is ok
                /// Wave diagnostics (`log_per_bucket_merge_timings`): non-null only when this
                /// merge runs inside a multi-way wave (the internal pairwise fallback of
                /// `parallelizeMergeMulti`), whose coordinating thread set the sink.
                MergeWaveStats * wave_stats = current_merge_wave_stats;
                ThreadPoolCallbackRunnerLocal<void> runner(*thread_pool, ThreadName::UNIQ_EXACT_MERGER);
                try
                {
                    auto next_bucket_to_merge = std::make_shared<std::atomic_uint32_t>(0);

                    auto thread_func = [&lhs, &rhs, prefetch_merge, next_bucket_to_merge, is_cancelled, wave_stats]()
                    {
                        MergeWaveTaskTimer task_timer(wave_stats);

                        while (true)
                        {
                            if (is_cancelled->load())
                                return;

                            const auto bucket = next_bucket_to_merge->fetch_add(1);
                            if (bucket >= rhs.NUM_BUCKETS)
                                return;
                            /// An empty destination sub-table keeps `merge` for its buffer-copy fast path.
                            if (prefetch_merge && !lhs.impls[bucket].empty())
                                rhs.impls[bucket].template mergeInto</*prefetch=*/ true>(lhs.impls[bucket]);
                            else
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

    /// True when this set holds a two-level pointee that has escaped to another holder (`is_shared`
    /// set by `getTwoLevelSet`, e.g. when an empty destination adopted it in `merge`): the next
    /// mutating access through `asTwoLevelChecked` forks it - the serial 256-sub-table deep copy in
    /// `doDeepCopyIfNeeded` - instead of mutating in place. `parallelizeMergeMulti` asserts this is
    /// false for every participant, so that the deep copy never silently fires inside the wave.
    bool hasSharedTwoLevelPointee() const
    {
        return two_level_set && two_level_set->is_shared.load(std::memory_order_acquire);
    }

private:
    static constexpr size_t insert_prefetch_look_ahead = 16;

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
