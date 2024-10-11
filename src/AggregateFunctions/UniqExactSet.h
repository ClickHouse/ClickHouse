#pragma once

#include <exception>
#include <Common/CurrentThread.h>
#include <Common/HashTable/HashSet.h>
#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>


namespace DB
{

namespace ErrorCodes
{
extern const int TOO_LARGE_ARRAY_SIZE;
}

template <typename SingleLevelSet, typename TwoLevelSet>
class UniqExactSet
{
    static_assert(std::is_same_v<typename SingleLevelSet::value_type, typename TwoLevelSet::value_type>);
    static_assert(std::is_same_v<typename SingleLevelSet::Cell::State, HashTableNoState>);

public:
    using value_type = typename SingleLevelSet::value_type;

    template <typename Arg, bool use_single_level_hash_table = true>
    auto ALWAYS_INLINE insert(Arg && arg)
    {
        if constexpr (use_single_level_hash_table)
            asSingleLevel().insert(std::forward<Arg>(arg));
        else
            asTwoLevel().insert(std::forward<Arg>(arg));
    }

    /// In merge, if one of the lhs and rhs is twolevelset and the other is singlelevelset, then the singlelevelset will need to convertToTwoLevel().
    /// It's not in parallel and will cost extra large time if the thread_num is large.
    /// This method will convert all the SingleLevelSet to TwoLevelSet in parallel if the hashsets are not all singlelevel or not all twolevel.
    static void parallelizeMergePrepare(const std::vector<UniqExactSet *> & data_vec, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled)
    {
        UInt64 single_level_set_num = 0;
        UInt64 all_single_hash_size = 0;

        for (auto ele : data_vec)
        {
            if (ele->isSingleLevel())
                single_level_set_num ++;
        }

        if (single_level_set_num == data_vec.size())
        {
            for (auto ele : data_vec)
                all_single_hash_size += ele->size();
        }

        /// If all the hashtables are mixed by singleLevel and twoLevel, or all singleLevel (larger than 6000 for average value), they could be converted into
        /// twoLevel hashtables in parallel and then merge together. please refer to the following PR for more details.
        /// https://github.com/ClickHouse/ClickHouse/pull/50748
        /// https://github.com/ClickHouse/ClickHouse/pull/52973
        if ((single_level_set_num > 0 && single_level_set_num < data_vec.size()) || ((all_single_hash_size/data_vec.size()) > 6000))
        {
            try
            {
                auto data_vec_atomic_index = std::make_shared<std::atomic_uint32_t>(0);
                auto thread_func = [data_vec, data_vec_atomic_index, &is_cancelled, thread_group = CurrentThread::getGroup()]()
                {
                    SCOPE_EXIT_SAFE(
                        if (thread_group)
                            CurrentThread::detachFromGroupIfNotDetached();
                    );
                    if (thread_group)
                        CurrentThread::attachToGroupIfDetached(thread_group);

                    setThreadName("UniqExaConvert");

                    while (true)
                    {
                        if (is_cancelled.load(std::memory_order_seq_cst))
                            return;

                        const auto i = data_vec_atomic_index->fetch_add(1);
                        if (i >= data_vec.size())
                            return;
                        if (data_vec[i]->isSingleLevel())
                            data_vec[i]->convertToTwoLevel();
                    }
                };
                for (size_t i = 0; i < std::min<size_t>(thread_pool.getMaxThreads(), single_level_set_num); ++i)
                    thread_pool.scheduleOrThrowOnError(thread_func);

                thread_pool.wait();
            }
            catch (...)
            {
                thread_pool.wait();
                throw;
            }
        }
    }

    auto merge(const UniqExactSet & other, ThreadPool * thread_pool = nullptr, std::atomic<bool> * is_cancelled = nullptr)
    {
        /// If the size is large, we may convert the singleLevelHash to twoLevelHash and merge in parallel.
        if (other.size() > 40000)
        {
            if (isSingleLevel())
                convertToTwoLevel();
        }

        if (isSingleLevel() && other.isTwoLevel())
            convertToTwoLevel();

        if (isSingleLevel())
        {
            asSingleLevel().merge(other.asSingleLevel());
        }
        else
        {
            auto & lhs = asTwoLevel();
            const auto rhs_ptr = other.getTwoLevelSet();
            const auto & rhs = *rhs_ptr;
            if (!thread_pool)
            {
                for (size_t i = 0; i < rhs.NUM_BUCKETS; ++i)
                {
                    lhs.impls[i].merge(rhs.impls[i]);
                }
            }
            else
            {
                try
                {
                    auto next_bucket_to_merge = std::make_shared<std::atomic_uint32_t>(0);

                    auto thread_func = [&lhs, &rhs, next_bucket_to_merge, is_cancelled, thread_group = CurrentThread::getGroup()]()
                    {
                        SCOPE_EXIT_SAFE(
                            if (thread_group)
                                CurrentThread::detachFromGroupIfNotDetached();
                        );
                        if (thread_group)
                            CurrentThread::attachToGroupIfDetached(thread_group);
                        setThreadName("UniqExactMerger");

                        while (true)
                        {
                            if (is_cancelled->load(std::memory_order_seq_cst))
                                return;

                            const auto bucket = next_bucket_to_merge->fetch_add(1);
                            if (bucket >= rhs.NUM_BUCKETS)
                                return;
                            lhs.impls[bucket].merge(rhs.impls[bucket]);
                        }
                    };

                    for (size_t i = 0; i < std::min<size_t>(thread_pool->getMaxThreads(), rhs.NUM_BUCKETS); ++i)
                        thread_pool->scheduleOrThrowOnError(thread_func);
                    thread_pool->wait();
                }
                catch (...)
                {
                    thread_pool->wait();
                    throw;
                }
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
            two_level_set = std::make_shared<TwoLevelSet>(new_size);
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

    /// To convert set to two level before merging (we cannot just call convertToTwoLevel() on right hand side set, because it is declared const).
    std::shared_ptr<TwoLevelSet> getTwoLevelSet() const
    {
        return two_level_set ? two_level_set : std::make_shared<TwoLevelSet>(asSingleLevel());
    }

    static bool worthConvertingToTwoLevel(size_t size) { return size > 100'000; }

    void convertToTwoLevel()
    {
        two_level_set = getTwoLevelSet();
        single_level_set.clear();
    }

    bool isSingleLevel() const { return !two_level_set; }
    bool isTwoLevel() const { return !!two_level_set; }

private:
    SingleLevelSet & asSingleLevel() { return single_level_set; }
    const SingleLevelSet & asSingleLevel() const { return single_level_set; }

    TwoLevelSet & asTwoLevel() { return *two_level_set; }
    const TwoLevelSet & asTwoLevel() const { return *two_level_set; }

    SingleLevelSet single_level_set;
    std::shared_ptr<TwoLevelSet> two_level_set;
};
}
