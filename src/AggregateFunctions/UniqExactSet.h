#pragma once

#include <Common/CurrentThread.h>
#include <Common/HashTable/HashSet.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/scope_guard_safe.h>


namespace DB
{

template <typename SingleLevelSet, typename TwoLevelSet>
class UniqExactSet
{
    static_assert(std::is_same_v<typename SingleLevelSet::value_type, typename TwoLevelSet::value_type>);

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

    auto merge(const UniqExactSet & other, ThreadPool * thread_pool = nullptr)
    {
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
                    lhs.impls[i].merge(rhs.impls[i]);
            }
            else
            {
                auto next_bucket_to_merge = std::make_shared<std::atomic_uint32_t>(0);

                auto thread_func = [&lhs, &rhs, next_bucket_to_merge, thread_group = CurrentThread::getGroup()]()
                {
                    SCOPE_EXIT_SAFE(
                        if (thread_group)
                            CurrentThread::detachFromGroupIfNotDetached();
                    );
                    if (thread_group)
                        CurrentThread::attachToGroupIfDetached(thread_group);
                    setThreadName("UniqExactMerger");

                    /// The ref_count in share_ptr will have increase or decrease in each while loop, which will cause heavy lock contention.
                    /// Get the raw_ptr from the share_ptr will reduce the ref_count atomic_update.
                    auto next_bucket_to_merge_raw = next_bucket_to_merge.get();
                    while (true)
                    {
                        const auto bucket = next_bucket_to_merge_raw->fetch_add(1);
                        if (bucket >= rhs.NUM_BUCKETS)
                            return;
                        lhs.impls[bucket].merge(rhs.impls[bucket]);
                    }
                };

                for (size_t i = 0; i < std::min<size_t>(thread_pool->getMaxThreads(), rhs.NUM_BUCKETS); ++i)
                    thread_pool->scheduleOrThrowOnError(thread_func);
                thread_pool->wait();
            }
        }
    }

    void read(ReadBuffer & in) { asSingleLevel().read(in); }

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
