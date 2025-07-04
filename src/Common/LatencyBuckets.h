#pragma once

#include <base/types.h>
#include <base/strong_typedef.h>

#include <atomic>

namespace LatencyBuckets
{
    using LatencyEvent = StrongTypedef<size_t, struct LatencyEventTag>;
    using Count = UInt64;
    using BucketList = std::vector<std::atomic<Count>>;
    class BucketLists;

    extern BucketLists global_bucket_lists;

    class BucketLists
    {
    private:
        BucketList * bucket_lists = nullptr;

    public:

        explicit BucketLists(BucketList * allocated_bucket_lists) noexcept
            : bucket_lists(allocated_bucket_lists) {}

        BucketList & operator[] (LatencyEvent event)
        {
            return bucket_lists[event];
        }

        const BucketList & operator[] (LatencyEvent event) const
        {
            return bucket_lists[event];
        }

        void increment(LatencyEvent event, Count amount = 1);

        void reset();

        static const LatencyEvent num_events;
    };

    void increment(LatencyEvent event, Count amount = 1);

    const char * getName(LatencyEvent event);

    const char * getDocumentation(LatencyEvent event);

    std::vector<Count> & getBucketBounds(LatencyEvent event);

    LatencyEvent end();
}
