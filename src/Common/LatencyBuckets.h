#pragma once

#include <base/types.h>
#include <base/strong_typedef.h>

namespace LatencyBuckets
{
    using Event = StrongTypedef<size_t, struct LatencyEventTag>;
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

        BucketList & operator[] (Event event)
        {
            return bucket_lists[event];
        }

        const BucketList & operator[] (Event event) const
        {
            return bucket_lists[event];
        }

        void increment(Event event, Count amount = 1);

        void reset();

        static const Event num_events;
    };

    void increment(Event event, Count amount = 1);

    const char * getName(Event event);

    const char * getDocumentation(Event event);

    std::vector<Count> & getBucketBounds(Event event);

    Event end();
}
