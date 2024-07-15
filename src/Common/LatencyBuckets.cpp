#include <atomic>
#include <Common/logger_useful.h>
#include <Common/LatencyBuckets.h>

#define APPLY_FOR_EVENTS(M) \
    M(Empty, "Empty", 0) \


namespace LatencyBuckets
{

#define M(NAME, DOCUMENTATION, ...) extern const Event NAME = Event(__COUNTER__);
    APPLY_FOR_EVENTS(M)
#undef M

constexpr Event END = Event(__COUNTER__);

BucketList global_bucket_lists_array[END] =
{
#define M(NAME, DOCUMENTATION, ...) BucketList(CH_VA_ARGS_NARGS(__VA_ARGS__) + 1),
    APPLY_FOR_EVENTS(M)
#undef M
};
BucketLists global_bucket_lists(global_bucket_lists_array);

const Event BucketLists::num_events = END;

void BucketLists::reset()
{
    if (bucket_lists)
    {
        for (Event i = Event(0); i < num_events; ++i)
            for (auto & bucket : bucket_lists[i])
                bucket.store(0, std::memory_order_relaxed);
    }
}

const char * getName(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, ...) #NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

const char * getDocumentation(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION, ...) DOCUMENTATION "",
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

std::vector<Count> & getBucketBounds(Event event)
{
    static std::vector<Count> all_buckets_bounds[] =
    {
    #define M(NAME, DOCUMENTATION, ...) {__VA_ARGS__, UINT_MAX},
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return all_buckets_bounds[event];
}

Event end() { return END; }

void increment(Event event, Count amount)
{
    global_bucket_lists.increment(event, amount);
}

void BucketLists::increment(Event event, Count amount)
{
    auto & bucket_bounds = getBucketBounds(event);
    auto & bucket_list = this->bucket_lists[event];

    for (ssize_t i = bucket_list.size() - 1; i > -1 && bucket_bounds[i] >= amount; --i)
        bucket_list[i].fetch_add(1, std::memory_order_relaxed);
}

}

#undef APPLY_FOR_EVENTS
