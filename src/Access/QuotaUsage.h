#pragma once

#include <Access/Quota.h>
#include <chrono>


namespace DB
{
/// The information about a quota consumption.
struct QuotaUsage
{
    using ResourceType = Quota::ResourceType;
    using ResourceAmount = Quota::ResourceAmount;
    static constexpr auto MAX_RESOURCE_TYPE = Quota::MAX_RESOURCE_TYPE;

    struct Interval
    {
        ResourceAmount used[MAX_RESOURCE_TYPE];
        std::optional<ResourceAmount> max[MAX_RESOURCE_TYPE];
        std::chrono::seconds duration = std::chrono::seconds::zero();
        bool randomize_interval = false;
        std::chrono::system_clock::time_point end_of_interval;
        Interval();
    };

    std::vector<Interval> intervals;
    UUID quota_id;
    String quota_name;
    String quota_key;
    QuotaUsage();
};
}
