#pragma once

#include <Access/Common/QuotaDefs.h>
#include <chrono>
#include <optional>


namespace DB
{
/// The information about a quota consumption.
struct QuotaUsage
{
    struct Interval
    {
        QuotaValue used[static_cast<size_t>(QuotaType::MAX)];
        std::optional<QuotaValue> max[static_cast<size_t>(QuotaType::MAX)];
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
