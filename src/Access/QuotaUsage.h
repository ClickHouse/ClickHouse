#pragma once

#include <Access/Common/QuotaDefs.h>
#include <Common/ProfileEvents.h>
#include <chrono>
#include <optional>
#include <vector>


namespace DB
{
/// The information about a quota consumption.
struct QuotaUsage
{
    /// The consumption of a single profile event a quota defines a limit over.
    struct ProfileEventUsage
    {
        ProfileEvents::Event event = ProfileEvents::Event(0);
        QuotaValue used = 0;
        QuotaValue max = 0; /// 0 means "no limit, track only".
    };

    struct Interval
    {
        QuotaValue used[static_cast<size_t>(QuotaType::MAX)]{};
        std::optional<QuotaValue> max[static_cast<size_t>(QuotaType::MAX)];
        std::vector<ProfileEventUsage> profile_events;
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
