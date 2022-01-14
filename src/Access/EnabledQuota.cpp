#include <Access/EnabledQuota.h>
#include <Access/QuotaUsage.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <common/chrono_io.h>
#include <common/range.h>
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/range/algorithm/fill.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int QUOTA_EXPIRED;
}

struct EnabledQuota::Impl
{
    [[noreturn]] static void throwQuotaExceed(
        const String & user_name,
        const String & quota_name,
        ResourceType resource_type,
        ResourceAmount used,
        ResourceAmount max,
        std::chrono::seconds duration,
        std::chrono::system_clock::time_point end_of_interval)
    {
        const auto & type_info = Quota::ResourceTypeInfo::get(resource_type);
        throw Exception(
            "Quota for user " + backQuote(user_name) + " for " + to_string(duration) + " has been exceeded: "
                + type_info.outputWithAmount(used) + "/" + type_info.amountToString(max) + ". "
                + "Interval will end at " + to_string(end_of_interval) + ". " + "Name of quota template: " + backQuote(quota_name),
            ErrorCodes::QUOTA_EXPIRED);
    }


    /// Returns the end of the current interval. If the passed `current_time` is greater than that end,
    /// the function automatically recalculates the interval's end by adding the interval's duration
    /// one or more times until the interval's end is greater than `current_time`.
    /// If that recalculation occurs the function also resets amounts of resources used and sets the variable
    /// `counters_were_reset`.
    static std::chrono::system_clock::time_point getEndOfInterval(
        const Interval & interval, std::chrono::system_clock::time_point current_time, bool & counters_were_reset)
    {
        auto & end_of_interval = interval.end_of_interval;
        auto end_loaded = end_of_interval.load();
        auto end = std::chrono::system_clock::time_point{end_loaded};
        if (current_time < end)
        {
            counters_were_reset = false;
            return end;
        }

        bool need_reset_counters = false;

        do
        {
            /// Calculate the end of the next interval:
            ///  |                     X                                 |
            /// end               current_time                next_end = end + duration * n
            /// where n is an integer number, n >= 1.
            const auto duration = interval.duration;
            UInt64 n = static_cast<UInt64>((current_time - end + duration) / duration);
            end = end + duration * n;
            if (end_of_interval.compare_exchange_strong(end_loaded, end.time_since_epoch()))
            {
                need_reset_counters = true;
                break;
            }
            end = std::chrono::system_clock::time_point{end_loaded};
        }
        while (current_time >= end);

        if (need_reset_counters)
        {
            boost::range::fill(interval.used, 0);
            counters_were_reset = true;
        }
        return end;
    }


    static void used(
        const String & user_name,
        const Intervals & intervals,
        ResourceType resource_type,
        ResourceAmount amount,
        std::chrono::system_clock::time_point current_time,
        bool check_exceeded)
    {
        for (const auto & interval : intervals.intervals)
        {
            if (!interval.end_of_interval.load().count())
            {
                /// We need to calculate end of the interval if it hasn't been calculated before.
                bool dummy;
                getEndOfInterval(interval, current_time, dummy);
            }

            ResourceAmount used = (interval.used[resource_type] += amount);
            ResourceAmount max = interval.max[resource_type];
            if (!max)
                continue;

            if (used > max)
            {
                bool counters_were_reset = false;
                auto end_of_interval = getEndOfInterval(interval, current_time, counters_were_reset);
                if (counters_were_reset)
                {
                    used = (interval.used[resource_type] += amount);
                    if ((used > max) && check_exceeded)
                        throwQuotaExceed(user_name, intervals.quota_name, resource_type, used, max, interval.duration, end_of_interval);
                }
                else if (check_exceeded)
                    throwQuotaExceed(user_name, intervals.quota_name, resource_type, used, max, interval.duration, end_of_interval);
            }
        }
    }

    static void checkExceeded(
        const String & user_name,
        const Intervals & intervals,
        ResourceType resource_type,
        std::chrono::system_clock::time_point current_time)
    {
        for (const auto & interval : intervals.intervals)
        {
            if (!interval.end_of_interval.load().count())
            {
                /// We need to calculate end of the interval if it hasn't been calculated before.
                bool dummy;
                getEndOfInterval(interval, current_time, dummy);
            }

            ResourceAmount used = interval.used[resource_type];
            ResourceAmount max = interval.max[resource_type];
            if (!max)
                continue;

            if (used > max)
            {
                bool counters_were_reset = false;
                std::chrono::system_clock::time_point end_of_interval = getEndOfInterval(interval, current_time, counters_were_reset);
                if (!counters_were_reset)
                    throwQuotaExceed(user_name, intervals.quota_name, resource_type, used, max, interval.duration, end_of_interval);
            }
        }
    }

    static void checkExceeded(
        const String & user_name,
        const Intervals & intervals,
        std::chrono::system_clock::time_point current_time)
    {
        for (auto resource_type : collections::range(Quota::MAX_RESOURCE_TYPE))
            checkExceeded(user_name, intervals, resource_type, current_time);
    }
};


EnabledQuota::Interval::Interval()
{
    for (auto resource_type : collections::range(MAX_RESOURCE_TYPE))
    {
        used[resource_type].store(0);
        max[resource_type] = 0;
    }
}


EnabledQuota::Interval & EnabledQuota::Interval::operator =(const Interval & src)
{
    if (this == &src)
        return *this;

    randomize_interval = src.randomize_interval;
    duration = src.duration;
    end_of_interval.store(src.end_of_interval.load());
    for (auto resource_type : collections::range(MAX_RESOURCE_TYPE))
    {
        max[resource_type] = src.max[resource_type];
        used[resource_type].store(src.used[resource_type].load());
    }
    return *this;
}


std::optional<QuotaUsage> EnabledQuota::Intervals::getUsage(std::chrono::system_clock::time_point current_time) const
{
    if (!quota_id)
        return {};
    QuotaUsage usage;
    usage.quota_id = *quota_id;
    usage.quota_name = quota_name;
    usage.quota_key = quota_key;
    usage.intervals.reserve(intervals.size());
    for (const auto & in : intervals)
    {
        usage.intervals.push_back({});
        auto & out = usage.intervals.back();
        out.duration = in.duration;
        out.randomize_interval = in.randomize_interval;
        bool counters_were_reset = false;
        out.end_of_interval = Impl::getEndOfInterval(in, current_time, counters_were_reset);
        for (auto resource_type : collections::range(MAX_RESOURCE_TYPE))
        {
            if (in.max[resource_type])
                out.max[resource_type] = in.max[resource_type];
            out.used[resource_type] = in.used[resource_type];
        }
    }
    return usage;
}


EnabledQuota::EnabledQuota(const Params & params_) : params(params_)
{
}

EnabledQuota::~EnabledQuota() = default;


void EnabledQuota::used(ResourceType resource_type, ResourceAmount amount, bool check_exceeded) const
{
    used({resource_type, amount}, check_exceeded);
}


void EnabledQuota::used(const std::pair<ResourceType, ResourceAmount> & resource, bool check_exceeded) const
{
    auto loaded = intervals.load();
    auto current_time = std::chrono::system_clock::now();
    Impl::used(getUserName(), *loaded, resource.first, resource.second, current_time, check_exceeded);
}


void EnabledQuota::used(const std::pair<ResourceType, ResourceAmount> & resource1, const std::pair<ResourceType, ResourceAmount> & resource2, bool check_exceeded) const
{
    auto loaded = intervals.load();
    auto current_time = std::chrono::system_clock::now();
    Impl::used(getUserName(), *loaded, resource1.first, resource1.second, current_time, check_exceeded);
    Impl::used(getUserName(), *loaded, resource2.first, resource2.second, current_time, check_exceeded);
}


void EnabledQuota::used(const std::pair<ResourceType, ResourceAmount> & resource1, const std::pair<ResourceType, ResourceAmount> & resource2, const std::pair<ResourceType, ResourceAmount> & resource3, bool check_exceeded) const
{
    auto loaded = intervals.load();
    auto current_time = std::chrono::system_clock::now();
    Impl::used(getUserName(), *loaded, resource1.first, resource1.second, current_time, check_exceeded);
    Impl::used(getUserName(), *loaded, resource2.first, resource2.second, current_time, check_exceeded);
    Impl::used(getUserName(), *loaded, resource3.first, resource3.second, current_time, check_exceeded);
}


void EnabledQuota::used(const std::vector<std::pair<ResourceType, ResourceAmount>> & resources, bool check_exceeded) const
{
    auto loaded = intervals.load();
    auto current_time = std::chrono::system_clock::now();
    for (const auto & resource : resources)
        Impl::used(getUserName(), *loaded, resource.first, resource.second, current_time, check_exceeded);
}


void EnabledQuota::checkExceeded() const
{
    auto loaded = intervals.load();
    Impl::checkExceeded(getUserName(), *loaded, std::chrono::system_clock::now());
}


void EnabledQuota::checkExceeded(ResourceType resource_type) const
{
    auto loaded = intervals.load();
    Impl::checkExceeded(getUserName(), *loaded, resource_type, std::chrono::system_clock::now());
}


std::optional<QuotaUsage> EnabledQuota::getUsage() const
{
    auto loaded = intervals.load();
    return loaded->getUsage(std::chrono::system_clock::now());
}


std::shared_ptr<const EnabledQuota> EnabledQuota::getUnlimitedQuota()
{
    static const std::shared_ptr<const EnabledQuota> res = []
    {
        auto unlimited_quota = std::shared_ptr<EnabledQuota>(new EnabledQuota);
        unlimited_quota->intervals = boost::make_shared<Intervals>();
        return unlimited_quota;
    }();
    return res;
}

}
