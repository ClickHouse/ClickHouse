#include <Access/EnabledQuota.h>
#include <Access/QuotaUsage.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <base/chrono_io.h>
#include <base/range.h>
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
        QuotaType quota_type,
        QuotaValue used,
        QuotaValue max,
        std::chrono::seconds duration,
        std::chrono::system_clock::time_point end_of_interval)
    {
        const auto & type_info = QuotaTypeInfo::get(quota_type);
        throw Exception(
            "Quota for user " + backQuote(user_name) + " for " + to_string(duration) + " has been exceeded: "
                + type_info.valueToStringWithName(used) + "/" + type_info.valueToString(max) + ". "
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
                /// We reset counters only if the interval's end has been calculated before.
                /// If it hasn't we just calculate the interval's end for the first time and don't reset counters yet.
                need_reset_counters = (end_loaded.count() != 0);
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
        QuotaType quota_type,
        QuotaValue value,
        std::chrono::system_clock::time_point current_time,
        bool check_exceeded)
    {
        for (const auto & interval : intervals.intervals)
        {
            auto quota_type_i = static_cast<size_t>(quota_type);
            QuotaValue used = (interval.used[quota_type_i] += value);
            QuotaValue max = interval.max[quota_type_i];
            if (!max)
                continue;
            if (used > max)
            {
                bool counters_were_reset = false;
                auto end_of_interval = getEndOfInterval(interval, current_time, counters_were_reset);
                if (counters_were_reset)
                {
                    used = (interval.used[quota_type_i] += value);
                    if ((used > max) && check_exceeded)
                        throwQuotaExceed(user_name, intervals.quota_name, quota_type, used, max, interval.duration, end_of_interval);
                }
                else if (check_exceeded)
                    throwQuotaExceed(user_name, intervals.quota_name, quota_type, used, max, interval.duration, end_of_interval);
            }
        }
    }

    static void checkExceeded(
        const String & user_name,
        const Intervals & intervals,
        QuotaType quota_type,
        std::chrono::system_clock::time_point current_time)
    {
        auto quota_type_i = static_cast<size_t>(quota_type);
        for (const auto & interval : intervals.intervals)
        {
            QuotaValue used = interval.used[quota_type_i];
            QuotaValue max = interval.max[quota_type_i];
            if (!max)
                continue;
            if (used > max)
            {
                bool counters_were_reset = false;
                std::chrono::system_clock::time_point end_of_interval = getEndOfInterval(interval, current_time, counters_were_reset);
                if (!counters_were_reset)
                    throwQuotaExceed(user_name, intervals.quota_name, quota_type, used, max, interval.duration, end_of_interval);
            }
        }
    }

    static void checkExceeded(
        const String & user_name,
        const Intervals & intervals,
        std::chrono::system_clock::time_point current_time)
    {
        for (auto quota_type : collections::range(QuotaType::MAX))
            checkExceeded(user_name, intervals, quota_type, current_time);
    }
};


EnabledQuota::Interval::Interval()
{
    for (auto quota_type : collections::range(QuotaType::MAX))
    {
        auto quota_type_i = static_cast<size_t>(quota_type);
        used[quota_type_i].store(0);
        max[quota_type_i] = 0;
    }
}


EnabledQuota::Interval & EnabledQuota::Interval::operator =(const Interval & src)
{
    if (this == &src)
        return *this;

    randomize_interval = src.randomize_interval;
    duration = src.duration;
    end_of_interval.store(src.end_of_interval.load());
    for (auto quota_type : collections::range(QuotaType::MAX))
    {
        auto quota_type_i = static_cast<size_t>(quota_type);
        max[quota_type_i] = src.max[quota_type_i];
        used[quota_type_i].store(src.used[quota_type_i].load());
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
        for (auto quota_type : collections::range(QuotaType::MAX))
        {
            auto quota_type_i = static_cast<size_t>(quota_type);
            if (in.max[quota_type_i])
                out.max[quota_type_i] = in.max[quota_type_i];
            out.used[quota_type_i] = in.used[quota_type_i];
        }
    }
    return usage;
}


EnabledQuota::EnabledQuota(const Params & params_) : params(params_)
{
}

EnabledQuota::~EnabledQuota() = default;


void EnabledQuota::used(QuotaType quota_type, QuotaValue value, bool check_exceeded) const
{
    used({quota_type, value}, check_exceeded);
}


void EnabledQuota::used(const std::pair<QuotaType, QuotaValue> & usage1, bool check_exceeded) const
{
    auto loaded = intervals.load();
    auto current_time = std::chrono::system_clock::now();
    Impl::used(getUserName(), *loaded, usage1.first, usage1.second, current_time, check_exceeded);
}


void EnabledQuota::used(const std::pair<QuotaType, QuotaValue> & usage1, const std::pair<QuotaType, QuotaValue> & usage2, bool check_exceeded) const
{
    auto loaded = intervals.load();
    auto current_time = std::chrono::system_clock::now();
    Impl::used(getUserName(), *loaded, usage1.first, usage1.second, current_time, check_exceeded);
    Impl::used(getUserName(), *loaded, usage2.first, usage2.second, current_time, check_exceeded);
}


void EnabledQuota::used(const std::pair<QuotaType, QuotaValue> & usage1, const std::pair<QuotaType, QuotaValue> & usage2, const std::pair<QuotaType, QuotaValue> & usage3, bool check_exceeded) const
{
    auto loaded = intervals.load();
    auto current_time = std::chrono::system_clock::now();
    Impl::used(getUserName(), *loaded, usage1.first, usage1.second, current_time, check_exceeded);
    Impl::used(getUserName(), *loaded, usage2.first, usage2.second, current_time, check_exceeded);
    Impl::used(getUserName(), *loaded, usage3.first, usage3.second, current_time, check_exceeded);
}


void EnabledQuota::used(const std::vector<std::pair<QuotaType, QuotaValue>> & usages, bool check_exceeded) const
{
    auto loaded = intervals.load();
    auto current_time = std::chrono::system_clock::now();
    for (const auto & usage : usages)
        Impl::used(getUserName(), *loaded, usage.first, usage.second, current_time, check_exceeded);
}


void EnabledQuota::checkExceeded() const
{
    auto loaded = intervals.load();
    Impl::checkExceeded(getUserName(), *loaded, std::chrono::system_clock::now());
}


void EnabledQuota::checkExceeded(QuotaType quota_type) const
{
    auto loaded = intervals.load();
    Impl::checkExceeded(getUserName(), *loaded, quota_type, std::chrono::system_clock::now());
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
