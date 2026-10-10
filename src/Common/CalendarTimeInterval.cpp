#include <Common/CalendarTimeInterval.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

CalendarTimeInterval::CalendarTimeInterval(const CalendarTimeInterval::Intervals & intervals)
{
    for (auto [kind, val] : intervals)
    {
        switch (kind.kind)
        {
            case IntervalKind::Kind::Nanosecond:
            case IntervalKind::Kind::Microsecond:
            case IntervalKind::Kind::Millisecond:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sub-second intervals are not supported here");

            case IntervalKind::Kind::Second:
            case IntervalKind::Kind::Minute:
            case IntervalKind::Kind::Hour:
            case IntervalKind::Kind::Day:
            case IntervalKind::Kind::Week:
                seconds += val * kind.toAvgSeconds();
                break;

            case IntervalKind::Kind::Month:
                months += val;
                break;
            case IntervalKind::Kind::Quarter:
                months += val * 3;
                break;
            case IntervalKind::Kind::Year:
                months += val * 12;
                break;
        }
    }
}

CalendarTimeInterval::Intervals CalendarTimeInterval::toIntervals() const
{
    Intervals res;
    auto greedy = [&](UInt64 x, std::initializer_list<std::pair<IntervalKind, UInt64>> kinds)
    {
        for (auto [kind, count] : kinds)
        {
            UInt64 k = x / count;
            if (k == 0)
                continue;
            x -= k * count;
            res.emplace_back(kind, k);
        }
        chassert(x == 0);
    };
    greedy(months, {{IntervalKind::Kind::Year, 12}, {IntervalKind::Kind::Month, 1}});
    greedy(seconds, {{IntervalKind::Kind::Week, 3600*24*7}, {IntervalKind::Kind::Day, 3600*24}, {IntervalKind::Kind::Hour, 3600}, {IntervalKind::Kind::Minute, 60}, {IntervalKind::Kind::Second, 1}});
    if (res.empty())
        res.emplace_back(IntervalKind::Kind::Second, 0);
    return res;
}

UInt64 CalendarTimeInterval::minSeconds() const
{
    return 3600*24 * (months/12 * 365 + months%12 * 28) + seconds;
}

UInt64 CalendarTimeInterval::maxSeconds() const
{
    return 3600*24 * (months/12 * 366 + months%12 * 31) + seconds;
}

void CalendarTimeInterval::assertSingleUnit() const
{
    if (seconds && months)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interval shouldn't contain both calendar units and clock units (e.g. months and days)");
}

void CalendarTimeInterval::assertPositive() const
{
    if (!seconds && !months)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Interval must be positive");
}

/// Number of whole months between 1970-01-01 and `t`.
static Int64 toAbsoluteMonth(std::chrono::system_clock::time_point t)
{
    std::chrono::year_month_day ymd(std::chrono::floor<std::chrono::days>(t));
    return (Int64(int(ymd.year())) - 1970) * 12 + Int64(unsigned(ymd.month()) - 1);
}

static std::chrono::sys_seconds startOfAbsoluteMonth(Int64 absolute_month)
{
    Int64 year = absolute_month >= 0 ? absolute_month/12 : -((-absolute_month+11)/12);
    Int64 month = absolute_month - year*12;
    chassert(month >= 0 && month < 12);
    std::chrono::year_month_day ymd(
        std::chrono::year(int(year + 1970)),
        std::chrono::month(unsigned(month + 1)),
        std::chrono::day(1));
    return std::chrono::sys_days(ymd);
}

std::chrono::sys_seconds CalendarTimeInterval::advance(std::chrono::system_clock::time_point tp) const
{
    auto t = std::chrono::sys_seconds(std::chrono::floor<std::chrono::seconds>(tp));
    if (months)
    {
        auto m = toAbsoluteMonth(t);
        auto s = t - startOfAbsoluteMonth(m);
        t = startOfAbsoluteMonth(m + Int64(months)) + s;
    }
    return t + std::chrono::seconds(Int64(seconds));
}

std::chrono::sys_seconds CalendarTimeInterval::floor(std::chrono::system_clock::time_point tp) const
{
    assertSingleUnit();
    assertPositive();

    if (months)
        return startOfAbsoluteMonth(toAbsoluteMonth(tp) / months * months);

    constexpr std::chrono::seconds epoch(-3600 * 24 * 3);
    auto t = std::chrono::sys_seconds(std::chrono::floor<std::chrono::seconds>(tp));
    /// We want to align with weeks, but 1970-01-01 is a Thursday, so align with 1969-12-29 instead.
    return std::chrono::sys_seconds((t.time_since_epoch() - epoch) / seconds * seconds + epoch);
}

bool CalendarTimeInterval::operator==(const CalendarTimeInterval & rhs) const
{
    return std::tie(months, seconds) == std::tie(rhs.months, rhs.seconds);
}

bool CalendarTimeInterval::operator!=(const CalendarTimeInterval & rhs) const
{
    return !(*this == rhs);
}

}
