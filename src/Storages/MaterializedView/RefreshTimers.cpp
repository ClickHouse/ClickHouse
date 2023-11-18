#include <Storages/MaterializedView/RefreshTimers.h>

#include <Parsers/ASTTimeInterval.h>

namespace DB
{

namespace
{
    constexpr std::chrono::days ZERO_DAYS{0};
    constexpr std::chrono::days ONE_DAY{1};
}

RefreshAfterTimer::RefreshAfterTimer(const ASTTimeInterval * time_interval)
{
    if (time_interval)
    {
        for (auto && [kind, value] : time_interval->kinds)
            setWithKind(kind, value);
    }
}

std::chrono::sys_seconds RefreshAfterTimer::after(std::chrono::system_clock::time_point tp) const
{
    auto tp_date = std::chrono::floor<std::chrono::days>(tp);
    auto tp_time_offset = std::chrono::floor<std::chrono::seconds>(tp - tp_date);
    std::chrono::year_month_day ymd(tp_date);
    ymd += years;
    ymd += months;
    std::chrono::sys_days date = ymd;
    date += weeks;
    date += days;
    auto result = std::chrono::time_point_cast<std::chrono::seconds>(date);
    result += tp_time_offset;
    result += hours;
    result += minutes;
    result += seconds;
    return result;
}

void RefreshAfterTimer::setWithKind(IntervalKind kind, UInt64 val)
{
    switch (kind)
    {
        case IntervalKind::Second:
            seconds = std::chrono::seconds{val};
            break;
        case IntervalKind::Minute:
            minutes = std::chrono::minutes{val};
            break;
        case IntervalKind::Hour:
            hours = std::chrono::hours{val};
            break;
        case IntervalKind::Day:
            days = std::chrono::days{val};
            break;
        case IntervalKind::Week:
            weeks = std::chrono::weeks{val};
            break;
        case IntervalKind::Month:
            months = std::chrono::months{val};
            break;
        case IntervalKind::Year:
            years = std::chrono::years{val};
            break;
        default:
            break;
    }
}

RefreshEveryTimer::RefreshEveryTimer(const ASTTimePeriod & time_period, const ASTTimeInterval * time_offset)
    : offset(time_offset)
    , value{static_cast<UInt32>(time_period.value)}
    , kind{time_period.kind}
{
    // TODO: validate invariants
}

std::chrono::sys_seconds RefreshEveryTimer::next(std::chrono::system_clock::time_point tp) const
{
    if (value == 0)
        return std::chrono::floor<std::chrono::seconds>(tp);
    switch (kind)
    {
        case IntervalKind::Second:
            return alignedToSeconds(tp);
        case IntervalKind::Minute:
            return alignedToMinutes(tp);
        case IntervalKind::Hour:
            return alignedToHours(tp);
        case IntervalKind::Day:
            return alignedToDays(tp);
        case IntervalKind::Week:
            return alignedToWeeks(tp);
        case IntervalKind::Month:
            return alignedToMonths(tp);
        case IntervalKind::Year:
            return alignedToYears(tp);
        default:
            return std::chrono::ceil<std::chrono::seconds>(tp);
    }
}

std::chrono::sys_seconds RefreshEveryTimer::alignedToYears(std::chrono::system_clock::time_point tp) const
{
    using namespace std::chrono_literals;

    auto tp_days = std::chrono::floor<std::chrono::days>(tp);
    std::chrono::year_month_day tp_ymd(tp_days);
    auto normalize_years = [](std::chrono::year year) -> std::chrono::sys_days
    {
        return year / std::chrono::January / 1d;
    };

    auto prev_years = normalize_years(tp_ymd.year());
    if (auto prev_time = offset.after(prev_years); prev_time > tp)
        return prev_time;

    auto next_years = normalize_years(std::chrono::year((int(tp_ymd.year()) / value + 1) * value));
    return offset.after(next_years);
}

std::chrono::sys_seconds RefreshEveryTimer::alignedToMonths(std::chrono::system_clock::time_point tp) const
{
    using namespace std::chrono_literals;

    auto tp_days = std::chrono::floor<std::chrono::days>(tp);
    std::chrono::year_month_day tp_ymd(tp_days);
    auto normalize_months = [](const std::chrono::year_month_day & ymd, unsigned month_value) -> std::chrono::sys_days
    {
        return ymd.year() / std::chrono::month{month_value} / 1d;
    };

    auto prev_month_value = static_cast<unsigned>(tp_ymd.month()) / value * value;
    auto prev_months = normalize_months(tp_ymd, prev_month_value);
    if (auto prev_time = offset.after(prev_months); prev_time > tp)
        return prev_time;

    auto next_month_value = (static_cast<unsigned>(tp_ymd.month()) / value + 1) * value;
    auto next_months = normalize_months(tp_ymd, next_month_value);
    std::chrono::year_month_day next_ymd(next_months);
    if (next_ymd.year() > tp_ymd.year())
        return offset.after(normalize_months(next_ymd, value));
    return offset.after(next_months);
}

std::chrono::sys_seconds RefreshEveryTimer::alignedToWeeks(std::chrono::system_clock::time_point tp) const
{
    using namespace std::chrono_literals;

    auto cpp_weekday = offset.getDays() + ONE_DAY;
    std::chrono::weekday offset_weekday((cpp_weekday - std::chrono::floor<std::chrono::weeks>(cpp_weekday)).count());

    auto tp_days = std::chrono::floor<std::chrono::days>(tp);
    std::chrono::year_month_weekday tp_ymd(tp_days);
    auto normalize_weeks = [offset_weekday](const std::chrono::year_month_weekday & ymd, unsigned week_value)
    {
        return std::chrono::sys_days(ymd.year() / ymd.month() / std::chrono::weekday{offset_weekday}[week_value]);
    };

    auto prev_week_value = tp_ymd.index() / value * value;
    auto prev_days = normalize_weeks(tp_ymd, prev_week_value);
    if (auto prev_time = offset.after(prev_days - offset.getDays()); prev_time > tp)
        return prev_time;

    auto next_day_value = (tp_ymd.index() / value + 1) * value;
    auto next_days = normalize_weeks(tp_ymd, next_day_value);
    std::chrono::year_month_weekday next_ymd(next_days);
    if (next_ymd.year() > tp_ymd.year() || next_ymd.month() > tp_ymd.month())
        return offset.after(normalize_weeks(next_ymd, value) - offset.getDays());
    return offset.after(next_days);
}

std::chrono::sys_seconds RefreshEveryTimer::alignedToDays(std::chrono::system_clock::time_point tp) const
{
    auto tp_days = std::chrono::floor<std::chrono::days>(tp);
    std::chrono::year_month_day tp_ymd(tp_days);
    auto normalize_days = [](const std::chrono::year_month_day & ymd, unsigned day_value) -> std::chrono::sys_days
    {
        return ymd.year() / ymd.month() / std::chrono::day{day_value};
    };

    auto prev_day_value = static_cast<unsigned>(tp_ymd.day()) / value * value;
    auto prev_days = normalize_days(tp_ymd, prev_day_value);
    if (auto prev_time = offset.after(prev_days); prev_time > tp)
        return prev_time;

    auto next_day_value = (static_cast<unsigned>(tp_ymd.day()) / value + 1) * value;
    auto next_days = normalize_days(tp_ymd, next_day_value);
    std::chrono::year_month_day next_ymd(next_days);
    if (next_ymd.year() > tp_ymd.year() || next_ymd.month() > tp_ymd.month())
        return offset.after(normalize_days(next_ymd, value));
    return offset.after(next_days);
}

std::chrono::sys_seconds RefreshEveryTimer::alignedToHours(std::chrono::system_clock::time_point tp) const
{
    using namespace std::chrono_literals;

    auto tp_days = std::chrono::floor<std::chrono::days>(tp);
    auto tp_hours = std::chrono::floor<std::chrono::hours>(tp - tp_days);

    auto prev_hours = (tp_hours / value) * value;
    if (auto prev_time = offset.after(tp_days + prev_hours); prev_time > tp)
        return prev_time;

    auto next_hours = (tp_hours / value + 1h) * value;
    if (std::chrono::floor<std::chrono::days>(next_hours - 1h) > ZERO_DAYS)
        return offset.after(tp_days + ONE_DAY + std::chrono::hours{value});
    return offset.after(tp_days + next_hours);
}

std::chrono::sys_seconds RefreshEveryTimer::alignedToMinutes(std::chrono::system_clock::time_point tp) const
{
    using namespace std::chrono_literals;

    auto tp_hours = std::chrono::floor<std::chrono::hours>(tp);
    auto tp_minutes = std::chrono::floor<std::chrono::minutes>(tp - tp_hours);

    auto prev_minutes = (tp_minutes / value) * value;
    if (auto prev_time = offset.after(tp_hours + prev_minutes); prev_time > tp)
        return prev_time;

    auto next_minutes = (tp_minutes / value + 1min) * value;
    if (std::chrono::floor<std::chrono::hours>(next_minutes - 1min) > 0h)
        return offset.after(tp_hours + 1h + std::chrono::minutes{value});
    return offset.after(tp_hours + next_minutes);
}

std::chrono::sys_seconds RefreshEveryTimer::alignedToSeconds(std::chrono::system_clock::time_point tp) const
{
    using namespace std::chrono_literals;

    auto tp_minutes = std::chrono::floor<std::chrono::minutes>(tp);
    auto tp_seconds = std::chrono::floor<std::chrono::seconds>(tp - tp_minutes);

    auto next_seconds = (tp_seconds / value + 1s) * value;
    if (std::chrono::floor<std::chrono::minutes>(next_seconds - 1s) > 0min)
        return tp_minutes + 1min + std::chrono::seconds{value};
    return tp_minutes + next_seconds;
}

}
