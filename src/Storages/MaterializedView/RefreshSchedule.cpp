#include <Storages/MaterializedView/RefreshSchedule.h>

#include <Common/thread_local_rng.h>

#include <algorithm>
#include <limits>

namespace DB
{

RefreshSchedule::RefreshSchedule(const ASTRefreshStrategy & strategy)
{
    kind = strategy.schedule_kind;
    chassert(kind != RefreshScheduleKind::UNKNOWN);
    if (strategy.period)
        period = strategy.period->interval;
    if (strategy.offset)
        offset = strategy.offset->interval;
    if (strategy.spread)
        spread = strategy.spread->interval;
}

bool RefreshSchedule::operator!=(const RefreshSchedule & rhs) const
{
    static_assert(sizeof(*this) == 7*8, "If fields were added or removed in RefreshSchedule, please update this comparator.");
    return std::tie(kind, period, offset, spread) != std::tie(rhs.kind, rhs.period, rhs.offset, rhs.spread);
}

static std::chrono::sys_seconds floorEvery(std::chrono::sys_seconds prev, CalendarTimeInterval period, CalendarTimeInterval offset)
{
    auto period_start = period.floor(prev);
    auto t = offset.advance(period_start);
    if (t <= prev)
        return t;
    period_start = period.floor(period_start - std::chrono::seconds(1));
    t = offset.advance(period_start);
    chassert(t <= prev);
    return t;
}

static std::chrono::sys_seconds advanceEvery(std::chrono::system_clock::time_point prev, CalendarTimeInterval period, CalendarTimeInterval offset)
{
    auto period_start = period.floor(prev);
    auto t = offset.advance(period_start);
    if (t > prev)
        return t;
    t = offset.advance(period.advance(period_start));
    chassert(t > prev);
    return t;
}

std::chrono::sys_seconds RefreshSchedule::timeslotForCompletedRefresh(std::chrono::sys_seconds last_completed_timeslot, std::chrono::sys_seconds start_time, std::chrono::sys_seconds end_time, bool out_of_schedule) const
{
    if (kind == RefreshScheduleKind::AFTER)
        return end_time;
    /// Timeslot based on when the refresh actually happened. Useful if we fell behind and missed
    /// some timeslots.
    auto res = floorEvery(start_time, period, offset);
    if (out_of_schedule)
        return res;
    /// Next timeslot after the last completed one. Useful in case we did a refresh a little early
    /// because of random spread.
    res = std::max(res, advanceEvery(last_completed_timeslot, period, offset));
    return res;
}

std::chrono::sys_seconds RefreshSchedule::advance(std::chrono::sys_seconds last_completed_timeslot) const
{
    if (kind == RefreshScheduleKind::AFTER)
        return period.advance(last_completed_timeslot);
    return advanceEvery(last_completed_timeslot, period, offset);
}

std::chrono::system_clock::time_point RefreshSchedule::addRandomSpread(std::chrono::system_clock::time_point when, Int64 randomness) const
{
    /// `spread.minSeconds()` and `randomness` are both unbounded, so every intermediate must be
    /// wide enough for their product. Dividing by 2000000 before scaling back up by 1000 keeps the
    /// product inside Int128; scaling by 1000 first would not.
    const Int128 spread_ms = Int128(spread.minSeconds()) * randomness / 2000000;
    const Int128 res = Int128(when.time_since_epoch().count()) + spread_ms * 1000;

    /// Saturate strictly inside the range: `time_point::max()` means "no refresh scheduled".
    constexpr Int128 hi = Int128(std::numeric_limits<Int64>::max()) - 1;
    constexpr Int128 lo = Int128(std::numeric_limits<Int64>::min()) + 1;
    return std::chrono::system_clock::time_point(
        std::chrono::system_clock::duration(Int64(std::clamp(res, lo, hi))));
}

}
