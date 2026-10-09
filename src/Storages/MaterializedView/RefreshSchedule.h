#pragma once

#include <Common/CalendarTimeInterval.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <chrono>

namespace DB
{

class ASTRefreshStrategy;

struct RefreshSchedule
{
    RefreshScheduleKind kind;
    CalendarTimeInterval period;
    CalendarTimeInterval offset;
    CalendarTimeInterval spread;

    explicit RefreshSchedule(const ASTRefreshStrategy & strategy);
    bool operator!=(const RefreshSchedule & rhs) const;

    /// What to store as "last completed timeslot" value after a refresh completes.
    /// This value is used for scheduling subsequent refreshes.
    std::chrono::sys_seconds timeslotForCompletedRefresh(std::chrono::sys_seconds last_completed_timeslot, std::chrono::sys_seconds start_time, std::chrono::sys_seconds end_time, bool out_of_schedule) const;

    std::chrono::sys_seconds advance(std::chrono::sys_seconds last_completed_timeslot) const;

    std::chrono::system_clock::time_point addRandomSpread(std::chrono::sys_seconds timeslot, Int64 randomness) const;
};

}
