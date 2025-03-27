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

    /// Tells when to do the next refresh (without random spread).
    std::chrono::sys_seconds prescribeNext(
        std::chrono::system_clock::time_point last_prescribed, std::chrono::system_clock::time_point now) const;

    std::chrono::system_clock::time_point addRandomSpread(std::chrono::sys_seconds prescribed_time) const;
};

}
