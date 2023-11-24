#pragma once

#include <Common/IntervalKind.h>

#include <chrono>

namespace DB
{

class ASTTimeInterval;
class ASTTimePeriod;
class ASTRefreshStrategy;

/// Schedule timer for MATERIALIZED VIEW ... REFRESH AFTER ... queries
class RefreshAfterTimer
{
public:
    explicit RefreshAfterTimer(const ASTTimeInterval * time_interval);

    std::chrono::sys_seconds after(std::chrono::system_clock::time_point tp) const;

    std::chrono::seconds getSeconds() const { return seconds; }
    std::chrono::minutes getMinutes() const { return minutes; }
    std::chrono::hours getHours() const { return hours; }
    std::chrono::days getDays() const { return days; }
    std::chrono::weeks getWeeks() const { return weeks; }
    std::chrono::months getMonths() const { return months; }
    std::chrono::years getYears() const { return years; }

    bool operator==(const RefreshAfterTimer & rhs) const;

private:
    void setWithKind(IntervalKind kind, UInt64 val);

    std::chrono::seconds seconds{0};
    std::chrono::minutes minutes{0};
    std::chrono::hours hours{0};
    std::chrono::days days{0};
    std::chrono::weeks weeks{0};
    std::chrono::months months{0};
    std::chrono::years years{0};
};

/// Schedule timer for MATERIALIZED VIEW ... REFRESH EVERY ... queries
class RefreshEveryTimer
{
public:
    explicit RefreshEveryTimer(const ASTTimePeriod & time_period, const ASTTimeInterval * time_offset);

    std::chrono::sys_seconds next(std::chrono::system_clock::time_point tp) const;

    bool operator==(const RefreshEveryTimer & rhs) const;

private:
    std::chrono::sys_seconds alignedToYears(std::chrono::system_clock::time_point tp) const;

    std::chrono::sys_seconds alignedToMonths(std::chrono::system_clock::time_point tp) const;

    std::chrono::sys_seconds alignedToWeeks(std::chrono::system_clock::time_point tp) const;

    std::chrono::sys_seconds alignedToDays(std::chrono::system_clock::time_point tp) const;

    std::chrono::sys_seconds alignedToHours(std::chrono::system_clock::time_point tp) const;

    std::chrono::sys_seconds alignedToMinutes(std::chrono::system_clock::time_point tp) const;

    std::chrono::sys_seconds alignedToSeconds(std::chrono::system_clock::time_point tp) const;

    RefreshAfterTimer offset;
    UInt32 value{0};
    IntervalKind kind{IntervalKind::Second};
};

struct RefreshTimer
{
    std::variant<RefreshEveryTimer, RefreshAfterTimer> timer;

    explicit RefreshTimer(const ASTRefreshStrategy & strategy);

    std::chrono::sys_seconds next(std::chrono::system_clock::time_point tp) const;

    bool operator==(const RefreshTimer & rhs) const;

    const RefreshAfterTimer * tryGetAfter() const;
    const RefreshEveryTimer * tryGetEvery() const;
};

}
