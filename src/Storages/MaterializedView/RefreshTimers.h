#pragma once

#include <Common/IntervalKind.h>

#include <chrono>

namespace DB
{

class ASTTimeInterval;
class ASTTimePeriod;

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

}
