#pragma once

#include <Poco/Timespan.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <QueryPipeline/SizeLimits.h>

class Stopwatch;

namespace ProfileEvents
{
    extern const Event OverflowBreak;
    extern const Event OverflowThrow;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Limits for query execution speed.
class ExecutionSpeedLimits
{
public:
    /// For rows per second.
    size_t min_execution_rps = 0;
    size_t max_execution_rps = 0;
    /// For bytes per second.
    size_t min_execution_bps = 0;
    size_t max_execution_bps = 0;

    Poco::Timespan max_execution_time = 0;
    Poco::Timespan max_estimated_execution_time = 0;
    /// Verify that the speed is not too low after the specified time has elapsed.
    Poco::Timespan timeout_before_checking_execution_speed = 0;

    /// Pause execution in case if speed limits were exceeded.
    void throttle(size_t read_rows, size_t read_bytes, size_t total_rows_to_read, UInt64 total_elapsed_microseconds,
        OverflowMode timeout_overflow_mode) const;

    template <typename... Args>
    static bool handleOverflowMode(OverflowMode mode, int code, FormatStringHelper<Args...> fmt, Args &&... args)
    {
        switch (mode)
        {
            case OverflowMode::THROW:
                ProfileEvents::increment(ProfileEvents::OverflowThrow);
                throw Exception(code, std::move(fmt), std::forward<Args>(args)...);
            case OverflowMode::BREAK:
                ProfileEvents::increment(ProfileEvents::OverflowBreak);
                return false;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown overflow mode");
        }
    }
};

}

