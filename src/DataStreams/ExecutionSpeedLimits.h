#pragma once

#include <Poco/Timespan.h>
#include <Core/Types.h>
#include <DataStreams/SizeLimits.h>

namespace DB
{

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
    /// Verify that the speed is not too low after the specified time has elapsed.
    Poco::Timespan timeout_before_checking_execution_speed = 0;

    /// Pause execution in case if speed limits were exceeded.
    void throttle(size_t read_rows, size_t read_bytes, size_t total_rows_to_read, UInt64 total_elapsed_microseconds) const;

    bool checkTimeLimit(UInt64 elapsed_ns, OverflowMode overflow_mode) const;
};

}

