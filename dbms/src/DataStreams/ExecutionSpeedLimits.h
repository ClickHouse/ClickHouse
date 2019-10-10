#pragma once

#include <Poco/Timespan.h>
#include <Core/Types.h>

namespace DB
{

/// Limits for query execution speed.
/// In rows per second.
class ExecutionSpeedLimits
{
public:
    size_t min_execution_speed = 0;
    size_t max_execution_speed = 0;
    size_t min_execution_speed_bytes = 0;
    size_t max_execution_speed_bytes = 0;

    Poco::Timespan max_execution_time = 0;
    /// Verify that the speed is not too low after the specified time has elapsed.
    Poco::Timespan timeout_before_checking_execution_speed = 0;

    /// Pause execution in case if speed limits were exceeded.
    void throttle(size_t read_rows, size_t read_bytes, size_t total_rows, UInt64 total_elapsed_microseconds);
};

}

