#include <QueryPipeline/ExecutionSpeedLimits.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <IO/WriteHelpers.h>
#include <Common/Stopwatch.h>
#include <base/sleep.h>

namespace ProfileEvents
{
    extern const Event ThrottlerSleepMicroseconds;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_SLOW;
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

static void limitProgressingSpeed(size_t total_progress_size, size_t max_speed_in_seconds, UInt64 total_elapsed_microseconds)
{
    /// How much time to wait for the average speed to become `max_speed_in_seconds`.
    UInt64 desired_microseconds = total_progress_size * 1000000 / max_speed_in_seconds;

    if (desired_microseconds > total_elapsed_microseconds)
    {
        UInt64 sleep_microseconds = desired_microseconds - total_elapsed_microseconds;

        /// Never sleep more than one second (it should be enough to limit speed for a reasonable amount,
        /// and otherwise it's too easy to make query hang).
        sleep_microseconds = std::min(UInt64(1000000), sleep_microseconds);

        sleepForMicroseconds(sleep_microseconds);

        ProfileEvents::increment(ProfileEvents::ThrottlerSleepMicroseconds, sleep_microseconds);
    }
}

void ExecutionSpeedLimits::throttle(
    size_t read_rows, size_t read_bytes,
    size_t total_rows_to_read, UInt64 total_elapsed_microseconds) const
{
    if ((min_execution_rps != 0 || max_execution_rps != 0
         || min_execution_bps != 0 || max_execution_bps != 0
         || (total_rows_to_read != 0 && timeout_before_checking_execution_speed != 0))
        && (static_cast<Int64>(total_elapsed_microseconds) > timeout_before_checking_execution_speed.totalMicroseconds()))
    {
        /// Do not count sleeps in throttlers
        UInt64 throttler_sleep_microseconds = CurrentThread::getProfileEvents()[ProfileEvents::ThrottlerSleepMicroseconds];

        double elapsed_seconds = 0;
        if (total_elapsed_microseconds > throttler_sleep_microseconds)
            elapsed_seconds = static_cast<double>(total_elapsed_microseconds - throttler_sleep_microseconds) / 1000000.0;

        if (elapsed_seconds > 0)
        {
            auto rows_per_second = read_rows / elapsed_seconds;
            if (min_execution_rps && rows_per_second < min_execution_rps)
                throw Exception("Query is executing too slow: " + toString(read_rows / elapsed_seconds)
                                + " rows/sec., minimum: " + toString(min_execution_rps),
                                ErrorCodes::TOO_SLOW);

            auto bytes_per_second = read_bytes / elapsed_seconds;
            if (min_execution_bps && bytes_per_second < min_execution_bps)
                throw Exception("Query is executing too slow: " + toString(read_bytes / elapsed_seconds)
                                + " bytes/sec., minimum: " + toString(min_execution_bps),
                                ErrorCodes::TOO_SLOW);

            /// If the predicted execution time is longer than `max_execution_time`.
            if (max_execution_time != 0 && total_rows_to_read && read_rows)
            {
                double estimated_execution_time_seconds = elapsed_seconds * (static_cast<double>(total_rows_to_read) / read_rows);

                if (estimated_execution_time_seconds > max_execution_time.totalSeconds())
                    throw Exception("Estimated query execution time (" + toString(estimated_execution_time_seconds) + " seconds)"
                                    + " is too long. Maximum: " + toString(max_execution_time.totalSeconds())
                                    + ". Estimated rows to process: " + toString(total_rows_to_read),
                                    ErrorCodes::TOO_SLOW);
            }

            if (max_execution_rps && rows_per_second >= max_execution_rps)
                limitProgressingSpeed(read_rows, max_execution_rps, total_elapsed_microseconds);

            if (max_execution_bps && bytes_per_second >= max_execution_bps)
                limitProgressingSpeed(read_bytes, max_execution_bps, total_elapsed_microseconds);
        }
    }
}

static bool handleOverflowMode(OverflowMode mode, const String & message, int code)
{
    switch (mode)
    {
        case OverflowMode::THROW:
            throw Exception(message, code);
        case OverflowMode::BREAK:
            return false;
        default:
            throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
    }
}

bool ExecutionSpeedLimits::checkTimeLimit(const Stopwatch & stopwatch, OverflowMode overflow_mode) const
{
    if (max_execution_time != 0)
    {
        auto elapsed_ns = stopwatch.elapsed();

        if (elapsed_ns > static_cast<UInt64>(max_execution_time.totalMicroseconds()) * 1000)
            return handleOverflowMode(overflow_mode,
                                  "Timeout exceeded: elapsed " + toString(static_cast<double>(elapsed_ns) / 1000000000ULL)
                                  + " seconds, maximum: " + toString(max_execution_time.totalMicroseconds() / 1000000.0),
                                  ErrorCodes::TIMEOUT_EXCEEDED);
    }

    return true;
}

}
