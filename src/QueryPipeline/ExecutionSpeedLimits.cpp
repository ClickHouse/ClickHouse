#include <QueryPipeline/ExecutionSpeedLimits.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <IO/WriteHelpers.h>
#include <Common/Stopwatch.h>
#include <base/sleep.h>

namespace ProfileEvents
{
    extern const Event ThrottlerSleepMicroseconds;
    extern const Event OverflowBreak;
    extern const Event OverflowThrow;
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
        sleep_microseconds = std::min(static_cast<UInt64>(1000000), sleep_microseconds);

        sleepForMicroseconds(sleep_microseconds);

        ProfileEvents::increment(ProfileEvents::ThrottlerSleepMicroseconds, sleep_microseconds);
    }
}

void ExecutionSpeedLimits::throttle(
    size_t read_rows, size_t read_bytes,
    size_t total_rows_to_read, UInt64 total_elapsed_microseconds,
    OverflowMode timeout_overflow_mode) const
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
                throw Exception(
                    ErrorCodes::TOO_SLOW,
                    "Query is executing too slow: {} rows/sec., minimum: {}",
                    read_rows / elapsed_seconds,
                    min_execution_rps);

            auto bytes_per_second = read_bytes / elapsed_seconds;
            if (min_execution_bps && bytes_per_second < min_execution_bps)
                throw Exception(
                    ErrorCodes::TOO_SLOW,
                    "Query is executing too slow: {} bytes/sec., minimum: {}",
                    read_bytes / elapsed_seconds,
                    min_execution_bps);

            /// If the predicted execution time is longer than `max_estimated_execution_time`.
            if (max_estimated_execution_time != 0 && total_rows_to_read && read_rows)
            {
                double estimated_execution_time_seconds = elapsed_seconds * (static_cast<double>(total_rows_to_read) / read_rows);

                if (timeout_overflow_mode == OverflowMode::THROW && estimated_execution_time_seconds > max_estimated_execution_time.totalSeconds())
                    throw Exception(
                        ErrorCodes::TOO_SLOW,
                        "Estimated query execution time ({} seconds) is too long. Maximum: {}. Estimated rows to process: {}",
                        estimated_execution_time_seconds,
                        max_estimated_execution_time.totalSeconds(),
                        total_rows_to_read);
            }

            if (max_execution_rps && rows_per_second >= max_execution_rps)
                limitProgressingSpeed(read_rows, max_execution_rps, total_elapsed_microseconds);

            if (max_execution_bps && bytes_per_second >= max_execution_bps)
                limitProgressingSpeed(read_bytes, max_execution_bps, total_elapsed_microseconds);
        }
    }
}

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

bool ExecutionSpeedLimits::checkTimeLimit(const Stopwatch & stopwatch, OverflowMode overflow_mode) const
{
    if (max_execution_time != 0)
    {
        auto elapsed_ns = stopwatch.elapsed();

        if (elapsed_ns > static_cast<UInt64>(max_execution_time.totalMicroseconds()) * 1000)
            return handleOverflowMode(
                overflow_mode,
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Timeout exceeded: elapsed {} seconds, maximum: {}",
                static_cast<double>(elapsed_ns) / 1000000000ULL,
                max_execution_time.totalMicroseconds() / 1000000.0);
    }

    return true;
}

}
