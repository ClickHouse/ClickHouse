#include <Processors/Sources/SourceWithProgress.h>

#include <Interpreters/ProcessList.h>
#include <Interpreters/Quota.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
}

void SourceWithProgress::progress(const Progress & value)
{
    if (total_rows_approx != 0)
    {
        Progress total_rows_progress = {0, 0, total_rows_approx};

        if (progress_callback)
            progress_callback(total_rows_progress);

        if (process_list_elem)
            process_list_elem->updateProgressIn(total_rows_progress);

        total_rows_approx = 0;
    }

    if (progress_callback)
        progress_callback(value);

    if (process_list_elem)
    {
        if (!process_list_elem->updateProgressIn(value))
            cancel();

        /// The total amount of data processed or intended for processing in all leaf sources, possibly on remote servers.

        ProgressValues progress = process_list_elem->getProgressIn();
        size_t total_rows_estimate = std::max(progress.read_rows, progress.total_rows_to_read);

        /// Check the restrictions on the amount of data to read, the speed of the query, the quota on the amount of data to read.
        /// NOTE: Maybe it makes sense to have them checked directly in ProcessList?
        if (limits.mode == LimitsMode::LIMITS_TOTAL)
        {
            if (!limits.size_limits.check(total_rows_estimate, progress.read_bytes, "rows to read",
                                          ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES))
                cancel();
        }

        size_t total_rows = progress.total_rows_to_read;

        constexpr UInt64 profile_events_update_period_microseconds = 10 * 1000; // 10 milliseconds
        UInt64 total_elapsed_microseconds = total_stopwatch.elapsedMicroseconds();

        if (last_profile_events_update_time + profile_events_update_period_microseconds < total_elapsed_microseconds)
        {
            /// Should be done in PipelineExecutor.
            /// It is here for compatibility with IBlockInputsStream.
            CurrentThread::updatePerformanceCounters();
            last_profile_events_update_time = total_elapsed_microseconds;
        }

        /// Should be done in PipelineExecutor.
        /// It is here for compatibility with IBlockInputsStream.
        limits.speed_limit.throttle(progress.read_rows, progress.read_bytes, total_rows, total_elapsed_microseconds);

        if (quota != nullptr && limits.mode == LimitsMode::LIMITS_TOTAL)
        {
            quota->checkAndAddReadRowsBytes(time(nullptr), value.read_rows, value.read_bytes);
        }
    }
}

}
