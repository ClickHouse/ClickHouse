#include <Processors/Sources/SourceWithProgress.h>

#include <Interpreters/ProcessList.h>
#include <Access/EnabledQuota.h>

namespace ProfileEvents
{
    extern const Event SelectedRows;
    extern const Event SelectedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
}

SourceWithProgress::SourceWithProgress(Block header, bool enable_auto_progress)
    : ISourceWithProgress(header), auto_progress(enable_auto_progress)
{
}

void SourceWithProgress::setProcessListElement(QueryStatus * elem)
{
    process_list_elem = elem;

    /// Update total_rows_approx as soon as possible.
    ///
    /// It is important to do this, since you will not get correct
    /// total_rows_approx until the query will start reading all parts (in case
    /// of query needs to read from multiple parts), and this is especially a
    /// problem in case of max_threads=1.
    ///
    /// NOTE: This can be done only if progress callback already set, since
    /// otherwise total_rows_approx will lost.
    if (total_rows_approx != 0 && progress_callback)
    {
        Progress total_rows_progress = {0, 0, total_rows_approx};

        progress_callback(total_rows_progress);
        process_list_elem->updateProgressIn(total_rows_progress);

        total_rows_approx = 0;
    }
}

void SourceWithProgress::work()
{
    if (!limits.speed_limits.checkTimeLimit(total_stopwatch.elapsed(), limits.timeout_overflow_mode))
    {
        cancel();
    }
    else
    {
        was_progress_called = false;

        ISourceWithProgress::work();

        if (auto_progress && !was_progress_called && has_input)
            progress({ current_chunk.chunk.getNumRows(), current_chunk.chunk.bytes() });
    }
}

/// Aggregated copy-paste from IBlockInputStream::progressImpl.
/// Most of this must be done in PipelineExecutor outside. Now it's done for compatibility with IBlockInputStream.
void SourceWithProgress::progress(const Progress & value)
{
    was_progress_called = true;

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

        /// The total amount of data processed or intended for processing in all sources, possibly on remote servers.

        ProgressValues progress = process_list_elem->getProgressIn();

        /// If the mode is "throw" and estimate of total rows is known, then throw early if an estimate is too high.
        /// If the mode is "break", then allow to read before limit even if estimate is very high.

        size_t rows_to_check_limit = progress.read_rows;
        if (limits.size_limits.overflow_mode == OverflowMode::THROW && progress.total_rows_to_read > progress.read_rows)
            rows_to_check_limit = progress.total_rows_to_read;

        /// Check the restrictions on the
        ///  * amount of data to read
        ///  * speed of the query
        ///  * quota on the amount of data to read
        /// NOTE: Maybe it makes sense to have them checked directly in ProcessList?

        if (limits.mode == LimitsMode::LIMITS_TOTAL)
        {
            if (!limits.size_limits.check(rows_to_check_limit, progress.read_bytes, "rows or bytes to read",
                                          ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES))
            {
                cancel();
            }
        }

        if (!leaf_limits.check(rows_to_check_limit, progress.read_bytes, "rows or bytes to read on leaf node",
                                          ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES))
        {
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
        limits.speed_limits.throttle(progress.read_rows, progress.read_bytes, total_rows, total_elapsed_microseconds);

        if (quota && limits.mode == LimitsMode::LIMITS_TOTAL)
            quota->used({Quota::READ_ROWS, value.read_rows}, {Quota::READ_BYTES, value.read_bytes});
    }

    ProfileEvents::increment(ProfileEvents::SelectedRows, value.read_rows);
    ProfileEvents::increment(ProfileEvents::SelectedBytes, value.read_bytes);
}

}
