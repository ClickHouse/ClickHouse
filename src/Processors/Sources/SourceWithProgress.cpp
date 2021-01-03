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
            progress(ReadProgress(current_chunk.chunk.getNumRows(), current_chunk.chunk.bytes()));
    }
}

/// Aggregated copy-paste from IBlockInputStream::progressImpl.
/// Most of this must be done in PipelineExecutor outside. Now it's done for compatibility with IBlockInputStream.
void SourceWithProgress::progress(const Progress & value)
{
    was_progress_called = true;

    if (total_rows_approx != 0)
    {
        Progress total_rows_progress(ReadProgress(0, 0, total_rows_approx));

        if (progress_callback)
            progress_callback(total_rows_progress);

        if (process_list_elem)
            process_list_elem->updateProgressIn(total_rows_progress);

        total_rows_approx = 0;
    }

    /// Will compute this and update last_total_elapsed_time only if needed.
    UInt64 current_chunk_elapsed_time = 0;

    if (progress_callback || process_list_elem)
    {
        const auto total_elapsed_time = total_stopwatch.elapsedNanoseconds();
        current_chunk_elapsed_time = total_elapsed_time - last_total_elapsed_time;
        last_total_elapsed_time = total_elapsed_time;
    }

    if (progress_callback)
    {
        value.setElapsedTimeIfNull(current_chunk_elapsed_time);
        progress_callback(value);
    }

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

        constexpr UInt64 profile_events_update_period = 10 * 1'000'000; // 10 milliseconds worth nanoseconds
        if (last_profile_events_update_time + profile_events_update_period < last_total_elapsed_time)
        {
            /// Should be done in PipelineExecutor.
            /// It is here for compatibility with IBlockInputsStream.
            CurrentThread::updatePerformanceCounters();
            last_profile_events_update_time = last_total_elapsed_time;
        }

        /// Should be done in PipelineExecutor.
        /// It is here for compatibility with IBlockInputsStream.
        limits.speed_limits.throttle(progress.read_rows, progress.read_bytes, progress.total_rows_to_read,
                                     last_total_elapsed_time / 1000 /* nanoseconds to microseconds */);

        if (quota && limits.mode == LimitsMode::LIMITS_TOTAL)
            quota->used({Quota::READ_ROWS, value.read_rows}, {Quota::READ_BYTES, value.read_bytes});
    }

    ProfileEvents::increment(ProfileEvents::SelectedRows, value.read_rows);
    ProfileEvents::increment(ProfileEvents::SelectedBytes, value.read_bytes);
}

}
