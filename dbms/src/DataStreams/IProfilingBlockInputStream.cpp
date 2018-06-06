#include <Interpreters/Quota.h>
#include <Interpreters/ProcessList.h>
#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TOO_SLOW;
    extern const int LOGICAL_ERROR;
    extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
}


IProfilingBlockInputStream::IProfilingBlockInputStream()
{
    info.parent = this;
}

Block IProfilingBlockInputStream::read()
{
    if (total_rows_approx)
    {
        progressImpl(Progress(0, 0, total_rows_approx));
        total_rows_approx = 0;
    }

    if (!info.started)
    {
        info.total_stopwatch.start();
        info.started = true;
    }

    Block res;

    if (isCancelledOrThrowIfKilled())
        return res;

    if (!checkTimeLimit())
        limit_exceeded_need_break = true;

    if (!limit_exceeded_need_break)
        res = readImpl();

    if (res)
    {
        info.update(res);

        if (enabled_extremes)
            updateExtremes(res);

        if (limits.mode == LIMITS_CURRENT && !limits.size_limits.check(info.rows, info.bytes, "result", ErrorCodes::TOO_MANY_ROWS_OR_BYTES))
            limit_exceeded_need_break = true;

        if (quota != nullptr)
            checkQuota(res);
    }
    else
    {
        /** If the thread is over, then we will ask all children to abort the execution.
          * This makes sense when running a query with LIMIT
          * - there is a situation when all the necessary data has already been read,
          *   but children sources are still working,
          *   herewith they can work in separate threads or even remotely.
          */
        cancel(false);
    }

    progress(Progress(res.rows(), res.bytes()));

#ifndef NDEBUG
    if (res)
    {
        Block header = getHeader();
        if (header)
            assertBlocksHaveEqualStructure(res, header, getName());
    }
#endif

    return res;
}


void IProfilingBlockInputStream::readPrefix()
{
    readPrefixImpl();

    forEachChild([&] (IBlockInputStream & child)
    {
        child.readPrefix();
        return false;
    });
}


void IProfilingBlockInputStream::readSuffix()
{
    forEachChild([&] (IBlockInputStream & child)
    {
        child.readSuffix();
        return false;
    });

    readSuffixImpl();
}


void IProfilingBlockInputStream::updateExtremes(Block & block)
{
    size_t num_columns = block.columns();

    if (!extremes)
    {
        MutableColumns extremes_columns(num_columns);

        for (size_t i = 0; i < num_columns; ++i)
        {
            const ColumnPtr & src = block.safeGetByPosition(i).column;

            if (src->isColumnConst())
            {
                /// Equal min and max.
                extremes_columns[i] = src->cloneResized(2);
            }
            else
            {
                Field min_value;
                Field max_value;

                src->getExtremes(min_value, max_value);

                extremes_columns[i] = src->cloneEmpty();

                extremes_columns[i]->insert(min_value);
                extremes_columns[i]->insert(max_value);
            }
        }

        extremes = block.cloneWithColumns(std::move(extremes_columns));
    }
    else
    {
        for (size_t i = 0; i < num_columns; ++i)
        {
            ColumnPtr & old_extremes = extremes.safeGetByPosition(i).column;

            if (old_extremes->isColumnConst())
                continue;

            Field min_value = (*old_extremes)[0];
            Field max_value = (*old_extremes)[1];

            Field cur_min_value;
            Field cur_max_value;

            block.safeGetByPosition(i).column->getExtremes(cur_min_value, cur_max_value);

            if (cur_min_value < min_value)
                min_value = cur_min_value;
            if (cur_max_value > max_value)
                max_value = cur_max_value;

            MutableColumnPtr new_extremes = old_extremes->cloneEmpty();

            new_extremes->insert(min_value);
            new_extremes->insert(max_value);

            old_extremes = std::move(new_extremes);
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
};


bool IProfilingBlockInputStream::checkTimeLimit()
{
    if (limits.max_execution_time != 0
        && info.total_stopwatch.elapsed() > static_cast<UInt64>(limits.max_execution_time.totalMicroseconds()) * 1000)
        return handleOverflowMode(limits.timeout_overflow_mode,
            "Timeout exceeded: elapsed " + toString(info.total_stopwatch.elapsedSeconds())
                + " seconds, maximum: " + toString(limits.max_execution_time.totalMicroseconds() / 1000000.0),
            ErrorCodes::TIMEOUT_EXCEEDED);

    return true;
}


void IProfilingBlockInputStream::checkQuota(Block & block)
{
    switch (limits.mode)
    {
        case LIMITS_TOTAL:
            /// Checked in `progress` method.
            break;

        case LIMITS_CURRENT:
        {
            time_t current_time = time(nullptr);
            double total_elapsed = info.total_stopwatch.elapsedSeconds();

            quota->checkAndAddResultRowsBytes(current_time, block.rows(), block.bytes());
            quota->checkAndAddExecutionTime(current_time, Poco::Timespan((total_elapsed - prev_elapsed) * 1000000.0));

            prev_elapsed = total_elapsed;
            break;
        }

        default:
            throw Exception("Logical error: unknown limits mode.", ErrorCodes::LOGICAL_ERROR);
    }
}


void IProfilingBlockInputStream::progressImpl(const Progress & value)
{
    if (progress_callback)
        progress_callback(value);

    if (process_list_elem)
    {
        if (!process_list_elem->updateProgressIn(value))
            cancel(/* kill */ true);

        /// The total amount of data processed or intended for processing in all leaf sources, possibly on remote servers.

        ProgressValues progress = process_list_elem->getProgressIn();
        size_t total_rows_estimate = std::max(progress.rows, progress.total_rows);

        /** Check the restrictions on the amount of data to read, the speed of the query, the quota on the amount of data to read.
            * NOTE: Maybe it makes sense to have them checked directly in ProcessList?
            */

        if (limits.mode == LIMITS_TOTAL
            && ((limits.size_limits.max_rows && total_rows_estimate > limits.size_limits.max_rows)
                || (limits.size_limits.max_bytes && progress.bytes > limits.size_limits.max_bytes)))
        {
            switch (limits.size_limits.overflow_mode)
            {
                case OverflowMode::THROW:
                {
                    if (limits.size_limits.max_rows && total_rows_estimate > limits.size_limits.max_rows)
                        throw Exception("Limit for rows to read exceeded: " + toString(total_rows_estimate)
                            + " rows read (or to read), maximum: " + toString(limits.size_limits.max_rows),
                            ErrorCodes::TOO_MANY_ROWS);
                    else
                        throw Exception("Limit for (uncompressed) bytes to read exceeded: " + toString(progress.bytes)
                            + " bytes read, maximum: " + toString(limits.size_limits.max_bytes),
                            ErrorCodes::TOO_MANY_BYTES);
                }

                case OverflowMode::BREAK:
                {
                    /// For `break`, we will stop only if so many rows were actually read, and not just supposed to be read.
                    if ((limits.size_limits.max_rows && progress.rows > limits.size_limits.max_rows)
                        || (limits.size_limits.max_bytes && progress.bytes > limits.size_limits.max_bytes))
                    {
                        cancel(false);
                    }

                    break;
                }

                default:
                    throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
            }
        }

        size_t total_rows = progress.total_rows;

        if (limits.min_execution_speed || (total_rows && limits.timeout_before_checking_execution_speed != 0))
        {
            double total_elapsed = info.total_stopwatch.elapsedSeconds();

            if (total_elapsed > limits.timeout_before_checking_execution_speed.totalMicroseconds() / 1000000.0)
            {
                if (limits.min_execution_speed && progress.rows / total_elapsed < limits.min_execution_speed)
                    throw Exception("Query is executing too slow: " + toString(progress.rows / total_elapsed)
                        + " rows/sec., minimum: " + toString(limits.min_execution_speed),
                        ErrorCodes::TOO_SLOW);

                /// If the predicted execution time is longer than `max_execution_time`.
                if (limits.max_execution_time != 0 && total_rows)
                {
                    double estimated_execution_time_seconds = total_elapsed * (static_cast<double>(total_rows) / progress.rows);

                    if (estimated_execution_time_seconds > limits.max_execution_time.totalSeconds())
                        throw Exception("Estimated query execution time (" + toString(estimated_execution_time_seconds) + " seconds)"
                            + " is too long. Maximum: " + toString(limits.max_execution_time.totalSeconds())
                            + ". Estimated rows to process: " + toString(total_rows),
                            ErrorCodes::TOO_SLOW);
                }
            }
        }

        if (quota != nullptr && limits.mode == LIMITS_TOTAL)
        {
            quota->checkAndAddReadRowsBytes(time(nullptr), value.rows, value.bytes);
        }
    }
}


void IProfilingBlockInputStream::cancel(bool kill)
{
    if (kill)
        is_killed = true;

    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    forEachProfilingChild([&] (IProfilingBlockInputStream & child)
    {
        child.cancel(kill);
        return false;
    });
}


bool IProfilingBlockInputStream::isCancelled() const
{
    return is_cancelled;
}

bool IProfilingBlockInputStream::isCancelledOrThrowIfKilled() const
{
    if (!is_cancelled)
        return false;
    if (is_killed)
        throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);
    return true;
}


void IProfilingBlockInputStream::setProgressCallback(const ProgressCallback & callback)
{
    progress_callback = callback;

    forEachProfilingChild([&] (IProfilingBlockInputStream & child)
    {
        child.setProgressCallback(callback);
        return false;
    });
}


void IProfilingBlockInputStream::setProcessListElement(ProcessListElement * elem)
{
    process_list_elem = elem;

    forEachProfilingChild([&] (IProfilingBlockInputStream & child)
    {
        child.setProcessListElement(elem);
        return false;
    });
}


Block IProfilingBlockInputStream::getTotals()
{
    if (totals)
        return totals;

    Block res;
    forEachProfilingChild([&] (IProfilingBlockInputStream & child)
    {
        res = child.getTotals();
        if (res)
            return true;
        return false;
    });
    return res;
}

Block IProfilingBlockInputStream::getExtremes()
{
    if (extremes)
        return extremes;

    Block res;
    forEachProfilingChild([&] (IProfilingBlockInputStream & child)
    {
        res = child.getExtremes();
        if (res)
            return true;
        return false;
    });
    return res;
}

}
