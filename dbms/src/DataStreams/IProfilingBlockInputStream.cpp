#include <iomanip>
#include <random>

#include <Columns/ColumnConst.h>
#include <Interpreters/Quota.h>
#include <Interpreters/ProcessList.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MUCH_ROWS;
    extern const int TOO_MUCH_BYTES;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TOO_SLOW;
}


IProfilingBlockInputStream::IProfilingBlockInputStream()
{
    info.parent = this;
}

Block IProfilingBlockInputStream::read()
{
    collectAndSendTotalRowsApprox();

    if (!info.started)
    {
        info.total_stopwatch.start();
        info.started = true;
    }

    Block res;

    if (is_cancelled.load(std::memory_order_seq_cst))
        return res;

    if (!limit_exceeded_need_break)
        res = readImpl();

    if (res)
    {
        info.update(res);

        if (enabled_extremes)
            updateExtremes(res);

        if (!checkLimits())
            limit_exceeded_need_break = true;

        if (quota != nullptr)
            checkQuota(res);
    }
    else
    {
        /** If the thread is over, then we will ask all children to abort the execution.
          * This makes sense when running a query with LIMIT
          * - there is a situation when all the necessary data has already been read,
          *   but `children sources are still working,
          *   herewith they can work in separate threads or even remotely.
          */
        cancel();
    }

    progress(Progress(res.rows(), res.bytes()));

    return res;
}


void IProfilingBlockInputStream::readPrefix()
{
    readPrefixImpl();

    for (auto & child : children)
        child->readPrefix();
}


void IProfilingBlockInputStream::readSuffix()
{
    for (auto & child : children)
        child->readSuffix();

    readSuffixImpl();
}


void IProfilingBlockInputStream::updateExtremes(Block & block)
{
    size_t columns = block.columns();

    if (!extremes)
    {
        extremes = block.cloneEmpty();

        for (size_t i = 0; i < columns; ++i)
        {
            Field min_value;
            Field max_value;

            block.safeGetByPosition(i).column->getExtremes(min_value, max_value);

            ColumnPtr & column = extremes.safeGetByPosition(i).column;

            if (auto converted = column->convertToFullColumnIfConst())
                column = converted;

            column->insert(min_value);
            column->insert(max_value);
        }
    }
    else
    {
        for (size_t i = 0; i < columns; ++i)
        {
            ColumnPtr & column = extremes.safeGetByPosition(i).column;

            Field min_value = (*column)[0];
            Field max_value = (*column)[1];

            Field cur_min_value;
            Field cur_max_value;

            block.safeGetByPosition(i).column->getExtremes(cur_min_value, cur_max_value);

            if (cur_min_value < min_value)
                min_value = cur_min_value;
            if (cur_max_value > max_value)
                max_value = cur_max_value;

            column = column->cloneEmpty();
            column->insert(min_value);
            column->insert(max_value);
        }
    }
}


bool IProfilingBlockInputStream::checkLimits()
{
    if (limits.mode == LIMITS_CURRENT)
    {
        /// Check current stream limitations (i.e. max_result_{rows,bytes})

        if (limits.max_rows_to_read && info.rows > limits.max_rows_to_read)
        {
            if (limits.read_overflow_mode == OverflowMode::THROW)
                throw Exception(std::string("Limit for result rows")
                    + " exceeded: read " + toString(info.rows)
                    + " rows, maximum: " + toString(limits.max_rows_to_read),
                    ErrorCodes::TOO_MUCH_ROWS);

            if (limits.read_overflow_mode == OverflowMode::BREAK)
                return false;

            throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
        }

        if (limits.max_bytes_to_read && info.bytes > limits.max_bytes_to_read)
        {
            if (limits.read_overflow_mode == OverflowMode::THROW)
                throw Exception(std::string("Limit for result bytes (uncompressed)")
                    + " exceeded: read " + toString(info.bytes)
                    + " bytes, maximum: " + toString(limits.max_bytes_to_read),
                    ErrorCodes::TOO_MUCH_BYTES);

            if (limits.read_overflow_mode == OverflowMode::BREAK)
                return false;

            throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
        }
    }

    if (limits.max_execution_time != 0
        && info.total_stopwatch.elapsed() > static_cast<UInt64>(limits.max_execution_time.totalMicroseconds()) * 1000)
    {
        if (limits.timeout_overflow_mode == OverflowMode::THROW)
            throw Exception("Timeout exceeded: elapsed " + toString(info.total_stopwatch.elapsedSeconds())
                + " seconds, maximum: " + toString(limits.max_execution_time.totalMicroseconds() / 1000000.0),
            ErrorCodes::TIMEOUT_EXCEEDED);

        if (limits.timeout_overflow_mode == OverflowMode::BREAK)
            return false;

        throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
    }

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
            time_t current_time = time(0);
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
            cancel();

        /// The total amount of data processed or intended for processing in all leaf sources, possibly on remote servers.

        size_t rows_processed = process_list_elem->progress_in.rows;
        size_t bytes_processed = process_list_elem->progress_in.bytes;

        size_t total_rows_estimate = std::max(rows_processed, process_list_elem->progress_in.total_rows.load(std::memory_order_relaxed));

        /** Check the restrictions on the amount of data to read, the speed of the query, the quota on the amount of data to read.
            * NOTE: Maybe it makes sense to have them checked directly in ProcessList?
            */

        if (limits.mode == LIMITS_TOTAL
            && ((limits.max_rows_to_read && total_rows_estimate > limits.max_rows_to_read)
                || (limits.max_bytes_to_read && bytes_processed > limits.max_bytes_to_read)))
        {
            if (limits.read_overflow_mode == OverflowMode::THROW)
            {
                if (limits.max_rows_to_read && total_rows_estimate > limits.max_rows_to_read)
                    throw Exception("Limit for rows to read exceeded: " + toString(total_rows_estimate)
                        + " rows read (or to read), maximum: " + toString(limits.max_rows_to_read),
                        ErrorCodes::TOO_MUCH_ROWS);
                else
                    throw Exception("Limit for (uncompressed) bytes to read exceeded: " + toString(bytes_processed)
                        + " bytes read, maximum: " + toString(limits.max_bytes_to_read),
                        ErrorCodes::TOO_MUCH_BYTES);
            }
            else if (limits.read_overflow_mode == OverflowMode::BREAK)
            {
                /// For `break`, we will stop only if so many lines were actually read, and not just supposed to be read.
                if ((limits.max_rows_to_read && rows_processed > limits.max_rows_to_read)
                    || (limits.max_bytes_to_read && bytes_processed > limits.max_bytes_to_read))
                {
                    cancel();
                }
            }
            else
                throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
        }

        size_t total_rows = process_list_elem->progress_in.total_rows;

        if (limits.min_execution_speed || (total_rows && limits.timeout_before_checking_execution_speed != 0))
        {
            double total_elapsed = info.total_stopwatch.elapsedSeconds();

            if (total_elapsed > limits.timeout_before_checking_execution_speed.totalMicroseconds() / 1000000.0)
            {
                if (limits.min_execution_speed && rows_processed / total_elapsed < limits.min_execution_speed)
                    throw Exception("Query is executing too slow: " + toString(rows_processed / total_elapsed)
                        + " rows/sec., minimum: " + toString(limits.min_execution_speed),
                        ErrorCodes::TOO_SLOW);

                size_t total_rows = process_list_elem->progress_in.total_rows;

                /// If the predicted execution time is longer than `max_execution_time`.
                if (limits.max_execution_time != 0 && total_rows)
                {
                    double estimated_execution_time_seconds = total_elapsed * (static_cast<double>(total_rows) / rows_processed);

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
            quota->checkAndAddReadRowsBytes(time(0), value.rows, value.bytes);
        }
    }
}


void IProfilingBlockInputStream::cancel()
{
    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    for (auto & child : children)
        if (IProfilingBlockInputStream * p_child = dynamic_cast<IProfilingBlockInputStream *>(&*child))
            p_child->cancel();
}


void IProfilingBlockInputStream::setProgressCallback(ProgressCallback callback)
{
    progress_callback = callback;

    for (auto & child : children)
        if (IProfilingBlockInputStream * p_child = dynamic_cast<IProfilingBlockInputStream *>(&*child))
            p_child->setProgressCallback(callback);
}


void IProfilingBlockInputStream::setProcessListElement(ProcessListElement * elem)
{
    process_list_elem = elem;

    for (auto & child : children)
        if (IProfilingBlockInputStream * p_child = dynamic_cast<IProfilingBlockInputStream *>(&*child))
            p_child->setProcessListElement(elem);
}


const Block & IProfilingBlockInputStream::getTotals()
{
    if (totals)
        return totals;

    for (auto & child : children)
    {
        if (IProfilingBlockInputStream * p_child = dynamic_cast<IProfilingBlockInputStream *>(&*child))
        {
            const Block & res = p_child->getTotals();
            if (res)
                return res;
        }
    }

    return totals;
}

const Block & IProfilingBlockInputStream::getExtremes() const
{
    if (extremes)
        return extremes;

    for (const auto & child : children)
    {
        if (const IProfilingBlockInputStream * p_child = dynamic_cast<const IProfilingBlockInputStream *>(&*child))
        {
            const Block & res = p_child->getExtremes();
            if (res)
                return res;
        }
    }

    return extremes;
}

void IProfilingBlockInputStream::collectTotalRowsApprox()
{
    if (collected_total_rows_approx)
        return;

    collected_total_rows_approx = true;

    for (auto & child : children)
    {
        if (IProfilingBlockInputStream * p_child = dynamic_cast<IProfilingBlockInputStream *>(&*child))
        {
            p_child->collectTotalRowsApprox();
            total_rows_approx += p_child->total_rows_approx;
        }
    }
}

void IProfilingBlockInputStream::collectAndSendTotalRowsApprox()
{
    if (collected_total_rows_approx)
        return;

    collectTotalRowsApprox();
    progressImpl(Progress(0, 0, total_rows_approx));
}


}
