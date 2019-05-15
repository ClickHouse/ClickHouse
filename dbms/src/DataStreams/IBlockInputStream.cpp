#include <DataStreams/IBlockInputStream.h>

#include <Interpreters/ProcessList.h>
#include <Interpreters/Quota.h>
#include <Common/CurrentThread.h>


namespace ProfileEvents
{
    extern const Event ThrottlerSleepMicroseconds;
}


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
    extern const int TOO_DEEP_PIPELINE;
}


/// It's safe to access children without mutex as long as these methods are called before first call to `read()` or `readPrefix()`.


Block IBlockInputStream::read()
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


void IBlockInputStream::readPrefix()
{
#ifndef NDEBUG
    if (!read_prefix_is_called)
        read_prefix_is_called = true;
    else
        throw Exception("readPrefix is called twice for " + getName() + " stream", ErrorCodes::LOGICAL_ERROR);
#endif

    readPrefixImpl();

    forEachChild([&] (IBlockInputStream & child)
    {
        child.readPrefix();
        return false;
    });
}


void IBlockInputStream::readSuffix()
{
#ifndef NDEBUG
    if (!read_suffix_is_called)
        read_suffix_is_called = true;
    else
        throw Exception("readSuffix is called twice for " + getName() + " stream", ErrorCodes::LOGICAL_ERROR);
#endif

    forEachChild([&] (IBlockInputStream & child)
    {
        child.readSuffix();
        return false;
    });

    readSuffixImpl();
}


void IBlockInputStream::updateExtremes(Block & block)
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
}


bool IBlockInputStream::checkTimeLimit()
{
    if (limits.max_execution_time != 0
        && info.total_stopwatch.elapsed() > static_cast<UInt64>(limits.max_execution_time.totalMicroseconds()) * 1000)
        return handleOverflowMode(limits.timeout_overflow_mode,
            "Timeout exceeded: elapsed " + toString(info.total_stopwatch.elapsedSeconds())
                + " seconds, maximum: " + toString(limits.max_execution_time.totalMicroseconds() / 1000000.0),
            ErrorCodes::TIMEOUT_EXCEEDED);

    return true;
}


void IBlockInputStream::checkQuota(Block & block)
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
    }
}

static void limitProgressingSpeed(size_t total_progress_size, size_t max_speed_in_seconds, UInt64 total_elapsed_microseconds)
{
    /// How much time to wait for the average speed to become `max_speed_in_seconds`.
    UInt64 desired_microseconds = total_progress_size * 1000000 / max_speed_in_seconds;

    if (desired_microseconds > total_elapsed_microseconds)
    {
        UInt64 sleep_microseconds = desired_microseconds - total_elapsed_microseconds;
        ::timespec sleep_ts;
        sleep_ts.tv_sec = sleep_microseconds / 1000000;
        sleep_ts.tv_nsec = sleep_microseconds % 1000000 * 1000;

        /// NOTE: Returns early in case of a signal. This is considered normal.
        /// NOTE: It's worth noting that this behavior affects kill of queries.
        ::nanosleep(&sleep_ts, nullptr);

        ProfileEvents::increment(ProfileEvents::ThrottlerSleepMicroseconds, sleep_microseconds);
    }
}


void IBlockInputStream::progressImpl(const Progress & value)
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

        constexpr UInt64 profile_events_update_period_microseconds = 10 * 1000; // 10 milliseconds
        UInt64 total_elapsed_microseconds = info.total_stopwatch.elapsedMicroseconds();

        if (last_profile_events_update_time + profile_events_update_period_microseconds < total_elapsed_microseconds)
        {
            CurrentThread::updatePerformanceCounters();
            last_profile_events_update_time = total_elapsed_microseconds;
        }

        if ((limits.min_execution_speed || limits.max_execution_speed || limits.min_execution_speed_bytes ||
             limits.max_execution_speed_bytes || (total_rows && limits.timeout_before_checking_execution_speed != 0)) &&
            (static_cast<Int64>(total_elapsed_microseconds) > limits.timeout_before_checking_execution_speed.totalMicroseconds()))
        {
            /// Do not count sleeps in throttlers
            UInt64 throttler_sleep_microseconds = CurrentThread::getProfileEvents()[ProfileEvents::ThrottlerSleepMicroseconds];
            double elapsed_seconds = (throttler_sleep_microseconds > total_elapsed_microseconds)
                                     ? 0.0 : (total_elapsed_microseconds - throttler_sleep_microseconds) / 1000000.0;

            if (elapsed_seconds > 0)
            {
                if (limits.min_execution_speed && progress.rows / elapsed_seconds < limits.min_execution_speed)
                    throw Exception("Query is executing too slow: " + toString(progress.rows / elapsed_seconds)
                        + " rows/sec., minimum: " + toString(limits.min_execution_speed),
                        ErrorCodes::TOO_SLOW);

                if (limits.min_execution_speed_bytes && progress.bytes / elapsed_seconds < limits.min_execution_speed_bytes)
                    throw Exception("Query is executing too slow: " + toString(progress.bytes / elapsed_seconds)
                        + " bytes/sec., minimum: " + toString(limits.min_execution_speed_bytes),
                        ErrorCodes::TOO_SLOW);

                /// If the predicted execution time is longer than `max_execution_time`.
                if (limits.max_execution_time != 0 && total_rows)
                {
                    double estimated_execution_time_seconds = elapsed_seconds * (static_cast<double>(total_rows) / progress.rows);

                    if (estimated_execution_time_seconds > limits.max_execution_time.totalSeconds())
                        throw Exception("Estimated query execution time (" + toString(estimated_execution_time_seconds) + " seconds)"
                            + " is too long. Maximum: " + toString(limits.max_execution_time.totalSeconds())
                            + ". Estimated rows to process: " + toString(total_rows),
                            ErrorCodes::TOO_SLOW);
                }

                if (limits.max_execution_speed && progress.rows / elapsed_seconds >= limits.max_execution_speed)
                    limitProgressingSpeed(progress.rows, limits.max_execution_speed, total_elapsed_microseconds);

                if (limits.max_execution_speed_bytes && progress.bytes / elapsed_seconds >= limits.max_execution_speed_bytes)
                    limitProgressingSpeed(progress.bytes, limits.max_execution_speed_bytes, total_elapsed_microseconds);
            }
        }

        if (quota != nullptr && limits.mode == LIMITS_TOTAL)
        {
            quota->checkAndAddReadRowsBytes(time(nullptr), value.rows, value.bytes);
        }
    }
}


void IBlockInputStream::cancel(bool kill)
{
    if (kill)
        is_killed = true;

    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    forEachChild([&] (IBlockInputStream & child)
    {
        child.cancel(kill);
        return false;
    });
}


bool IBlockInputStream::isCancelled() const
{
    return is_cancelled;
}

bool IBlockInputStream::isCancelledOrThrowIfKilled() const
{
    if (!is_cancelled)
        return false;
    if (is_killed)
        throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);
    return true;
}


void IBlockInputStream::setProgressCallback(const ProgressCallback & callback)
{
    progress_callback = callback;

    forEachChild([&] (IBlockInputStream & child)
    {
        child.setProgressCallback(callback);
        return false;
    });
}


void IBlockInputStream::setProcessListElement(QueryStatus * elem)
{
    process_list_elem = elem;

    forEachChild([&] (IBlockInputStream & child)
    {
        child.setProcessListElement(elem);
        return false;
    });
}


Block IBlockInputStream::getTotals()
{
    if (totals)
        return totals;

    Block res;
    forEachChild([&] (IBlockInputStream & child)
    {
        res = child.getTotals();
        if (res)
            return true;
        return false;
    });
    return res;
}


Block IBlockInputStream::getExtremes()
{
    if (extremes)
        return extremes;

    Block res;
    forEachChild([&] (IBlockInputStream & child)
    {
        res = child.getExtremes();
        if (res)
            return true;
        return false;
    });
    return res;
}


String IBlockInputStream::getTreeID() const
{
    std::stringstream s;
    s << getName();

    if (!children.empty())
    {
        s << "(";
        for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
        {
            if (it != children.begin())
                s << ", ";
            s << (*it)->getTreeID();
        }
        s << ")";
    }

    return s.str();
}


size_t IBlockInputStream::checkDepthImpl(size_t max_depth, size_t level) const
{
    if (children.empty())
        return 0;

    if (level > max_depth)
        throw Exception("Query pipeline is too deep. Maximum: " + toString(max_depth), ErrorCodes::TOO_DEEP_PIPELINE);

    size_t res = 0;
    for (BlockInputStreams::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        size_t child_depth = (*it)->checkDepth(level + 1);
        if (child_depth > res)
            res = child_depth;
    }

    return res + 1;
}


void IBlockInputStream::dumpTree(std::ostream & ostr, size_t indent, size_t multiplier) const
{
    ostr << String(indent, ' ') << getName();
    if (multiplier > 1)
        ostr << " × " << multiplier;
    //ostr << ": " << getHeader().dumpStructure();
    ostr << std::endl;
    ++indent;

    /// If the subtree is repeated several times, then we output it once with the multiplier.
    using Multipliers = std::map<String, size_t>;
    Multipliers multipliers;

    for (const auto & child : children)
        ++multipliers[child->getTreeID()];

    for (const auto & child : children)
    {
        String id = child->getTreeID();
        size_t & subtree_multiplier = multipliers[id];
        if (subtree_multiplier != 0)    /// Already printed subtrees are marked with zero in the array of multipliers.
        {
            child->dumpTree(ostr, indent, subtree_multiplier);
            subtree_multiplier = 0;
        }
    }
}

}
