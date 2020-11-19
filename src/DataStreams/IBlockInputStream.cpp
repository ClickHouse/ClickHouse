#include <DataStreams/IBlockInputStream.h>

#include <Core/Field.h>
#include <Interpreters/ProcessList.h>
#include <Access/EnabledQuota.h>
#include <Common/CurrentThread.h>
#include <common/sleep.h>

namespace ProfileEvents
{
    extern const Event ThrottlerSleepMicroseconds;
    extern const Event SelectedRows;
    extern const Event SelectedBytes;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int LOGICAL_ERROR;
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

        if (quota)
            checkQuota(res);
    }
    else
    {
        /** If the stream is over, then we will ask all children to abort the execution.
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

            if (isColumnConst(*src))
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

            if (isColumnConst(*old_extremes))
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


bool IBlockInputStream::checkTimeLimit() const
{
    return limits.speed_limits.checkTimeLimit(info.total_stopwatch.elapsed(), limits.timeout_overflow_mode);
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
            UInt64 total_elapsed = info.total_stopwatch.elapsedNanoseconds();
            quota->used({Quota::RESULT_ROWS, block.rows()}, {Quota::RESULT_BYTES, block.bytes()}, {Quota::EXECUTION_TIME, total_elapsed - prev_elapsed});
            prev_elapsed = total_elapsed;
            break;
        }
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
        size_t total_rows_estimate = std::max(progress.read_rows, progress.total_rows_to_read);

        /** Check the restrictions on the amount of data to read, the speed of the query, the quota on the amount of data to read.
            * NOTE: Maybe it makes sense to have them checked directly in ProcessList?
            */
        if (limits.mode == LIMITS_TOTAL)
        {
            if (!limits.size_limits.check(total_rows_estimate, progress.read_bytes, "rows to read",
                                         ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES))
                cancel(false);
        }

        size_t total_rows = progress.total_rows_to_read;

        constexpr UInt64 profile_events_update_period_microseconds = 10 * 1000; // 10 milliseconds
        UInt64 total_elapsed_microseconds = info.total_stopwatch.elapsedMicroseconds();

        if (last_profile_events_update_time + profile_events_update_period_microseconds < total_elapsed_microseconds)
        {
            CurrentThread::updatePerformanceCounters();
            last_profile_events_update_time = total_elapsed_microseconds;
        }

        limits.speed_limits.throttle(progress.read_rows, progress.read_bytes, total_rows, total_elapsed_microseconds);

        if (quota && limits.mode == LIMITS_TOTAL)
            quota->used({Quota::READ_ROWS, value.read_rows}, {Quota::READ_BYTES, value.read_bytes});
    }

    ProfileEvents::increment(ProfileEvents::SelectedRows, value.read_rows);
    ProfileEvents::increment(ProfileEvents::SelectedBytes, value.read_bytes);
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
        return bool(res);
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
        return bool(res);
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
    for (const auto & child : children)
    {
        size_t child_depth = child->checkDepth(level + 1);
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
