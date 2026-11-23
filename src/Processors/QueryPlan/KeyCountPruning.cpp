#include <Processors/QueryPlan/KeyCountPruning.h>

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <atomic>

namespace CurrentMetrics
{
    extern const Metric MergeTreeDataSelectExecutorThreads;
    extern const Metric MergeTreeDataSelectExecutorThreadsActive;
    extern const Metric MergeTreeDataSelectExecutorThreadsScheduled;
}

namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 max_threads_for_indexes;
}

/// The rows of the read that have a value of the first primary key column lie in the granules whose value interval
/// contains it. Within a part, sorted by the key, granule `g` holds values from its first key in the primary index up
/// to the first key of granule `g + 1` (the last key of the part for the last granule of a part with a final mark,
/// any larger value otherwise). The rows of the selected granules whose interval contains a value therefore bound how
/// many rows of the read have it, and a granule whose whole interval stays below `min_count` can only hold
/// groups that the HAVING bound rejects.
std::optional<RangesInDataParts> pruneRangesByKeyCountLowerBound(
    const RangesInDataParts & parts, UInt64 min_count, const ContextPtr & context, const LoggerPtr & log)
{
    /// The sweep below runs in this thread before reading and costs about 10-30 ns and 4-20 bytes plus at most one key
    /// value per selected mark. Bound it, and keep it small against reading the marks: a mark of 4096 rows of the cheapest column
    /// still takes about 1 us of a 16-thread read.
    static constexpr size_t max_marks_to_sweep = 1 << 20;
    static constexpr size_t min_rows_per_mark_to_sweep = 4096;
    const size_t selected_marks = parts.getMarksCountAllParts();
    if (selected_marks > max_marks_to_sweep || parts.getRowsCountAllParts() < selected_marks * min_rows_per_mark_to_sweep)
        return {};

    /// The values of a granule include its first one, which the granule alone gives as many rows as it has, and the
    /// first one of the next granule, which the next granule has too. So a granule can only be skipped if it has fewer
    /// rows than the bound, also together with each neighbour it shares a value with.
    bool has_candidate = false;
    for (const auto & part : parts)
    {
        const auto & granularity = *part.data_part->index_granularity;
        for (const auto & range : part.ranges)
        {
            size_t previous = 0;
            size_t current = range.begin < range.end ? granularity.getMarkRows(range.begin) : 0;
            for (size_t mark = range.begin; mark < range.end && !has_candidate; ++mark)
            {
                const size_t next = mark + 1 < range.end ? granularity.getMarkRows(mark + 1) : 0;
                has_candidate = previous + current < min_count && current + next < min_count;
                previous = current;
                current = next;
            }
            if (has_candidate)
                break;
        }
        if (has_candidate)
            break;
    }
    if (!has_candidate)
        return {};

    const size_t num_parts = parts.size();
    for (const auto & part : parts)
        if (part.parent_part || !part.exact_ranges.empty())
            return {};

    Stopwatch watch;

    /// Load the primary indexes like the index analysis does: in parallel, and checking the query's limits.
    auto query_status = context->getProcessListElement();
    VectorWithMemoryTracking<IMergeTreeDataPart::IndexPtr> part_indexes(num_parts);
    {
        const auto & query_settings = context->getSettingsRef();
        size_t parts_to_load = 0;
        size_t marks_to_load = 0;
        for (const auto & part : parts)
        {
            if (part.data_part->isIndexInMemory())
                continue;
            ++parts_to_load;
            marks_to_load += part.data_part->index_granularity->getMarksCountWithoutFinal();
        }
        /// A read of a few marks of large parts must not load their whole primary indexes.
        if (marks_to_load > selected_marks)
            return {};
        size_t num_threads = std::min<size_t>(parts_to_load, query_settings[Setting::max_threads]);
        if (query_settings[Setting::max_threads_for_indexes])
            num_threads = std::min<size_t>(num_threads, query_settings[Setting::max_threads_for_indexes]);

        auto load_index = [&](size_t part_index)
        {
            if (query_status)
                query_status->checkTimeLimit();
            part_indexes[part_index] = parts[part_index].data_part->getIndex();
        };

        if (num_threads <= 1)
        {
            for (size_t part_index = 0; part_index < num_parts; ++part_index)
                load_index(part_index);
        }
        else
        {
            /// Outlives `pool`, whose destructor joins the jobs that still read it when scheduling throws.
            std::atomic<size_t> next_part_index = 0;
            ThreadPool pool(
                CurrentMetrics::MergeTreeDataSelectExecutorThreads,
                CurrentMetrics::MergeTreeDataSelectExecutorThreadsActive,
                CurrentMetrics::MergeTreeDataSelectExecutorThreadsScheduled,
                num_threads);
            for (size_t job = 0; job < num_threads; ++job)
            {
                pool.scheduleOrThrowOnError(
                    [&, thread_group = CurrentThread::getGroup(), component_name = Coordination::getCurrentComponent()]
                    {
                        ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGETREE_INDEX);
                        auto local_component_guard = Coordination::setCurrentComponent(component_name);
                        while (!pool.isFinished())
                        {
                            const size_t part_index = next_part_index.fetch_add(1, std::memory_order_relaxed);
                            if (part_index >= num_parts)
                                break;
                            load_index(part_index);
                        }
                    });
            }
            pool.wait();
        }
    }

    VectorWithMemoryTracking<const IColumn *> key_columns(num_parts);
    for (size_t i = 0; i < num_parts; ++i)
    {
        if (!part_indexes[i] || part_indexes[i]->empty())
            return {};
        key_columns[i] = (*part_indexes[i])[0].get();
        if (i && !key_columns[i]->structureEquals(*key_columns[0]))
            return {};
    }

    auto compare_keys = [&](size_t part_index, size_t row, const std::pair<size_t, size_t> & other)
    {
        return key_columns[part_index]->compareAt(row, other.second, *key_columns[other.first], 1);
    };

    /// A part whose selected granules form one run of at least two gives each value from the run's first key to the
    /// first key of its last granule at least the rows of its smallest granule but the last one. If that adds up to the
    /// bound over the values common to such parts, a granule reaching into them holds a value that reaches the bound,
    /// and only the granules reaching out of them hold the other values. The sweep then goes over these alone, which
    /// saves most of it when many parts each cover most of the key range.
    struct Run
    {
        size_t part;
        size_t begin;
        size_t end;
    };
    VectorWithMemoryTracking<Run> single_runs;
    UInt64 average_rows = 0;
    for (size_t part_index = 0; part_index < num_parts; ++part_index)
    {
        const auto & ranges = parts[part_index].ranges;
        if (ranges.empty())
            continue;
        const size_t begin = ranges.front().begin;
        size_t end = ranges.front().end;
        bool one_run = true;
        for (size_t i = 1; i < ranges.size() && one_run; ++i)
        {
            one_run = ranges[i].begin == end;
            end = ranges[i].end;
        }
        if (!one_run || end < begin + 2 || end > key_columns[part_index]->size())
            continue;
        single_runs.push_back({part_index, begin, end});
        const auto & granularity = *parts[part_index].data_part->index_granularity;
        average_rows += granularity.getRowsCountInRange(begin, end - 1) / (end - 1 - begin);
    }

    std::optional<std::pair<size_t, size_t>> common_begin;
    std::optional<std::pair<size_t, size_t>> common_end;
    /// The smallest granule has at most the average rows, so the exact check is only needed when the averages suffice.
    if (average_rows >= min_count)
    {
        UInt64 common_rows = 0;
        for (const auto & run : single_runs)
        {
            const auto & granularity = *parts[run.part].data_part->index_granularity;
            /// With a constant granularity only the last granule of a part can have fewer rows.
            const auto constant_rows = granularity.getConstantGranularity();
            size_t min_rows = constant_rows ? *constant_rows : granularity.getMarkRows(run.begin);
            if (!constant_rows)
                for (size_t mark = run.begin + 1; mark + 1 < run.end; ++mark)
                    min_rows = std::min(min_rows, granularity.getMarkRows(mark));
            common_rows += min_rows;
            if (!common_begin || compare_keys(run.part, run.begin, *common_begin) > 0)
                common_begin.emplace(run.part, run.begin);
            if (!common_end || compare_keys(run.part, run.end - 1, *common_end) < 0)
                common_end.emplace(run.part, run.end - 1);
        }
        if (common_rows < min_count || compare_keys(common_begin->first, common_begin->second, *common_end) > 0)
        {
            common_begin.reset();
            common_end.reset();
        }
    }

    /// The selected granules to sweep over: all of them, or those that reach out of the common values.
    VectorWithMemoryTracking<MarkRanges> sweep_ranges(num_parts);
    for (size_t part_index = 0; part_index < num_parts; ++part_index)
    {
        const auto & ranges = parts[part_index].ranges;
        const size_t index_rows = key_columns[part_index]->size();
        auto & sweep = sweep_ranges[part_index];
        for (const auto & range : ranges)
        {
            if (range.end > index_rows)
                return {};
            if (!common_begin)
            {
                sweep.push_back(range);
                continue;
            }
            for (size_t mark = range.begin; mark < range.end; ++mark)
            {
                if (compare_keys(part_index, mark, *common_begin) >= 0 && mark + 1 < index_rows
                    && compare_keys(part_index, mark + 1, *common_end) <= 0)
                    continue;
                if (!sweep.empty() && sweep.back().end == mark)
                    ++sweep.back().end;
                else
                    sweep.emplace_back(mark, mark + 1);
            }
        }
    }

    /// The points are the first keys of the selected granules and, after each run of consecutive selected granules,
    /// the first key of the next granule, which ends the run's last granule; the last granule of a part without that
    /// key ends after every value. Calls `f(part, begin, end, has_end_point, first_point)` for the runs [begin, end) in
    /// the order of the parts and their ranges; false if the ranges are not ascending or not covered by the index.
    auto for_each_run = [&](auto && f)
    {
        size_t first_point = 0;
        for (size_t part_index = 0; part_index < num_parts; ++part_index)
        {
            const auto & ranges = sweep_ranges[part_index];
            const size_t index_rows = key_columns[part_index]->size();
            for (size_t i = 0; i < ranges.size();)
            {
                const size_t begin = ranges[i].begin;
                size_t end = ranges[i].end;
                for (++i; i < ranges.size() && ranges[i].begin == end; ++i)
                    end = ranges[i].end;
                if (begin >= end || end > index_rows || (i < ranges.size() && ranges[i].begin < end))
                    return false;
                const bool has_end_point = end < index_rows;
                f(part_index, begin, end, has_end_point, first_point);
                first_point += end - begin + has_end_point;
            }
        }
        return true;
    };

    size_t num_points = 0;
    if (!for_each_run([&](size_t, size_t begin, size_t end, bool has_end_point, size_t) { num_points += end - begin + has_end_point; })
        || !num_points)
        return {};

    /// Rank the points by value. Consecutive points of a part are often equal, for a value that spans granules, so the
    /// typed sort gets the first point of each run of equal ones. Comparing each point with the previous one of its
    /// part also checks that the part ascends.
    VectorWithMemoryTracking<UInt32> rank(num_points);
    size_t num_values = 0;
    {
        auto points = key_columns[0]->cloneEmpty();
        bool ascending = true;
        size_t previous_part = num_parts;
        size_t previous_row = 0;
        for_each_run([&](size_t part_index, size_t begin, size_t end, bool has_end_point, size_t first_point)
        {
            const auto & column = *key_columns[part_index];
            for (size_t row = begin; row < end + has_end_point; ++row)
            {
                const int cmp = part_index == previous_part ? column.compareAt(row, previous_row, column, 1) : 1;
                ascending &= cmp >= 0;
                if (cmp != 0)
                    points->insertFrom(column, row);
                rank[first_point + row - begin] = static_cast<UInt32>(points->size() - 1);
                previous_part = part_index;
                previous_row = row;
            }
        });
        if (!ascending)
            return {};

        IColumn::Permutation order;
        points->getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Unstable, 0, 1, order);
        VectorWithMemoryTracking<UInt32> value_of_point(order.size());
        for (size_t k = 0; k < order.size(); ++k)
        {
            if (k && points->compareAt(order[k - 1], order[k], *points, 1) != 0)
                ++num_values;
            value_of_point[order[k]] = static_cast<UInt32>(num_values);
        }
        ++num_values;
        for (auto & value : rank)
            value = value_of_point[value];
    }

    if (query_status && !query_status->checkTimeLimit())
        return {};

    /// Calls `f(part, begin, end, first, last)` for the selected granules [begin, end) of a part with values from value
    /// `first` to value `last`: one granule, or consecutive ones that all hold the single value spanning them.
    auto for_each_granules = [&](auto && f)
    {
        for_each_run([&](size_t part_index, size_t begin, size_t end, bool has_end_point, size_t first_point)
        {
            /// The first value of the next granule, or every value after the last one of the part.
            auto last_value = [&](size_t mark)
            {
                return mark + 1 < end || has_end_point ? rank[first_point + mark - begin + 1] : static_cast<UInt32>(num_values - 1);
            };
            for (size_t mark = begin; mark < end;)
            {
                const UInt32 first = rank[first_point + mark - begin];
                const UInt32 last = last_value(mark);
                size_t next = mark + 1;
                if (first == last)
                    while (next < end && last_value(next) == first)
                        ++next;
                f(part_index, mark, next, first, last);
                mark = next;
            }
        });
    };

    /// The rows that can have each value, from the change at the first and after the last value of each granule. A
    /// value between two points has at most the rows of the point below it, which is in every granule it is in, so only
    /// the points need checking. The array then turns into the number of values below each one that can reach the bound.
    VectorWithMemoryTracking<UInt64> reaching_below(num_values + 1, 0);
    for_each_granules([&](size_t part_index, size_t begin, size_t end, UInt32 first, UInt32 last)
    {
        const UInt64 granules_rows = parts[part_index].data_part->index_granularity->getRowsCountInRange(begin, end);
        /// Unsigned: the running sum below wraps back to the true, non-negative counts.
        reaching_below[first] += granules_rows;
        reaching_below[last + 1] -= granules_rows;
    });

    UInt64 rows = 0;
    UInt64 reaching = 0;
    for (size_t value = 0; value < num_values; ++value)
    {
        rows += reaching_below[value];
        reaching_below[value] = reaching;
        reaching += rows >= min_count;
    }
    reaching_below[num_values] = reaching;

    VectorWithMemoryTracking<MarkRanges> skipped(num_parts);
    size_t pruned_marks = 0;
    size_t pruned_rows = 0;
    for_each_granules([&](size_t part_index, size_t begin, size_t end, UInt32 first, UInt32 last)
    {
        if (reaching_below[last + 1] != reaching_below[first])
            return;
        /// The common values reach the bound, but the sweep left out granules that hold them.
        if (common_begin)
        {
            const bool below = end < key_columns[part_index]->size() && compare_keys(part_index, end, *common_begin) < 0;
            if (!below && compare_keys(part_index, begin, *common_end) <= 0)
                return;
        }
        pruned_marks += end - begin;
        pruned_rows += parts[part_index].data_part->index_granularity->getRowsCountInRange(begin, end);
        auto & ranges = skipped[part_index];
        if (!ranges.empty() && ranges.back().end == begin)
            ranges.back().end = end;
        else
            ranges.emplace_back(begin, end);
    });

    LOG_DEBUG(log, "Skipped {} of {} marks ({} rows) whose primary key values cannot have {} rows, swept {} points in {:.3f} ms",
        pruned_marks, selected_marks, pruned_rows, min_count, num_points, watch.elapsedSeconds() * 1000);
    if (!pruned_marks)
        return {};

    RangesInDataParts kept_parts;
    for (size_t i = 0; i < num_parts; ++i)
    {
        /// The selected ranges without the skipped ones; both ascend.
        MarkRanges kept;
        size_t next_skipped = 0;
        for (const auto & range : parts[i].ranges)
        {
            size_t begin = range.begin;
            while (begin < range.end)
            {
                while (next_skipped < skipped[i].size() && skipped[i][next_skipped].end <= begin)
                    ++next_skipped;
                if (next_skipped == skipped[i].size() || skipped[i][next_skipped].begin >= range.end)
                {
                    kept.emplace_back(begin, range.end);
                    break;
                }
                if (skipped[i][next_skipped].begin > begin)
                    kept.emplace_back(begin, skipped[i][next_skipped].begin);
                begin = std::min(range.end, skipped[i][next_skipped].end);
            }
        }
        if (kept.empty())
            continue;
        auto part = parts[i];
        part.ranges = std::move(kept);
        kept_parts.push_back(std::move(part));
    }
    return kept_parts;
}

}
