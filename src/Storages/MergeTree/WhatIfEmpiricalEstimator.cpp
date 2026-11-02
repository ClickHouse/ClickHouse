#include <Storages/MergeTree/WhatIfEmpiricalEstimator.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/WhatIfSampling.h>

#include <Columns/ColumnSparse.h>
#include <Common/Stopwatch.h>
#include <Core/Settings.h>

#include <algorithm>
#include <cmath>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 merge_tree_min_rows_for_seek;
    extern const SettingsUInt64 merge_tree_min_bytes_for_seek;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsOverflowMode read_overflow_mode;
}

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
}

namespace
{

/// Expand each baseline mark range to the full skip-index windows it touches (a window is
/// `granularity` data granules) and merge them into ranges
MarkRanges skipIndexWindowsOverlapping(const MarkRanges & baseline, size_t granularity, size_t total_marks)
{
    MarkRanges windows;
    if (granularity == 0 || total_marks == 0)
        return windows;

    for (const auto & range : baseline)
    {
        const size_t last = std::min(range.end, total_marks);
        if (range.begin >= last)
            continue;

        const size_t begin = range.begin / granularity * granularity;
        const size_t end = std::min((last - 1) / granularity * granularity + granularity, total_marks);
        if (!windows.empty() && begin <= windows.back().end)
            windows.back().end = std::max(windows.back().end, end);
        else
            windows.emplace_back(begin, end);
    }
    return windows;
}

size_t windowsInRange(const MarkRange & range, size_t granularity)
{
    return (range.end - range.begin + granularity - 1) / granularity;
}

size_t countWindows(const MarkRanges & windows, size_t granularity)
{
    size_t count = 0;
    for (const auto & range : windows)
        count += windowsInRange(range, granularity);
    return count;
}

/// all windows (`run` = 0), or the windows of a sample of the baseline marks, so a window with more baseline marks is more likely in the sample
/// one mark from every run of `run` baseline marks. All parts use the same run, so a part gets draws in proportion to its baseline marks
/// the same `GRANULARITY` gives every candidate the same windows
/// `weights` gets the baseline marks that each sampled window stands for: the runs that took it, and the last run can be shorter
MarkRanges windowsToScan(
    const MarkRanges & baseline, const MarkRanges & windows, size_t granularity, size_t total_marks, size_t run,
    const String & part_name, std::vector<double> & weights)
{
    if (run == 0 || windows.empty())
        return windows;

    const size_t marks = baseline.getNumberOfMarks();
    const auto sampled = sampleUnits(marks, run, part_name);
    MarkRanges ranges;
    const auto * range = baseline.begin();
    /// the baseline marks in the ranges before `range`
    size_t marks_before = 0;
    for (size_t i = 0; i < sampled.size(); ++i)
    {
        while (sampled[i] >= marks_before + range->getNumberOfMarks())
        {
            marks_before += range->getNumberOfMarks();
            ++range;
        }
        const size_t begin = (range->begin + sampled[i] - marks_before) / granularity * granularity;
        const auto run_marks = static_cast<double>(std::min(run, marks - i * run));
        /// a window with the marks of more than a run can take more than one mark
        if (!ranges.empty() && ranges.back().end > begin)
        {
            weights.back() += run_marks;
            continue;
        }
        const size_t end = std::min(begin + granularity, total_marks);
        if (!ranges.empty() && ranges.back().end == begin)
            ranges.back().end = end;
        else
            ranges.emplace_back(begin, end);
        weights.push_back(run_marks);
    }
    return ranges;
}

}

bool tryEstimateEmpirical(
    WhatIfCandidateResult & result,
    const MergeTreeIndexPtr & index_helper,
    const MergeTreeIndexConditionPtr & condition,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & saved_parts,
    std::vector<UInt8> * surviving_marks,
    UInt64 index_scan_budget_rows,
    ContextPtr context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto & storage_snapshot = read_step->getStorageSnapshot();
    const auto & mutations_snapshot = read_step->getMutationsSnapshot();

    Names index_columns = index_helper->getColumnsRequiredForIndexCalc();
    if (index_columns.empty())
        return false;

    /// With non-zero seek gaps a real read coalesces ranges, so our per-granule count would diverge
    if (context->getSettingsRef()[Setting::merge_tree_min_rows_for_seek] != 0
        || context->getSettingsRef()[Setting::merge_tree_min_bytes_for_seek] != 0)
    {
        result.empirical_unsupported_reason = "A real read coalesces mark ranges when "
            "`merge_tree_min_rows_for_seek` or `merge_tree_min_bytes_for_seek` is not 0, which the "
            "per-granule count does not model";
        return false;
    }

    UInt64 total_data_granules = 0;
    UInt64 skipped_data_granules = 0;
    Stopwatch watch;

    /// The whole-part scan is not the normal read pipeline, so enforce the query's
    /// read limits explicitly (max_execution_time is handled by the process-list element)
    const auto & limit_settings = context->getSettingsRef();
    const SizeLimits read_limits(
        limit_settings[Setting::max_rows_to_read],
        limit_settings[Setting::max_bytes_to_read],
        limit_settings[Setting::read_overflow_mode]);
    UInt64 total_rows_read = 0;
    UInt64 total_bytes_read = 0;

    const size_t skip_index_granularity = index_helper->index.granularity;
    auto index_expression = index_helper->index.expression;

    /// the skip-index windows overlapping the baseline, all or a sample past the budget, as the projection estimate plans
    std::vector<MarkRanges> read_ranges(saved_parts.size());
    std::vector<size_t> part_windows(saved_parts.size());
    UInt64 rows_to_scan = 0;
    size_t windows_to_scan = 0;
    size_t marks_to_scan = 0;
    for (size_t i = 0; i < saved_parts.size(); ++i)
    {
        const auto & granularity = *saved_parts[i].data_part->index_granularity;
        read_ranges[i]
            = skipIndexWindowsOverlapping(saved_parts[i].ranges, skip_index_granularity, granularity.getMarksCountWithoutFinal());
        part_windows[i] = countWindows(read_ranges[i], skip_index_granularity);
        rows_to_scan += granularity.getRowsCountInRanges(read_ranges[i]);
        windows_to_scan += part_windows[i];
        marks_to_scan += saved_parts[i].ranges.getNumberOfMarks();
    }
    const UInt64 read_limit = limit_settings[Setting::max_rows_to_read];
    const size_t step = samplingStep(rows_to_scan, windows_to_scan, index_scan_budget_rows, read_limit);
    /// the baseline marks of `step` windows on average, so the sample reads about one window of every `step`
    const size_t run = step <= 1 ? 0 : (marks_to_scan * step + windows_to_scan - 1) / windows_to_scan;

    bool sampled = false;
    std::vector<std::vector<double>> sample_weights(saved_parts.size());
    UInt64 rows_planned = 0;
    for (size_t i = 0; i < saved_parts.size(); ++i)
    {
        const auto & part = *saved_parts[i].data_part;
        read_ranges[i] = windowsToScan(
            saved_parts[i].ranges, read_ranges[i], skip_index_granularity, part.index_granularity->getMarksCountWithoutFinal(), run,
            part.name, sample_weights[i]);
        sampled |= countWindows(read_ranges[i], skip_index_granularity) < part_windows[i];
        rows_planned += part.index_granularity->getRowsCountInRanges(read_ranges[i]);
    }
    if (read_limit != 0 && rows_planned > read_limit)
    {
        result.empirical_unsupported_reason = fmt::format(
            "The estimate would read {} rows, over max_rows_to_read = {} (a sample keeps at least ~{} index windows and one per part)",
            rows_planned,
            read_limit,
            min_sampled_units);
        return false;
    }
    /// the windows out of the sample are unknown, so the caller does not combine this candidate
    if (sampled)
        surviving_marks = nullptr;

    /// with a sample every part counts with its draws, also a part with all windows in the sample:
    /// the draws decide which parts these are, so their exact counts would bias the estimate
    /// the most baseline marks that one run stands for
    double heaviest_draw = 0;
    UInt64 baseline_marks = 0;
    std::vector<double> window_survives;
    /// a sampled window counts with the baseline marks it stands for
    std::vector<double> window_weights;

    /// Position of the next baseline mark, gives every candidate's bitmap the same coordinates
    size_t baseline_mark_pos = 0;

    for (size_t part_idx = 0; part_idx < saved_parts.size(); ++part_idx)
    {
        const auto & part_with_ranges = saved_parts[part_idx];
        auto part = part_with_ranges.data_part;
        const auto & mark_ranges = part_with_ranges.ranges;

        if (mark_ranges.empty())
            continue;

        const auto & part_index_granularity = part->index_granularity;
        const size_t total_marks = part_index_granularity->getMarksCountWithoutFinal();

        std::vector<bool> in_baseline(part->getMarksCount(), false);
        for (const auto & range : mark_ranges)
            for (size_t m = range.begin; m < range.end && m < in_baseline.size(); ++m)
                in_baseline[m] = true;

        if (read_ranges[part_idx].empty())
            continue;
        const UInt64 part_baseline_marks = mark_ranges.getNumberOfMarks();
        const UInt64 rows_before = total_rows_read;
        const size_t window_weights_before = window_weights.size();

        /// Apply patch parts / on-the-fly mutations so we see the up-to-date values
        auto alter_conversions = mutations_snapshot
            ? MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, context
#if CLICKHOUSE_CLOUD
                , context->getAccess()->getEnabledMaskingPolicies()
#endif
                )
            : std::make_shared<AlterConversions>();

        /// aggregate each skip-index granule and count how many
        /// baseline granules the candidate would skip. Returns false if a read limit was hit
        auto scan_ranges = [&](const MarkRanges & ranges) -> bool
        {
            RangesInDataPart part_for_read(part);

            Pipe pipe = createMergeTreeSequentialSource(
                MergeTreeSequentialSourceType::Merge,
                data,
                storage_snapshot,
                std::move(part_for_read),
                alter_conversions,
                nullptr,
                index_columns,
                ranges,
                std::make_shared<std::atomic<size_t>>(0),
                false,
                false,
                false);

            /// Apply the query's execution-speed limits here too (size is the explicit check below)
            if (auto query_limits = read_step->getQueryInfo().storage_limits)
            {
                auto speed_limits = std::make_shared<StorageLimitsList>(*query_limits);
                for (auto & entry : *speed_limits)
                {
                    entry.local_limits.size_limits = {};
                    entry.leaf_limits = {};
                    entry.local_limits.speed_limits.max_execution_time = {};
                }
                for (const auto & processor : pipe.getProcessors())
                    processor->setStorageLimits(speed_limits);
            }

            QueryPipeline pipeline(std::move(pipe));
            /// Tie the scan to the query so quota / speed / time limits apply
            pipeline.setProcessListElement(context->getProcessListElement());
            pipeline.setProgressCallback(context->getProgressCallback());
            pipeline.setQuota(context->getQuota());
            /// Carry the query hash so the empirical scan's `read_rows`/`read_bytes` are accounted to the
            /// query's own bucket under a `KEYED BY normalized_query_hash` quota, not the shared hash-0 one.
            pipeline.setNormalizedQueryHash(context->getNormalizedQueryHash());
            PullingPipelineExecutor executor(pipeline);

            auto aggregator = index_helper->createIndexAggregator();
            size_t range_idx = 0;
            size_t current_mark = ranges.front().begin;
            size_t rows_remaining_in_mark = current_mark < total_marks
                ? part_index_granularity->getMarkRows(current_mark)
                : 0;
            size_t data_granules_in_window = 0;
            size_t baseline_marks_in_window = 0;
            std::vector<size_t> window_baseline_marks;

            auto flush_window = [&]
            {
                if (baseline_marks_in_window == 0)
                    return;
                auto granule = aggregator->getGranuleAndReset();
                total_data_granules += baseline_marks_in_window;
                const bool survives = condition->mayBeTrueOnGranule(granule, {});
                if (!survives)
                    skipped_data_granules += baseline_marks_in_window;
                else if (surviving_marks)
                    for (size_t pos : window_baseline_marks)
                        (*surviving_marks)[pos] = 1;
                if (sampled)
                {
                    /// every sampled window overlaps the baseline, so the windows flush in the order of the sample
                    chassert(window_weights.size() - window_weights_before < sample_weights[part_idx].size());
                    window_survives.push_back(survives);
                    window_weights.push_back(sample_weights[part_idx][window_weights.size() - window_weights_before]);
                }
            };

            auto on_mark_finished = [&]
            {
                ++data_granules_in_window;
                if (current_mark < in_baseline.size() && in_baseline[current_mark])
                {
                    ++baseline_marks_in_window;
                    if (surviving_marks)
                        window_baseline_marks.push_back(baseline_mark_pos++);
                }

                if (data_granules_in_window >= skip_index_granularity)
                {
                    flush_window();
                    aggregator = index_helper->createIndexAggregator();
                    data_granules_in_window = 0;
                    baseline_marks_in_window = 0;
                    window_baseline_marks.clear();
                }

                ++current_mark;
                /// one source reads all ranges in order, and each range ends at the end of a window
                if (current_mark == ranges[range_idx].end && range_idx + 1 < ranges.size())
                    current_mark = ranges[++range_idx].begin;
                rows_remaining_in_mark = current_mark < total_marks
                    ? part_index_granularity->getMarkRows(current_mark)
                    : 0;
            };

            Block block;
            while (executor.pull(block))
            {
                if (block.rows() == 0)
                    continue;

                total_rows_read += block.rows();
                total_bytes_read += block.bytes();
                /// throw mode raises here; break mode returns false so we don't pass off a partial scan as done
                if (!read_limits.check(total_rows_read, total_bytes_read, "rows or bytes to read",
                                       ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES))
                {
                    result.empirical_unsupported_reason = "The scan hit `max_rows_to_read` or "
                        "`max_bytes_to_read` before finishing, so the partial result is discarded";
                    return false;
                }

                /// Evaluate the index expression so the aggregator sees what a real
                /// MATERIALIZE INDEX would see (lower(s) instead of raw s)
                if (index_expression)
                    index_expression->execute(block);

                /// Index aggregators require full columns, sparse-serialized parts
                /// would otherwise trip getRawData (matches the real index writer)
                for (auto & column : block)
                    column.column = recursiveRemoveSparse(column.column);

                size_t pos = 0;
                aggregator->update(block, &pos, block.rows());

                if (block.rows() <= rows_remaining_in_mark)
                    rows_remaining_in_mark -= block.rows();
                else
                    rows_remaining_in_mark = 0;

                if (rows_remaining_in_mark == 0 && current_mark < total_marks)
                    on_mark_finished();
            }

            if (!aggregator->empty())
                flush_window();
            return true;
        };

        if (!scan_ranges(read_ranges[part_idx]))
            return false;

        /// a `break` time limit or a cancelled query stops the read silently
        if (total_rows_read - rows_before != part_index_granularity->getRowsCountInRanges(read_ranges[part_idx]))
        {
            result.empirical_unsupported_reason = "A time limit in `break` mode or a cancelled query stopped the index scan early";
            return false;
        }
        baseline_marks += part_baseline_marks;
        heaviest_draw = std::max(heaviest_draw, static_cast<double>(std::min<UInt64>(run, part_baseline_marks)));
    }

    if (total_data_granules == 0)
    {
        result.empirical_unsupported_reason = "The baseline selected no granules, so there was nothing to aggregate";
        return false;
    }

    result.estimate_source = WhatIfCandidateResult::Empirical;
    result.empirical_status = WhatIfCandidateResult::Ok;
    result.sampled_parts = analysis.selected_parts;
    result.sampled_marks = analysis.selected_marks;
    result.elapsed_us = watch.elapsedMicroseconds();

    if (!sampled)
    {
        result.skip_ratio = static_cast<double>(skipped_data_granules) / static_cast<double>(total_data_granules);
        result.estimated_marks = total_data_granules - skipped_data_granules;
        return true;
    }

    /// one share for all parts: a share for each part would drop the marks of its windows when it samples few
    double weighted_marks = 0;
    double weighted_survivors = 0;
    for (size_t i = 0; i < window_survives.size(); ++i)
    {
        weighted_marks += window_weights[i];
        weighted_survivors += window_survives[i] * window_weights[i];
    }
    /// the scan does not count a trailing window with an empty aggregator, so the draws can count no marks
    const double estimated_survivors = weighted_survivors * static_cast<double>(baseline_marks) / std::max(weighted_marks, 1.0);

    /// one range that the sample missed, with its ends known only to a run, and two standard errors of the surviving
    /// share (successive differences, which fit a systematic sample)
    double spread = 2.0 * static_cast<double>(run);
    if (!window_survives.empty())
    {
        /// the rule of three is the least error, also for a sample with no change and no spread: a change that no draw found
        /// has fewer marks than 3 heaviest draws, with a 95% chance. Light draws find less, so they do not make this error smaller
        const double sample_error = window_survives.size() > 1 ? standardErrorOfSample(window_survives, window_weights) : 0;
        const double least_error = 1.5 * heaviest_draw / std::max(weighted_marks, 1.0);
        spread += 2.0 * std::max(sample_error, least_error) * static_cast<double>(baseline_marks);
    }

    /// the sampled windows are exact, so they bound the estimate and the span
    const UInt64 least = total_data_granules - skipped_data_granules;
    const UInt64 most = baseline_marks - skipped_data_granules;
    const auto estimated = std::clamp(static_cast<UInt64>(std::round(estimated_survivors)), least, most);
    const auto by = static_cast<UInt64>(std::ceil(spread));
    result.estimated_marks = estimated;
    result.estimated_marks_low = std::max(least, estimated > by ? estimated - by : 0);
    result.estimated_marks_high = std::min(most, estimated + by);
    result.skip_ratio = 1.0 - static_cast<double>(estimated) / static_cast<double>(baseline_marks);
    result.sampled_marks = total_data_granules;

    return true;
}

}
