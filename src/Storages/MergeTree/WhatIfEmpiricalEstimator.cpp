#include <Storages/MergeTree/WhatIfEmpiricalEstimator.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>

#include <Columns/ColumnSparse.h>
#include <Common/Stopwatch.h>
#include <Core/Settings.h>

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

}

bool tryEstimateEmpirical(
    WhatIfIndexEstimator::IndexResult & result,
    const MergeTreeIndexPtr & index_helper,
    const MergeTreeIndexConditionPtr & condition,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & saved_parts,
    std::vector<UInt8> * surviving_marks,
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
        return false;

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

    /// Position of the next baseline mark, gives every candidate's bitmap the same coordinates
    size_t baseline_mark_pos = 0;

    for (const auto & part_with_ranges : saved_parts)
    {
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

        /// Read only the skip-index windows overlapping the baseline instead of the whole part
        const MarkRanges read_ranges = skipIndexWindowsOverlapping(mark_ranges, skip_index_granularity, total_marks);
        if (read_ranges.empty())
            continue;

        /// Apply patch parts / on-the-fly mutations so we see the up-to-date values
        auto alter_conversions = mutations_snapshot
            ? MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, context)
            : std::make_shared<AlterConversions>();

        /// aggregate each skip-index granule and count how many
        /// baseline granules the candidate would skip. Returns false if a read limit was hit
        auto scan_range = [&](const MarkRange & read_range) -> bool
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
                MarkRanges{read_range},
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
            size_t current_mark = read_range.begin;
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
                if (!condition->mayBeTrueOnGranule(granule, {}))
                    skipped_data_granules += baseline_marks_in_window;
                else if (surviving_marks)
                    for (size_t pos : window_baseline_marks)
                        (*surviving_marks)[pos] = 1;
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
                    return false;

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

        for (const auto & read_range : read_ranges)
            if (!scan_range(read_range))
                return false;
    }

    if (total_data_granules == 0)
        return false;

    result.skip_ratio = static_cast<double>(skipped_data_granules) / static_cast<double>(total_data_granules);
    result.estimated_marks = total_data_granules - skipped_data_granules;
    result.estimate_source = "empirical";
    result.empirical_status = WhatIfIndexEstimator::IndexResult::Ok;
    result.sampled_parts = analysis.selected_parts;
    result.sampled_marks = analysis.selected_marks;
    result.elapsed_us = watch.elapsedMicroseconds();

    return true;
}

}
