#include <Storages/MergeTree/WhatIfProjectionEstimator.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnSparse.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Common/Stopwatch.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterHypotheticalObjectQuery.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/optimizeReadInOrder.h>
#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/PartDirIntent.h>
#include <Storages/MergeTree/WhatIfSettings.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ProjectionsDescription.h>

#include <cmath>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsBool optimize_use_projections;
    extern const SettingsBool use_primary_key;
    extern const SettingsBool use_constant_folding_in_index_analysis;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsBool use_const_adaptive_granularity;
    extern const MergeTreeSettingsNonZeroUInt64 merge_max_block_size;
    extern const MergeTreeSettingsUInt64 merge_max_block_size_bytes;
}

namespace
{

struct ProjectionPartData
{
    /// projection key columns, in read order
    Block key_block;
    /// sort permutation of key_block
    IColumn::Permutation order;
    size_t rows = 0;
    /// uncompressed bytes, used for granularity
    size_t bytes = 0;
    /// per-row bytes, only filled for adaptive granularity
    PaddedPODArray<UInt32> row_bytes;
    /// some stored column has values of different sizes
    bool variable_width = false;
};

/// per-row version of `getBlockSizeForGranularity`
void appendRowSizes(PaddedPODArray<UInt32> & row_bytes, bool & variable_width, const Block & block)
{
    const size_t rows = block.rows();
    const size_t offset = row_bytes.size();
    row_bytes.resize_fill(offset + rows, 0);

    UInt32 fixed = 0;
    for (const auto & elem : block)
    {
        if (!elem.column)
            continue;
        if (elem.column->valuesHaveFixedSize())
            fixed += static_cast<UInt32>(elem.column->sizeOfValueIfFixed());
        else
        {
            variable_width = true;
            for (size_t i = 0; i < rows; ++i)
                row_bytes[offset + i] += static_cast<UInt32>(elem.column->byteSizeAt(i));
        }
    }
    for (size_t i = 0; i < rows; ++i)
        row_bytes[offset + i] += fixed;
}

/// whether the projection order helps an outer ORDER BY
enum class SortOrderHelp
{
    Helps,
    NotUseful,
    NoOrderBy,
    ReadInOrderDisabled,
};

String describe(SortOrderHelp help)
{
    switch (help)
    {
        case SortOrderHelp::Helps:
            return "the projection order serves the ORDER BY";
        case SortOrderHelp::NotUseful:
            return "the projection order does not serve the ORDER BY";
        case SortOrderHelp::NoOrderBy:
            return "the query has no ORDER BY to serve";
        case SortOrderHelp::ReadInOrderDisabled:
            return "reading in order is disabled";
    }
}

bool findPath(QueryPlan::Node * node, const IQueryPlanStep * target, std::vector<QueryPlan::Node *> & path)
{
    if (!node)
        return false;
    path.push_back(node);
    if (node->step.get() == target)
        return true;
    for (auto * child : node->children)
        if (findPath(child, target, path))
            return true;
    path.pop_back();
    return false;
}

/// the filter/expression chain above the read that the optimizer would replace, and the sort above it
struct ReadSlice
{
    QueryPlan::Node * root = nullptr;
    const SortingStep * outer_sorting = nullptr;
};

ReadSlice findReadSlice(QueryPlan::Node * root, const ReadFromMergeTree * read_step)
{
    std::vector<QueryPlan::Node *> path;
    if (!findPath(root, read_step, path) || path.size() < 2)
        return {};

    size_t i = path.size() - 1;
    while (i > 0)
    {
        --i;
        const auto * step = path[i]->step.get();
        if (!typeid_cast<const FilterStep *>(step) && !typeid_cast<const ExpressionStep *>(step))
            break;
    }

    ReadSlice slice{path[i + 1], nullptr};
    const auto * sort = typeid_cast<const SortingStep *>(path[i]->step.get());
    if (sort && sort->getType() == SortingStep::Type::Full)
        slice.outer_sorting = sort;
    return slice;
}

/// the whole part, or one granule from every run of `sample_step` at a hashed position,
/// so data that repeats with the step can't line up with the sample
MarkRanges marksToScan(const DataPartPtr & part, size_t sample_step)
{
    const size_t marks = part->index_granularity->getMarksCountWithoutFinal();
    if (sample_step <= 1)
        return {{0, marks}};
    const UInt64 seed = sipHash64(part->name);
    MarkRanges ranges;
    for (size_t first = 0; first < marks; first += sample_step)
    {
        const size_t mark = first + intHash64(seed ^ first) % std::min(sample_step, marks - first);
        ranges.emplace_back(mark, mark + 1);
    }
    return ranges;
}

/// grow a [low, high] mark range by `spread` on both sides
void widen(UInt64 & low, UInt64 & high, double spread)
{
    const auto by = static_cast<UInt64>(std::ceil(spread));
    low = low > by ? low - by : 0;
    high += by;
}

/// scale a size down to the sample, keeping it at least 1
size_t scaleSize(size_t value, double scale)
{
    return value == 0 ? 0 : std::max<size_t>(1, static_cast<size_t>(std::llround(static_cast<double>(value) * scale)));
}

/// read the given granules of a part
Pipe makePartPipe(
    const DataPartPtr & part,
    const MarkRanges & ranges,
    const Names & columns_to_read,
    ReadFromMergeTree * read_step,
    const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto & mutations_snapshot = read_step->getMutationsSnapshot();

    /// apply patch parts and on-the-fly mutations
    auto alter_conversions = mutations_snapshot
        ? MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, context
#if CLICKHOUSE_CLOUD
            , context->getAccess()->getEnabledMaskingPolicies()
#endif
            )
        : std::make_shared<AlterConversions>();

    Pipe pipe = createMergeTreeSequentialSource(
        MergeTreeSequentialSourceType::Merge,
        data,
        read_step->getStorageSnapshot(),
        RangesInDataPart(part),
        alter_conversions,
        nullptr,
        columns_to_read,
        ranges,
        std::make_shared<std::atomic<size_t>>(0),
        false,
        false,
        false);

    /// keep speed limits, the caller checks sizes
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

    return pipe;
}

/// read only the key columns, false if a read limit was hit
bool buildProjectionPart(
    ProjectionPartData & out,
    const ProjectionDescription & projection,
    const DataPartPtr & part,
    const MarkRanges & ranges,
    ReadFromMergeTree * read_step,
    const SizeLimits & read_limits,
    bool need_row_bytes,
    UInt64 & total_rows_read,
    UInt64 & total_bytes_read,
    const ContextPtr & context)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    Pipe pipe = makePartPipe(part, ranges, projection.required_columns, read_step, context);
    QueryPipeline pipeline(std::move(pipe));
    pipeline.setProcessListElement(context->getProcessListElement());
    pipeline.setProgressCallback(context->getProgressCallback());
    pipeline.setQuota(context->getQuota());
    /// count against the query's quota
    pipeline.setNormalizedQueryHash(context->getNormalizedQueryHash());
    PullingPipelineExecutor executor(pipeline);

    MutableColumns key_columns;
    Block block;
    while (executor.pull(block))
    {
        if (!block.rows())
            continue;

        total_rows_read += block.rows();
        total_bytes_read += block.bytes();
        /// softCheck: hitting a limit makes the estimate unsupported instead of failing the query
        if (!read_limits.softCheck(total_rows_read, total_bytes_read))
            return false;

        /// key expression and sort need full columns
        for (auto & column : block)
            column.column = recursiveRemoveSparse(column.column);

        /// measured before the key expression, as the writer does
        out.bytes += getBlockSizeForGranularity(block);
        if (need_row_bytes)
            appendRowSizes(out.row_bytes, out.variable_width, block);
        /// `required_columns` can skip a subcolumn the key needs, add it back like the writer does
        for (const auto & required : proj_key.expression->getRequiredColumns())
            if (!block.has(required))
                block.insert(block.getSubcolumnByName(required));
        proj_key.expression->execute(block);

        if (key_columns.empty())
            for (const auto & name : proj_key.column_names)
                key_columns.push_back(block.getByName(name).column->convertToFullColumnIfConst()->cloneEmpty());

        for (size_t i = 0; i < key_columns.size(); ++i)
        {
            auto source = block.getByName(proj_key.column_names[i]).column->convertToFullColumnIfConst();
            key_columns[i]->insertRangeFrom(*source, 0, source->size());
        }
    }

    if (key_columns.empty() || key_columns[0]->empty())
        return true;

    out.rows = key_columns[0]->size();
    /// projection indexes also store the parent offset
    if (projection.with_parent_part_offset)
    {
        out.bytes += out.rows * sizeof(UInt64);
        for (auto & row_size : out.row_bytes)
            row_size += sizeof(UInt64);
    }
    for (size_t i = 0; i < key_columns.size(); ++i)
        out.key_block.insert({std::move(key_columns[i]), proj_key.data_types[i], proj_key.column_names[i]});

    return true;
}

/// reproduce how the writer splits a part into granules, block by block
std::vector<size_t> simulateWriterMarks(
    const ProjectionPartData & data,
    MergeTreeDataPartType part_type,
    const MergeTreeSettings & mt_settings,
    bool adaptive_marks,
    double scale,
    size_t block_rows_limit,
    size_t block_bytes_limit)
{
    const size_t granularity_bytes = scaleSize(mt_settings[MergeTreeSetting::index_granularity_bytes], scale);
    const size_t fixed_granularity_rows = scaleSize(mt_settings[MergeTreeSetting::index_granularity], scale);
    const bool per_row_bytes = data.row_bytes.size() == data.rows;
    const size_t average_row_bytes = data.rows != 0 ? std::max<size_t>(data.bytes / data.rows, 1) : 1;
    auto row_bytes = [&](size_t row) -> size_t { return per_row_bytes ? data.row_bytes[data.order[row]] : average_row_bytes; };

    std::vector<size_t> mark_rows;
    size_t recorded = 0; /// rows covered by marks
    size_t written = 0; /// rows written

    for (size_t row = 0; row < data.rows;)
    {
        size_t block_rows = 0;
        size_t block_bytes = 0;
        while (row + block_rows < data.rows && block_rows < block_rows_limit)
        {
            const size_t bytes = row_bytes(row + block_rows);
            if (block_bytes_limit != 0 && block_rows != 0 && block_bytes + bytes > block_bytes_limit)
                break;
            block_bytes += bytes;
            ++block_rows;
        }
        row += block_rows;

        const size_t granule_rows = computeIndexGranularity(
            block_rows, block_bytes, granularity_bytes, fixed_granularity_rows, /* blocks_are_granules */ false, adaptive_marks);

        /// rows that still fit into the mark the previous block left open
        size_t offset = 0;
        if (recorded > written)
        {
            const size_t open_rows = written - (recorded - mark_rows.back());
            /// shrink an oversized open mark first, as the wide writer does
            if (mark_rows.back() - open_rows > granule_rows)
            {
                recorded -= mark_rows.back();
                mark_rows.back() = std::max(open_rows, granule_rows);
                recorded += mark_rows.back();
            }
            offset = std::min(recorded - written, block_rows);
        }

        for (size_t cur = offset; cur < block_rows; cur += granule_rows)
        {
            const size_t left = block_rows - cur;
            /// the compact writer closes the block's tail, the wide one keeps the mark open
            const bool close_tail = part_type == MergeTreeDataPartType::Compact && left < granule_rows
                && (block_rows >= granule_rows || offset != 0) && !mark_rows.empty();
            if (close_tail)
            {
                if (left * 2 >= granule_rows)
                    mark_rows.push_back(left);
                else
                    mark_rows.back() += left;
                recorded += left;
            }
            else
            {
                mark_rows.push_back(granule_rows);
                recorded += granule_rows;
            }
        }
        written += block_rows;
    }

    /// trim the last mark, like `adjustLastMark`
    if (recorded > written)
    {
        recorded -= mark_rows.back();
        mark_rows.back() = written - recorded;
    }
    if (!mark_rows.empty() && mark_rows.back() == 0)
        mark_rows.pop_back();
    return mark_rows;
}

/// marks one part of the projection would read, and their possible range
struct PartEstimate
{
    MarkRanges pruned;
    MergeTreeIndexGranularityPtr granularity;
    UInt64 marks_low = 0;
    UInt64 marks_high = 0;
    /// for a sample, the selected share of every granule read
    std::vector<double> granule_shares;
};

/// build the projection's primary index in memory and prune it, nothing is written
PartEstimate pruneSyntheticProjectionPart(
    ProjectionPartData & data,
    const ProjectionDescription & projection,
    const MergeTreeData & merge_tree,
    const RangesInDataPart & parent_ranges,
    const KeyCondition * key_condition,
    const ConditionTemplate<KeyCondition>::Ptr & part_offset_condition,
    const ConditionTemplate<KeyCondition>::Ptr & total_offset_condition,
    const MergeTreeSettings & mt_settings,
    const Settings & query_settings,
    bool uneven_rows,
    double scale,
    const MarkRanges & ranges_read,
    LoggerPtr log)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    SortDescription sort_description;
    sort_description.reserve(proj_key.column_names.size());
    for (const auto & name : proj_key.column_names)
        sort_description.emplace_back(name, 1, 1);

    stableGetPermutation(data.key_block, sort_description, data.order);
    /// format of the full part, not the sample
    const auto full_size = [scale](size_t value) { return static_cast<size_t>(static_cast<double>(value) / scale); };
    const auto level = parent_ranges.data_part->info.level;
    const auto part_type = merge_tree.choosePartFormat(full_size(data.bytes), full_size(data.rows), level, &projection).part_type;
    const bool adaptive_marks = parent_ranges.data_part->index_granularity_info.mark_type.adaptive;
    /// only adaptive granularity lets block sizes change the layout
    const bool granularity_per_block = part_type == MergeTreeDataPartType::Compact
        || (adaptive_marks && !mt_settings[MergeTreeSetting::use_const_adaptive_granularity]);

    /// the layout depends on blocks a part doesn't record, so try each and keep the range
    const size_t merge_rows = scaleSize(mt_settings[MergeTreeSetting::merge_max_block_size], scale);
    const size_t merge_bytes = scaleSize(mt_settings[MergeTreeSetting::merge_max_block_size_bytes], scale);
    /// one granule of bytes is the finest block size that matters
    const size_t granule_bytes = scaleSize(mt_settings[MergeTreeSetting::index_granularity_bytes], scale);
    std::vector<std::pair<size_t, size_t>> chunkings;
    size_t primary = 0;
    chunkings.emplace_back(data.rows, 0); /// insert or materialization
    if (granularity_per_block)
    {
        chunkings.emplace_back(merge_rows, merge_bytes); /// merge
        chunkings.emplace_back(merge_rows, granule_bytes); /// merge cutting blocks at every source
        /// merged parts were written block by block
        if (parent_ranges.data_part->info.level > 0)
            primary = uneven_rows ? chunkings.size() - 1 : chunkings.size() - 2;
    }

    std::vector<std::vector<size_t>> layouts;
    layouts.reserve(chunkings.size());
    for (const auto & [rows_limit, bytes_limit] : chunkings)
        layouts.push_back(simulateWriterMarks(data, part_type, mt_settings, adaptive_marks, scale, rows_limit, bytes_limit));

    auto prune = [&](const std::vector<size_t> & rows_per_mark, MergeTreeIndexGranularityPtr & granularity)
    {
        const size_t num_marks = rows_per_mark.size();
        std::vector<size_t> partial_sums(num_marks);
        for (size_t mark = 0, row = 0; mark < num_marks; ++mark)
        {
            row += rows_per_mark[mark];
            partial_sums[mark] = row;
        }
        granularity = std::make_shared<MergeTreeIndexGranularityAdaptive>(partial_sums);

        if (!key_condition)
            return MarkRanges{{0, num_marks}};

        /// index value = key at the first row of each granule
        Columns index_columns;
        index_columns.reserve(data.key_block.columns());
        for (const auto & key_column : data.key_block)
        {
            auto index_column = key_column.column->cloneEmpty();
            for (size_t mark = 0; mark < num_marks; ++mark)
                index_column->insertFrom(*key_column.column, data.order[mark != 0 ? partial_sums[mark - 1] : 0]);
            index_columns.push_back(std::move(index_column));
        }

        /// `Synthetic` never touches the part directory, `CreateFresh` could delete a leftover `.tmp_proj`
        auto synthetic_part = const_cast<IMergeTreeDataPart &>(*parent_ranges.data_part)
                                  .getProjectionPartBuilder(
                                      projection.name, &projection, PartDirIntent::Synthetic, /* is_temp_projection */ true)
                                  .withPartType(MergeTreeDataPartType::Compact)
                                  .withBytesAndRows(0, data.rows, 0)
                                  .build();
        synthetic_part->index_granularity = granularity;
        synthetic_part->setIndex(index_columns);

        RangesInDataPart synthetic_ranges(
            synthetic_part, parent_ranges.data_part, parent_ranges.part_index_in_query, parent_ranges.part_starting_offset_in_query);
        synthetic_ranges.ranges = MarkRanges{{0, num_marks}};

        return MergeTreeDataSelectExecutor::markRangesFromPKRange(
            synthetic_ranges,
            projection.metadata,
            *key_condition,
            part_offset_condition ? &part_offset_condition->generateForPart(synthetic_part) : nullptr,
            total_offset_condition ? &total_offset_condition->generateForPart(synthetic_part) : nullptr,
            nullptr,
            nullptr,
            query_settings,
            log);
    };

    PartEstimate estimate;
    estimate.pruned = prune(layouts[primary], estimate.granularity);
    estimate.marks_low = estimate.pruned.getNumberOfMarks();
    estimate.marks_high = estimate.marks_low;
    for (size_t i = 0; i < layouts.size(); ++i)
    {
        /// same layout, same result
        if (i == primary || layouts[i] == layouts[primary])
            continue;
        MergeTreeIndexGranularityPtr other_granularity;
        const UInt64 other_marks = prune(layouts[i], other_granularity).getNumberOfMarks();
        estimate.marks_low = std::min(estimate.marks_low, other_marks);
        estimate.marks_high = std::max(estimate.marks_high, other_marks);
    }

    if (scale < 1.0)
    {
        /// the sampled granule each row came from, and how many rows each one has
        const auto & parent_granularity = *parent_ranges.data_part->index_granularity;
        std::vector<size_t> granule_rows;
        std::vector<UInt32> source;
        source.reserve(data.rows);
        for (const auto & range : ranges_read)
            for (size_t mark = range.begin; mark < range.end; ++mark)
            {
                granule_rows.push_back(parent_granularity.getMarkRows(mark));
                source.resize(source.size() + granule_rows.back(), static_cast<UInt32>(granule_rows.size() - 1));
            }
        chassert(source.size() == data.rows);

        /// neighbours in projection order from one sampled granule mean the key follows the parent order,
        /// and then a range end can be off by a whole sampling step instead of one granule
        size_t same_source = 0;
        for (size_t pos = 1; pos < data.rows; ++pos)
            same_source += source[data.order[pos]] == source[data.order[pos - 1]];
        const double follows_parent = static_cast<double>(same_source) / static_cast<double>(std::max<size_t>(1, data.rows - 1));
        const double step = static_cast<double>(parent_granularity.getMarksCountWithoutFinal()) / static_cast<double>(granule_rows.size());
        const double granules_per_end = 1.0 + follows_parent * (step - 1.0);
        const double range_ends = 2.0 * static_cast<double>(std::max<size_t>(1, estimate.pruned.size()));

        /// granule sizes rounded at the sample's scale are up to a row off
        const double marks = static_cast<double>(estimate.pruned.getNumberOfMarks());
        const double rounding = marks * static_cast<double>(layouts[primary].size()) / static_cast<double>(data.rows);

        widen(estimate.marks_low, estimate.marks_high, range_ends * granules_per_end + rounding);

        std::vector<size_t> selected(granule_rows.size());
        for (const auto & range : estimate.pruned)
        {
            const size_t end = estimate.granularity->getMarkStartingRow(range.end);
            for (size_t pos = estimate.granularity->getMarkStartingRow(range.begin); pos < end; ++pos)
                ++selected[source[data.order[pos]]];
        }
        for (size_t i = 0; i < granule_rows.size(); ++i)
            estimate.granule_shares.push_back(static_cast<double>(selected[i]) / static_cast<double>(granule_rows[i]));
    }
    return estimate;
}

bool tryEstimateProjection(
    WhatIfCandidateResult & result,
    const ProjectionDescription & projection,
    const KeyCondition * key_condition,
    const ConditionTemplate<KeyCondition>::Ptr & part_offset_condition,
    const ConditionTemplate<KeyCondition>::Ptr & total_offset_condition,
    SortOrderHelp sort_help,
    ReadFromMergeTree * read_step,
    const RangesInDataParts & baseline_parts,
    UInt64 baseline_marks,
    UInt64 max_rows_to_scan,
    const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto mt_settings_ptr = data.getSettings(&projection.settings_changes);
    const auto & mt_settings = *mt_settings_ptr;
    const auto & query_settings = context->getSettingsRef();

    /// check read limits by hand
    const SizeLimits read_limits(
        query_settings[Setting::max_rows_to_read], query_settings[Setting::max_bytes_to_read], query_settings[Setting::read_overflow_mode]);
    UInt64 total_rows_read = 0;
    UInt64 total_bytes_read = 0;

    /// the scan reads whole parts, so past the budget sample granules instead
    UInt64 rows_to_scan = 0;
    UInt64 marks_to_scan = 0;
    for (const auto & part_with_ranges : baseline_parts)
    {
        rows_to_scan += part_with_ranges.data_part->rows_count;
        marks_to_scan += part_with_ranges.data_part->index_granularity->getMarksCountWithoutFinal();
    }
    UInt64 budget = max_rows_to_scan;
    if (const UInt64 read_limit = query_settings[Setting::max_rows_to_read]; read_limit != 0)
        budget = budget == 0 ? read_limit : std::min(budget, read_limit);
    size_t sample_step = budget != 0 && rows_to_scan > budget ? (rows_to_scan + budget - 1) / budget : 1;
    /// but read at least ~30 granules, fewer can't give an error estimate
    if (sample_step > 1)
        sample_step = std::min<size_t>(sample_step, std::max<size_t>(1, marks_to_scan / 30));
    std::vector<double> granule_shares;
    UInt64 layout_marks = 0;

    /// a sample's row offsets are not the part's, so an offset filter can't be applied to it
    if (sample_step > 1 && (part_offset_condition || total_offset_condition))
    {
        result.empirical_unsupported_reason
            = "The query filters on part offsets, which an estimate from a sample of granules cannot follow (see max_rows_to_scan)";
        return false;
    }

    Stopwatch watch;
    auto log = getLogger("WhatIfProjectionEstimator");

    UInt64 projection_marks = 0;
    UInt64 projection_rows = 0;
    UInt64 scanned_parts = 0;
    UInt64 scanned_marks = 0;
    UInt64 marks_low = 0;
    UInt64 marks_high = 0;
    UInt64 uneven_width_parts = 0;

    for (const auto & part_with_ranges : baseline_parts)
    {
        const auto & part = part_with_ranges.data_part;
        const size_t part_marks = part->index_granularity->getMarksCountWithoutFinal();
        if (part_marks == 0)
            continue;

        /// row bytes only matter when granules are sized by bytes
        const bool adaptive = part->index_granularity_info.mark_type.adaptive
            && mt_settings[MergeTreeSetting::index_granularity_bytes] != 0;

        const MarkRanges ranges = marksToScan(part, sample_step);
        ProjectionPartData part_data;
        if (!buildProjectionPart(
                part_data, projection, part, ranges, read_step, read_limits, adaptive, total_rows_read, total_bytes_read, context))
        {
            result.empirical_unsupported_reason
                = "The projection scan hit the read limit of the query (max_rows_to_read / max_bytes_to_read)";
            return false;
        }

        ++scanned_parts;
        scanned_marks += ranges.getNumberOfMarks();
        /// with uneven row widths the layout depends on block boundaries we can't know
        /// a sample can't show that rows it didn't read have the same width
        bool uneven_rows = sample_step > 1 && part_data.variable_width;
        if (!uneven_rows && part_data.row_bytes.size() == part_data.rows && !part_data.row_bytes.empty())
        {
            const auto [narrowest, widest] = std::minmax_element(part_data.row_bytes.begin(), part_data.row_bytes.end());
            uneven_rows = *narrowest != *widest;
        }
        if (uneven_rows)
            ++uneven_width_parts;
        /// no key rows from a non-empty part: the key needs a column we don't read, e.g. `_part_offset`
        if (part_data.rows == 0 && part->rows_count > 0)
        {
            result.empirical_unsupported_reason = "The projection key could not be built from the columns the projection stores";
            return false;
        }
        if (part_data.rows == 0)
            continue;

        /// share of the part's rows that was read
        const double scale = sample_step > 1 ? static_cast<double>(part_data.rows) / static_cast<double>(part->rows_count) : 1.0;

        const auto estimate = pruneSyntheticProjectionPart(
            part_data,
            projection,
            data,
            part_with_ranges,
            key_condition,
            part_offset_condition,
            total_offset_condition,
            mt_settings,
            query_settings,
            uneven_rows,
            scale,
            ranges,
            log);

        projection_marks += estimate.pruned.getNumberOfMarks();
        projection_rows += static_cast<UInt64>(
            std::llround(static_cast<double>(estimate.granularity->getRowsCountInRanges(estimate.pruned)) / scale));
        marks_low += estimate.marks_low;
        marks_high += estimate.marks_high;
        layout_marks += estimate.granularity->getMarksCount();
        granule_shares.insert(granule_shares.end(), estimate.granule_shares.begin(), estimate.granule_shares.end());
    }

    /// widen by two standard errors of the selected share (successive differences, fits a systematic sample)
    if (granule_shares.size() > 1)
    {
        double sum_of_squares = 0;
        for (size_t i = 1; i < granule_shares.size(); ++i)
            sum_of_squares += (granule_shares[i] - granule_shares[i - 1]) * (granule_shares[i] - granule_shares[i - 1]);
        const double count = static_cast<double>(granule_shares.size());
        const double standard_error = std::sqrt(sum_of_squares / (2.0 * count * (count - 1.0)));
        widen(marks_low, marks_high, 2.0 * standard_error * static_cast<double>(layout_marks));
    }

    result.estimated_marks = projection_marks;
    result.estimated_rows = projection_rows;
    result.estimated_marks_low = marks_low;
    result.estimated_marks_high = marks_high;
    auto marks_text = [](UInt64 marks) { return fmt::format("{} mark{}", marks, marks == 1 ? "" : "s"); };
    /// decide only if the whole range agrees
    auto would_win = [&](UInt64 marks)
    { return marks < baseline_marks || (marks == baseline_marks && sort_help == SortOrderHelp::Helps); };
    if (uneven_width_parts != 0)
    {
        result.verdict = "too close to call";
        result.verdict_reason = fmt::format(
            "{} against {} from the base table, but the rows are not known to have the same width on {} of the {} parts read, "
            "so the granule layout depends on the blocks the writer was fed and the mark count is a model, not a measurement",
            marks_text(projection_marks),
            baseline_marks,
            uneven_width_parts,
            scanned_parts);
    }
    else if (would_win(marks_low) != would_win(marks_high))
    {
        result.verdict = "too close to call";
        result.verdict_reason = fmt::format(
            "{} against {} from the base table, and the layout a merge would leave is not recorded in a part, so the "
            "comparison comes out both ways over `marks_span`",
            marks_text(projection_marks),
            baseline_marks);
    }
    else if (projection_marks != baseline_marks)
    {
        result.verdict = projection_marks < baseline_marks ? "chosen" : "not chosen";
        result.verdict_reason
            = fmt::format("{} would be read instead of {} from the base table", marks_text(projection_marks), baseline_marks);
    }
    else
    {
        result.verdict = sort_help == SortOrderHelp::Helps ? "chosen" : "not chosen";
        result.verdict_reason
            = fmt::format("the same {} would be read, and {}", marks_text(projection_marks), describe(sort_help));
    }
    result.estimate_source = WhatIfCandidateResult::Empirical;
    result.empirical_status = WhatIfCandidateResult::Ok;
    result.sampled_parts = scanned_parts;
    result.sampled_marks = scanned_marks;
    /// out of what a full scan would read
    result.total_marks = marks_to_scan;
    result.elapsed_us = watch.elapsedMicroseconds();
    return true;
}

}

std::optional<ProjectionDescription> refreshHypotheticalProjection(
    const ProjectionDescription & stored,
    const MergeTreeData & data,
    const StorageMetadataPtr & metadata,
    const ContextPtr & context,
    String & reason)
{
    if (!stored.required_columns.empty())
        context->checkAccess(AccessType::SELECT, data.getStorageID(), stored.required_columns);
    context->checkAccess(AccessType::ALTER_ADD_PROJECTION, data.getStorageID());

    std::optional<ProjectionDescription> fresh;
    try
    {
        checkHypotheticalProjectionIsAddable(data, metadata, stored.definition_ast, /* if_not_exists */ false, context);
        fresh = ProjectionDescription::getProjectionFromAST(
            stored.definition_ast, metadata->getColumns(), &metadata->partition_key, context, LoadingStrictnessLevel::CREATE);
    }
    catch (const Exception &)
    {
        reason = "Hypothetical projection can no longer be added to this table: " + getCurrentExceptionMessage(false);
        return std::nullopt;
    }

    /// an ALTER may retarget an ALIAS, so check the columns actually read
    if (!fresh->required_columns.empty())
        context->checkAccess(AccessType::SELECT, data.getStorageID(), fresh->required_columns);
    return fresh;
}

WhatIfCandidateResult evaluateProjection(
    const ProjectionDescription & stored_projection,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & baseline_parts,
    const WhatIfSettings & settings,
    QueryPlan::Node * plan_root,
    ContextPtr context)
{
    const auto & data = read_step->getMergeTreeData();

    WhatIfCandidateResult result;
    result.kind = WhatIfCandidateResult::Projection;
    result.name = stored_projection.name;
    result.type = stored_projection.type == ProjectionDescription::Type::Aggregate ? "aggregate projection" : "normal projection";
    result.status = WhatIfCandidateResult::NotApplicable;
    result.total_parts = data.getActivePartsCount();
    result.total_marks = data.getTotalMarksCount();

    /// before the refresh: a projection-served read carries that projection's metadata
    if (analysis.readFromProjection() && !baseline_parts.empty())
    {
        result.not_applicable_reason = "The query is already served from projection '" + baseline_parts.front().data_part->name
            + "', EXPLAIN WHATIF estimates candidates against the base table read only";
        return result;
    }

    auto metadata = read_step->getStorageMetadata();
    auto projection = refreshHypotheticalProjection(stored_projection, data, metadata, context, result.not_applicable_reason);
    if (!projection)
        return result;

    if (!context->getSettingsRef()[Setting::optimize_use_projections])
    {
        result.not_applicable_reason = "Projections are disabled by `optimize_use_projections = 0`";
        return result;
    }

    if (projection->type == ProjectionDescription::Type::Aggregate)
    {
        result.not_applicable_reason
            = "EXPLAIN WHATIF estimates normal (sorted) hypothetical projections only, aggregate projections are not estimated yet";
        return result;
    }

    if (projection->where_clause_ast)
    {
        result.not_applicable_reason = "EXPLAIN WHATIF does not estimate projections with a WHERE clause yet";
        return result;
    }

    if (!projection->metadata->getSecondaryIndices().empty())
    {
        result.not_applicable_reason = "EXPLAIN WHATIF does not estimate projections with their own skip indexes yet";
        return result;
    }

    if (baseline_parts.empty())
    {
        result.not_applicable_reason = "The query reads no parts, so the optimizer would not consider a projection";
        return result;
    }

    if (!QueryPlanOptimizations::canUseProjectionForReadingStep(read_step))
    {
        result.not_applicable_reason = "The optimizer does not consider projections for this read (for example FINAL, SAMPLE, "
                                       "reading in order, pending mutations, or a parallel-replicas mode without projection support)";
        return result;
    }

    for (const auto & column_name : read_step->getAllColumnNames())
    {
        if (!projection->sample_block.findColumnOrSubcolumnByName(column_name) && !projection->metadata->virtuals.has(column_name))
        {
            result.not_applicable_reason = fmt::format(
                "Projection does not contain column {} required by the query, so it could only filter the base table's "
                "parts, which EXPLAIN WHATIF does not estimate yet",
                backQuoteIfNeed(column_name));
            return result;
        }
    }

    const auto & proj_key = projection->metadata->getSortingKey();
    if (proj_key.column_names.empty())
    {
        result.not_applicable_reason = "Projection has no sort key to prune on";
        return result;
    }

    /// commit-order keys and keys over columns the projection doesn't store can't be rebuilt
    for (const auto & required : proj_key.expression->getRequiredColumns())
    {
        if (required == BlockNumberColumn::name || required == BlockOffsetColumn::name)
        {
            result.not_applicable_reason = fmt::format(
                "Projection orders by the commit order ({}, {}), which EXPLAIN WHATIF does not estimate yet",
                backQuote(BlockNumberColumn::name),
                backQuote(BlockOffsetColumn::name));
            return result;
        }
        if (!metadata->getColumns().hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, required))
        {
            result.not_applicable_reason = fmt::format(
                "Projection orders by {}, which it does not store, so EXPLAIN WHATIF cannot rebuild its key",
                backQuoteIfNeed(required));
            return result;
        }
    }

    /// the writer builds these only on merge, so fresh parts have none
    if (projection->with_block_number)
    {
        result.not_applicable_reason = fmt::format(
            "Projection stores {}, so it is built only when a part is merged, which EXPLAIN WHATIF does not estimate yet",
            backQuote(BlockNumberColumn::name));
        return result;
    }

    const auto slice = findReadSlice(plan_root, read_step);

    /// the optimizer can't replay this slice on a projection, e.g. an ARRAY JOIN in it
    if (slice.root)
    {
        QueryPlanOptimizations::QueryDAG replay;
        if (!replay.build(*slice.root))
        {
            result.not_applicable_reason
                = "The filters and expressions above the read cannot be replayed on a projection (an ARRAY JOIN in one of "
                  "them, for example), so the optimizer would not use a projection for this read";
            return result;
        }
    }

    SortOrderHelp sort_help = SortOrderHelp::NoOrderBy;
    if (slice.outer_sorting)
    {
        if (!QueryPlanOptimizationSettings(context).read_in_order)
            sort_help = SortOrderHelp::ReadInOrderDisabled;
        else if (QueryPlanOptimizations::wouldReadInOrderBeUseful(*slice.outer_sorting, proj_key, *slice.root))
            sort_help = SortOrderHelp::Helps;
        else
            sort_help = SortOrderHelp::NotUseful;
    }

    /// key condition over the projection key
    const auto & filter_dag = read_step->getFilterActionsDAG();
    std::shared_ptr<ActionsDAGWithInversionPushDown> predicate_dag;
    std::optional<KeyCondition> key_condition;
    ConditionTemplate<KeyCondition>::Ptr part_offset_condition;
    ConditionTemplate<KeyCondition>::Ptr total_offset_condition;
    if (filter_dag)
    {
        predicate_dag = std::make_shared<ActionsDAGWithInversionPushDown>(
            filter_dag->getOutputs().front(), context, /* boolean_context */ true);
        key_condition.emplace(
            *predicate_dag, context, proj_key, /* single_point */ false, !context->getSettingsRef()[Setting::use_primary_key]);

        /// offset conditions as `ReadFromMergeTree` builds them, except for projections storing parent offsets
        const bool skip_folding = !context->getSettingsRef()[Setting::use_constant_folding_in_index_analysis];
        const auto & proj_columns = projection->metadata->getColumns();
        const bool offsets_are_the_parent_s = projection->with_parent_part_offset;
        if (!offsets_are_the_parent_s && !proj_columns.has("_part_offset") && !proj_columns.has("_part"))
            part_offset_condition = MergeTreeDataSelectExecutor::buildKeyConditionFromPartOffset(
                predicate_dag, projection->metadata, skip_folding, context);
        if (!offsets_are_the_parent_s && !proj_columns.has("_part_offset") && !proj_columns.has("_part_starting_offset"))
            total_offset_condition = MergeTreeDataSelectExecutor::buildKeyConditionFromTotalOffset(
                predicate_dag, projection->metadata, skip_folding, context);

        if (key_condition->alwaysUnknownOrTrue() && !part_offset_condition && !total_offset_condition)
            key_condition.reset();
    }

    /// same gate as the optimizer: needs a filter or a useful sort order
    if (!filter_dag && sort_help != SortOrderHelp::Helps)
    {
        result.not_applicable_reason = fmt::format("Query has no filter predicate, and {}", describe(sort_help));
        return result;
    }

    result.status = WhatIfCandidateResult::Applicable;

    if (settings.empirical)
    {
        if (tryEstimateProjection(
                result, *projection, key_condition ? &*key_condition : nullptr, part_offset_condition, total_offset_condition,
                sort_help, read_step, baseline_parts, analysis.selected_marks, settings.max_rows_to_scan, context))
            return result;
        result.empirical_status = WhatIfCandidateResult::Unsupported;
    }
    else
    {
        result.empirical_status = WhatIfCandidateResult::Disabled;
    }

    result.estimate_source = WhatIfCandidateResult::ApplicabilityOnly;
    return result;
}

}
