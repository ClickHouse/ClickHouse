#include <Storages/MergeTree/WhatIfProjectionEstimator.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnSparse.h>
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

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsBool optimize_use_projections;
    extern const SettingsBool use_primary_key;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsBool use_const_adaptive_granularity;
}

namespace
{

struct ProjectionPartData
{
    /// the projection key, one row per projection row, in read order
    Block key_block;
    /// sorted order over key_block
    IColumn::Permutation order;
    size_t rows = 0;
    /// uncompressed size of the projection part, drives the granularity
    size_t bytes = 0;
    /// per-row size in read order, empty unless the part needs the adaptive granule walk
    PaddedPODArray<UInt32> row_bytes;
};

/// per-row counterpart of `getBlockSizeForGranularity`, a fixed-width column costs the same in every row
void appendRowSizes(PaddedPODArray<UInt32> & row_bytes, const Block & block)
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
            for (size_t i = 0; i < rows; ++i)
                row_bytes[offset + i] += static_cast<UInt32>(elem.column->byteSizeAt(i));
    }
    for (size_t i = 0; i < rows; ++i)
        row_bytes[offset + i] += fixed;
}

/// why the ORDER BY tie-break is or is not available
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

bool findPath(const QueryPlan::Node * node, const IQueryPlanStep * target, std::vector<const QueryPlan::Node *> & path)
{
    if (!node)
        return false;
    path.push_back(node);
    if (node->step.get() == target)
        return true;
    for (const auto * child : node->children)
        if (findPath(child, target, path))
            return true;
    path.pop_back();
    return false;
}

/// the full sort right above the read, through filters and expressions only
std::pair<const SortingStep *, const QueryPlan::Node *>
findOuterSorting(const QueryPlan::Node * root, const ReadFromMergeTree * read_step)
{
    std::vector<const QueryPlan::Node *> path;
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

    const auto * sort = typeid_cast<const SortingStep *>(path[i]->step.get());
    if (sort && sort->getType() == SortingStep::Type::Full)
        return {sort, path[i + 1]};
    return {};
}

/// reads the whole part, wired like the empirical index scan
Pipe makeWholePartPipe(const DataPartPtr & part, const Names & columns_to_read, ReadFromMergeTree * read_step, const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto & mutations_snapshot = read_step->getMutationsSnapshot();

    /// apply patch parts / on-the-fly mutations so the projection sees the up-to-date values
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
        MarkRanges{{0, part->index_granularity->getMarksCountWithoutFinal()}},
        std::make_shared<std::atomic<size_t>>(0),
        false,
        false,
        false);

    /// speed limits apply here too, size is checked by the caller
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

/// keep only the key columns, so peak memory is one key copy plus the permutation, false when a read limit was hit
bool buildProjectionPart(
    ProjectionPartData & out,
    const ProjectionDescription & projection,
    const DataPartPtr & part,
    ReadFromMergeTree * read_step,
    const SizeLimits & read_limits,
    bool need_row_bytes,
    UInt64 & total_rows_read,
    UInt64 & total_bytes_read,
    const ContextPtr & context)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    Pipe pipe = makeWholePartPipe(part, projection.required_columns, read_step, context);
    QueryPipeline pipeline(std::move(pipe));
    pipeline.setProcessListElement(context->getProcessListElement());
    pipeline.setProgressCallback(context->getProgressCallback());
    pipeline.setQuota(context->getQuota());
    /// account the scan to the query's own quota bucket
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
        /// softCheck, so `read_overflow_mode = 'throw'` degrades to `unsupported` like `break` does:
        /// this scan reads whole parts, and a query the user can still run must stay explainable
        if (!read_limits.softCheck(total_rows_read, total_bytes_read))
            return false;

        /// the key expression and the sort need full columns
        for (auto & column : block)
            column.column = recursiveRemoveSparse(column.column);

        /// the same measure the writer takes of the block it is about to store, before the key
        /// expression adds columns a normal projection recomputes on read instead of storing
        out.bytes += getBlockSizeForGranularity(block);
        if (need_row_bytes)
            appendRowSizes(out.row_bytes, block);
        /// `required_columns` drops a subcolumn whose physical column is there, and the writer puts it
        /// back before executing the key expression (`addSubcolumnsFromSortingKeyAndSkipIndicesExpression`)
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
    /// a projection index also stores the parent offset, which the writer sizes but `required_columns` omits
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

/// build the primary index in memory and prune it with the engine's own PK-range pruning, nothing is written
MarkRanges pruneSyntheticProjectionPart(
    ProjectionPartData & data,
    const ProjectionDescription & projection,
    const MergeTreeData & merge_tree,
    const DataPartPtr & parent_part,
    const KeyCondition * key_condition,
    const MergeTreeSettings & mt_settings,
    const Settings & query_settings,
    MergeTreeIndexGranularityPtr & granularity_out,
    LoggerPtr log)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    SortDescription sort_description;
    sort_description.reserve(proj_key.column_names.size());
    for (const auto & name : proj_key.column_names)
        sort_description.emplace_back(name, 1, 1);

    /// sorted order via one permutation
    stableGetPermutation(data.key_block, sort_description, data.order);

    const auto part_type
        = merge_tree.choosePartFormat(data.bytes, data.rows, parent_part->info.level, &projection).part_type;

    /// The writer picks one granule size per block it stores, from that block's average row size.
    /// An insert stores the projection of one whole inserted block, so its granules come out even and
    /// the part average is exact; a merge feeds the writer a long run of small blocks, so each granule
    /// ends up holding the rows that fit `index_granularity_bytes` at the width found at that point in
    /// the key order. Which of the two a part got is not recorded, so read it off the merge level.
    /// ponytail: heuristic. It is exact at both ends and can miss where a part was written some third
    /// way (a partial merge, a rebuilt projection); the margin below carries what it can miss by.
    /// a constant granularity object pins every block to the same granule size, only an adaptive one
    /// lets the writer resize per block, so follow `createMergeTreeIndexGranularity` on which it gets
    const bool granularity_varies_per_block = part_type == MergeTreeDataPartType::Compact
        || !mt_settings[MergeTreeSetting::use_const_adaptive_granularity];
    const bool granules_follow_row_width
        = data.row_bytes.size() == data.rows && parent_part->info.level > 0 && granularity_varies_per_block;

    std::vector<size_t> mark_rows;
    if (granules_follow_row_width)
    {
        const size_t granularity_bytes = mt_settings[MergeTreeSetting::index_granularity_bytes];
        const size_t max_granule_rows = mt_settings[MergeTreeSetting::index_granularity];
        size_t rows_in_granule = 0;
        size_t bytes_in_granule = 0;
        for (size_t i = 0; i < data.rows; ++i)
        {
            const size_t row_bytes = data.row_bytes[data.order[i]];
            /// a granule holds the rows that fit, the way `index_granularity_bytes / row size` sizes it
            if (rows_in_granule != 0 && bytes_in_granule + row_bytes > granularity_bytes)
            {
                mark_rows.push_back(rows_in_granule);
                rows_in_granule = 0;
                bytes_in_granule = 0;
            }
            ++rows_in_granule;
            bytes_in_granule += row_bytes;
            if (rows_in_granule == max_granule_rows)
            {
                mark_rows.push_back(rows_in_granule);
                rows_in_granule = 0;
                bytes_in_granule = 0;
            }
        }
        if (rows_in_granule != 0)
            mark_rows.push_back(rows_in_granule);
    }
    else
    {
        const size_t granule_rows = computeIndexGranularity(
            data.rows,
            data.bytes,
            mt_settings[MergeTreeSetting::index_granularity_bytes],
            mt_settings[MergeTreeSetting::index_granularity],
            /* blocks_are_granules */ false,
            parent_part->index_granularity_info.mark_type.adaptive);

        const size_t num = (data.rows + granule_rows - 1) / granule_rows;
        mark_rows.assign(num, granule_rows);
        mark_rows.back() = data.rows - (num - 1) * granule_rows;
    }

    /// the two writers part ways on the remainder: the wide one keeps the short last mark
    /// (`fillIndexGranularityImpl` plus `adjustLastMark`), the compact one folds a remainder below
    /// half a granule into the previous mark, so follow the format this part would get
    if (part_type == MergeTreeDataPartType::Compact && mark_rows.size() > 1
        && mark_rows.back() * 2 < mark_rows[mark_rows.size() - 2])
    {
        mark_rows[mark_rows.size() - 2] += mark_rows.back();
        mark_rows.pop_back();
    }

    const size_t num_marks = mark_rows.size();
    std::vector<size_t> mark_starts(num_marks);
    std::vector<size_t> partial_sums(num_marks);
    for (size_t mark = 0, row = 0; mark < num_marks; ++mark)
    {
        mark_starts[mark] = row;
        row += mark_rows[mark];
        partial_sums[mark] = row;
    }
    granularity_out = std::make_shared<MergeTreeIndexGranularityAdaptive>(partial_sums);

    if (!key_condition)
        return MarkRanges{{0, num_marks}};

    /// primary index = the key at the first row of every granule
    Columns index_columns;
    index_columns.reserve(data.key_block.columns());
    for (const auto & key_column : data.key_block)
    {
        auto index_column = key_column.column->cloneEmpty();
        for (size_t mark = 0; mark < num_marks; ++mark)
            index_column->insertFrom(*key_column.column, data.order[mark_starts[mark]]);
        index_columns.push_back(std::move(index_column));
    }

    /// the builder only reads the parent, it does not mutate it
    auto synthetic_part = const_cast<IMergeTreeDataPart &>(*parent_part)
                              .getProjectionPartBuilder(
                                  projection.name, &projection, PartDirIntent::CreateFresh, /* is_temp_projection */ true)
                              .withPartType(MergeTreeDataPartType::Compact)
                              .withBytesAndRows(0, data.rows, 0)
                              .build();
    synthetic_part->index_granularity = granularity_out;
    synthetic_part->setIndex(std::move(index_columns));

    RangesInDataPart synthetic_ranges(synthetic_part);
    synthetic_ranges.ranges = MarkRanges{{0, num_marks}};

    return MergeTreeDataSelectExecutor::markRangesFromPKRange(
        synthetic_ranges, projection.metadata, *key_condition, nullptr, nullptr, nullptr, nullptr, query_settings, log);
}

bool tryEstimateProjection(
    WhatIfCandidateResult & result,
    const ProjectionDescription & projection,
    const KeyCondition * key_condition,
    SortOrderHelp sort_help,
    ReadFromMergeTree * read_step,
    const RangesInDataParts & baseline_parts,
    UInt64 baseline_marks,
    const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto mt_settings_ptr = data.getSettings(&projection.settings_changes);
    const auto & mt_settings = *mt_settings_ptr;
    const auto & query_settings = context->getSettingsRef();

    /// enforce the read limits by hand
    const SizeLimits read_limits(
        query_settings[Setting::max_rows_to_read], query_settings[Setting::max_bytes_to_read], query_settings[Setting::read_overflow_mode]);
    UInt64 total_rows_read = 0;
    UInt64 total_bytes_read = 0;

    Stopwatch watch;
    auto log = getLogger("WhatIfProjectionEstimator");

    UInt64 projection_marks = 0;
    UInt64 projection_rows = 0;
    UInt64 scanned_parts = 0;
    UInt64 scanned_marks = 0;
    UInt64 adaptive_parts = 0;
    UInt64 uneven_width_parts = 0;

    for (const auto & part_with_ranges : baseline_parts)
    {
        const auto & part = part_with_ranges.data_part;
        const size_t part_marks = part->index_granularity->getMarksCountWithoutFinal();
        if (part_marks == 0)
            continue;

        /// the byte walk only differs from a fixed granule count when the writer would size by bytes
        const bool adaptive = part->index_granularity_info.mark_type.adaptive
            && mt_settings[MergeTreeSetting::index_granularity_bytes] != 0;

        ProjectionPartData part_data;
        if (!buildProjectionPart(
                part_data, projection, part, read_step, read_limits, adaptive, total_rows_read, total_bytes_read, context))
        {
            result.empirical_unsupported_reason
                = "The projection scan hit the read limit of the query (max_rows_to_read / max_bytes_to_read)";
            return false;
        }

        ++scanned_parts;
        scanned_marks += part_marks;
        if (adaptive)
            ++adaptive_parts;
        /// the writer sizes a granule from the average width of the block it stores, so once the rows
        /// differ in width the layout follows the blocks it was fed, and that is not recorded anywhere
        if (adaptive && part_data.row_bytes.size() == part_data.rows && !part_data.row_bytes.empty())
        {
            UInt32 lo = part_data.row_bytes[0];
            UInt32 hi = part_data.row_bytes[0];
            for (const auto row_size : part_data.row_bytes)
            {
                lo = std::min(lo, row_size);
                hi = std::max(hi, row_size);
            }
            if (hi != lo)
                ++uneven_width_parts;
        }
        /// no key rows out of a part that has rows means the key needs something the scan cannot
        /// provide, `_part_offset` for one, so do not pass a zero-mark estimate off as measured
        if (part_data.rows == 0 && part->rows_count > 0)
        {
            result.empirical_unsupported_reason = "The projection key could not be built from the columns the projection stores";
            return false;
        }
        if (part_data.rows == 0)
            continue;

        MergeTreeIndexGranularityPtr granularity;
        MarkRanges pruned
            = pruneSyntheticProjectionPart(
                part_data, projection, data, part, key_condition, mt_settings, query_settings, granularity, log);

        projection_marks += pruned.getNumberOfMarks();
        projection_rows += granularity->getRowsCountInRanges(pruned);
    }

    result.estimated_marks = projection_marks;
    result.estimated_rows = projection_rows;
    auto marks_text = [](UInt64 marks) { return fmt::format("{} mark{}", marks, marks == 1 ? "" : "s"); };
    /// the walk cuts granules over the whole part while the writer restarts at every block it stores,
    /// which can cost a mark per part, so a decision that close to the base read is not a decision
    const UInt64 margin = adaptive_parts;
    /// fewer marks never loses, so the estimate decides only when both ends of its interval agree
    auto would_win = [&](UInt64 marks)
    { return marks < baseline_marks || (marks == baseline_marks && sort_help == SortOrderHelp::Helps); };
    const UInt64 fewest = projection_marks > margin ? projection_marks - margin : 0;
    if (uneven_width_parts != 0)
    {
        result.verdict = "too close to call";
        result.verdict_reason = fmt::format(
            "{} against {} from the base table, but the rows differ in width on {} of the {} parts read, so the granule "
            "layout depends on the blocks the writer was fed and the mark count is a model, not a measurement",
            marks_text(projection_marks),
            baseline_marks,
            uneven_width_parts,
            scanned_parts);
    }
    else if (would_win(fewest) != would_win(projection_marks + margin))
    {
        result.verdict = "too close to call";
        result.verdict_reason = fmt::format(
            "{} against {} from the base table, and the adaptive-granularity model can miss by up to {}",
            marks_text(projection_marks),
            baseline_marks,
            marks_text(margin));
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

    /// an ALTER can re-point an ALIAS the definition selects, so the columns the scan will really read
    /// are not the ones stored at CREATE time, and a denial here must not read as drift
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
    const QueryPlan::Node * plan_root,
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

    /// answer this before the refresh below: the read step of a baseline served by a projection
    /// carries that projection's metadata, which a base-table definition would not validate against
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

    /// both `TYPE commit_order` and its query form order by these, so name the surface instead of the type
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
    }

    /// the writer skips these on insert and only builds them on the first merge, so a fresh part has
    /// no projection part to read and the optimizer charges the parent's marks instead
    if (projection->with_block_number)
    {
        result.not_applicable_reason = fmt::format(
            "Projection stores {}, so it is built only when a part is merged, which EXPLAIN WHATIF does not estimate yet",
            backQuote(BlockNumberColumn::name));
        return result;
    }

    /// the scan reads what the projection stores, so a key over a virtual column has no source there
    for (const auto & required : proj_key.expression->getRequiredColumns())
    {
        if (!metadata->getColumns().hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, required))
        {
            result.not_applicable_reason = fmt::format(
                "Projection orders by {}, which it does not store, so EXPLAIN WHATIF cannot rebuild its key",
                backQuoteIfNeed(required));
            return result;
        }
    }

    const auto [outer_sorting, subtree_above_reading] = findOuterSorting(plan_root, read_step);
    SortOrderHelp sort_help = SortOrderHelp::NoOrderBy;
    if (outer_sorting)
    {
        if (!QueryPlanOptimizationSettings(context).read_in_order)
            sort_help = SortOrderHelp::ReadInOrderDisabled;
        else if (QueryPlanOptimizations::wouldReadInOrderBeUseful(*outer_sorting, proj_key, *subtree_above_reading))
            sort_help = SortOrderHelp::Helps;
        else
            sort_help = SortOrderHelp::NotUseful;
    }

    /// PK-range condition over the projection key, from the query predicate
    const auto & filter_dag = read_step->getFilterActionsDAG();
    std::optional<ActionsDAGWithInversionPushDown> predicate_dag;
    std::optional<KeyCondition> key_condition;
    if (filter_dag)
    {
        predicate_dag.emplace(filter_dag->getOutputs().front(), context, /* boolean_context */ true);
        key_condition.emplace(
            *predicate_dag, context, proj_key, /* single_point */ false, !context->getSettingsRef()[Setting::use_primary_key]);
        if (key_condition->alwaysUnknownOrTrue())
            key_condition.reset();
    }

    /// the same gate as `optimizeUseNormalProjections`: a filter has to exist or the order has to help,
    /// but a filter the projection key cannot prune still leaves a full projection scan worth measuring,
    /// which wins whenever the projection stores less per row than the table does
    if (!filter_dag && sort_help != SortOrderHelp::Helps)
    {
        result.not_applicable_reason = fmt::format("Query has no filter predicate, and {}", describe(sort_help));
        return result;
    }

    result.status = WhatIfCandidateResult::Applicable;

    if (settings.empirical)
    {
        if (tryEstimateProjection(
                result, *projection, key_condition ? &*key_condition : nullptr, sort_help, read_step, baseline_parts,
                analysis.selected_marks, context))
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
