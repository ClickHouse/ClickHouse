#include <Storages/MergeTree/WhatIfProjectionEstimator.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnSparse.h>
#include <Common/Stopwatch.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterHypotheticalObjectQuery.h>
#include <Interpreters/sortBlock.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/PartDirIntent.h>
#include <Storages/MergeTree/WhatIfSettings.h>
#include <Storages/ProjectionsDescription.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsBool optimize_use_projections;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
}

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
}

namespace
{

/// what the projection part built from one baseline part would look like
struct ProjectionPartData
{
    /// the projection's sorting-key columns, one row per projection row, in read order
    Block key_block;
    /// sorted order over key_block, the projection part stores its rows in this order
    IColumn::Permutation order;
    size_t rows = 0;
    /// uncompressed size the projection part would have, drives the granularity
    size_t bytes = 0;
};

/// a normal projection re-sorts every row, so the whole part is read; limits, quota and progress
/// are wired the same way the empirical index scan does it
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

    /// the query's speed limits apply here too, size is checked explicitly by the caller
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

/// read the parent part once and keep only the projection's key columns, so peak memory is one
/// copy of the key plus the permutation. Returns false when a read limit was hit
bool buildProjectionPart(
    ProjectionPartData & out,
    const ProjectionDescription & projection,
    const DataPartPtr & part,
    ReadFromMergeTree * read_step,
    const SizeLimits & read_limits,
    UInt64 & total_rows_read,
    UInt64 & total_bytes_read,
    const ContextPtr & context)
{
    const auto & proj_key = projection.metadata->getSortingKey();

    Pipe pipe = makeWholePartPipe(part, proj_key.expression->getRequiredColumns(), read_step, context);
    QueryPipeline pipeline(std::move(pipe));
    pipeline.setProcessListElement(context->getProcessListElement());
    pipeline.setProgressCallback(context->getProgressCallback());
    pipeline.setQuota(context->getQuota());
    /// account the scan to the query's own bucket under a KEYED BY normalized_query_hash quota
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
        if (!read_limits.check(
                total_rows_read, total_bytes_read, "rows or bytes to read", ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES))
            return false;

        /// the key expression and the sort need full columns, same as the real projection writer
        for (auto & column : block)
            column.column = recursiveRemoveSparse(column.column);
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
    for (size_t i = 0; i < key_columns.size(); ++i)
        out.key_block.insert({std::move(key_columns[i]), proj_key.data_types[i], proj_key.column_names[i]});

    /// bytes come from the parent's stored sizes of the projection's columns; compact parts report
    /// zero per column, then the whole part is an upper bound, which understates the benefit
    for (const auto & name : projection.required_columns)
        out.bytes += part->getColumnSize(name).data_uncompressed;
    if (out.bytes == 0)
        out.bytes = part->getBytesUncompressedOnDisk();

    return true;
}

/// build the projection part's primary index in memory and prune it with the engine's own PK-range
/// pruning. Nothing is written, the synthetic part carries only the index and its granularity
MarkRanges pruneSyntheticProjectionPart(
    ProjectionPartData & data,
    const ProjectionDescription & projection,
    const DataPartPtr & parent_part,
    const KeyCondition & key_condition,
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

    /// sorted order via one permutation, without rearranging the columns
    stableGetPermutation(data.key_block, sort_description, data.order);

    const size_t granule_rows = computeIndexGranularity(
        data.rows,
        data.bytes,
        mt_settings[MergeTreeSetting::index_granularity_bytes],
        mt_settings[MergeTreeSetting::index_granularity],
        /* blocks_are_granules */ false,
        parent_part->index_granularity_info.mark_type.adaptive);

    const size_t num_marks = (data.rows + granule_rows - 1) / granule_rows;
    const size_t last_mark_rows = data.rows - (num_marks - 1) * granule_rows;
    granularity_out
        = std::make_shared<MergeTreeIndexGranularityConstant>(granule_rows, last_mark_rows, num_marks, /* has_final_mark */ false);

    /// primary index = the key at the first row of every granule, in sorted order
    Columns index_columns;
    index_columns.reserve(data.key_block.columns());
    for (const auto & key_column : data.key_block)
    {
        auto index_column = key_column.column->cloneEmpty();
        for (size_t mark = 0; mark < num_marks; ++mark)
            index_column->insertFrom(*key_column.column, data.order[mark * granule_rows]);
        index_columns.push_back(std::move(index_column));
    }

    /// the builder only reads the parent's storage and settings, it does not mutate the parent
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
        synthetic_ranges, projection.metadata, key_condition, nullptr, nullptr, nullptr, nullptr, query_settings, log);
}

/// the optimizer builds projection candidates only from parts that survived the base analysis
/// (analyzeProjectionCandidate iterates parts_with_ranges), so the estimate does the same
bool tryEstimateProjection(
    WhatIfCandidateResult & result,
    const ProjectionDescription & projection,
    const KeyCondition & key_condition,
    ReadFromMergeTree * read_step,
    const RangesInDataParts & baseline_parts,
    UInt64 baseline_marks,
    const ContextPtr & context)
{
    const auto & data = read_step->getMergeTreeData();
    const auto & mt_settings = *data.getSettings();
    const auto & query_settings = context->getSettingsRef();

    /// the whole-part scan is not the normal read pipeline, so enforce the query's read limits
    const SizeLimits read_limits(
        query_settings[Setting::max_rows_to_read], query_settings[Setting::max_bytes_to_read], query_settings[Setting::read_overflow_mode]);
    UInt64 total_rows_read = 0;
    UInt64 total_bytes_read = 0;

    Stopwatch watch;
    auto log = getLogger("WhatIfProjectionEstimator");

    UInt64 projection_marks = 0;
    UInt64 projection_rows = 0;
    UInt64 projection_bytes = 0;
    UInt64 scanned_parts = 0;
    UInt64 scanned_marks = 0;

    for (const auto & part_with_ranges : baseline_parts)
    {
        const auto & part = part_with_ranges.data_part;
        const size_t part_marks = part->index_granularity->getMarksCountWithoutFinal();
        if (part_marks == 0)
            continue;

        ProjectionPartData part_data;
        if (!buildProjectionPart(part_data, projection, part, read_step, read_limits, total_rows_read, total_bytes_read, context))
        {
            result.empirical_unsupported_reason
                = "The projection scan hit the read limit of the query (max_rows_to_read / max_bytes_to_read)";
            return false;
        }

        ++scanned_parts;
        scanned_marks += part_marks;
        if (part_data.rows == 0)
            continue;

        MergeTreeIndexGranularityPtr granularity;
        MarkRanges pruned
            = pruneSyntheticProjectionPart(part_data, projection, part, key_condition, mt_settings, query_settings, granularity, log);

        const UInt64 rows_in_ranges = granularity->getRowsCountInRanges(pruned);
        projection_marks += pruned.getNumberOfMarks();
        projection_rows += rows_in_ranges;
        projection_bytes += static_cast<UInt64>(
            static_cast<double>(part_data.bytes) / static_cast<double>(part_data.rows) * static_cast<double>(rows_in_ranges));
    }

    result.estimated_marks = projection_marks;
    result.estimated_rows = projection_rows;
    result.estimated_bytes = projection_bytes;
    /// signed on purpose: a projection can read more marks than the base table, and that is the answer
    result.skip_ratio = baseline_marks > 0
        ? (static_cast<double>(baseline_marks) - static_cast<double>(projection_marks)) / static_cast<double>(baseline_marks)
        : 0.0;
    /// the optimizer switches only when the projection reads strictly fewer marks than the base
    /// read; every baseline part has the candidate, so there is no parent remainder to add
    result.would_be_chosen = baseline_marks > 0 && projection_marks < baseline_marks;
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
    /// the same ADD PROJECTION validation as CREATE ran, so a dropped column or a setting change
    /// since then becomes not_applicable instead of an exception
    try
    {
        checkHypotheticalProjectionIsAddable(data, metadata, stored.definition_ast, /* if_not_exists */ false, context);
        return ProjectionDescription::getProjectionFromAST(
            stored.definition_ast, metadata->getColumns(), &metadata->partition_key, context, LoadingStrictnessLevel::CREATE);
    }
    catch (const Exception &)
    {
        reason = "Hypothetical projection can no longer be added to this table: " + getCurrentExceptionMessage(false);
        return std::nullopt;
    }
}

WhatIfCandidateResult evaluateProjection(
    const ProjectionDescription & stored_projection,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & baseline_parts,
    const WhatIfSettings & settings,
    ContextPtr context)
{
    const auto & data = read_step->getMergeTreeData();

    WhatIfCandidateResult result;
    result.name = stored_projection.name;
    result.type = stored_projection.type == ProjectionDescription::Type::Aggregate ? "projection (aggregate)" : "projection (normal)";
    result.status = WhatIfCandidateResult::NotApplicable;
    result.total_parts = data.getActivePartsCount();
    result.total_marks = data.getTotalMarksCount();

    /// context already has the inner SELECT settings applied, so this matches a real read
    if (!context->getSettingsRef()[Setting::optimize_use_projections])
    {
        result.not_applicable_reason = "Projections are disabled by `optimize_use_projections = 0`";
        return result;
    }

    auto metadata = read_step->getStorageMetadata();
    auto projection = refreshHypotheticalProjection(stored_projection, data, metadata, context, result.not_applicable_reason);
    if (!projection)
        return result;

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

    if (analysis.readFromProjection() && !baseline_parts.empty())
    {
        result.not_applicable_reason = "The query is already served from projection '" + baseline_parts.front().data_part->name
            + "', EXPLAIN WHATIF estimates candidates against the base table read only";
        return result;
    }

    /// the engine's own preconditions for even considering a projection on this read
    if (!QueryPlanOptimizations::canUseProjectionForReadingStep(read_step))
    {
        result.not_applicable_reason = "The optimizer does not consider projections for this read (for example FINAL, SAMPLE, "
                                       "reading in order, pending mutations, or a parallel-replicas mode without projection support)";
        return result;
    }

    /// same coverage rule as optimizeUseNormalProjections: every column the read needs must be there
    for (const auto & column_name : read_step->getAllColumnNames())
    {
        if (!projection->sample_block.findColumnOrSubcolumnByName(column_name) && !projection->metadata->virtuals.has(column_name))
        {
            result.not_applicable_reason
                = fmt::format("Projection does not contain column {} required by the query", backQuoteIfNeed(column_name));
            return result;
        }
    }

    const auto & proj_key = projection->metadata->getSortingKey();
    if (proj_key.column_names.empty())
    {
        result.not_applicable_reason = "Projection has no sort key to prune on";
        return result;
    }

    /// CREATE did not need SELECT, the scan does, so check current grants now
    if (!projection->required_columns.empty())
        context->checkAccess(AccessType::SELECT, data.getStorageID(), projection->required_columns);

    const auto & filter_dag = read_step->getFilterActionsDAG();
    if (!filter_dag)
    {
        result.not_applicable_reason = "Query has no filter predicate";
        return result;
    }

    /// PK-range condition over the projection's own key, derived from the query predicate
    ActionsDAGWithInversionPushDown predicate_dag(filter_dag->getOutputs().front(), context, /* boolean_context */ true);
    KeyCondition key_condition(predicate_dag, context, proj_key.column_names, proj_key.expression);
    if (key_condition.alwaysUnknownOrTrue())
    {
        result.not_applicable_reason = "Projection sort key cannot filter this predicate (always unknown or true)";
        return result;
    }

    result.status = WhatIfCandidateResult::Applicable;

    if (settings.empirical)
    {
        if (tryEstimateProjection(result, *projection, key_condition, read_step, baseline_parts, analysis.selected_marks, context))
            return result;
        result.empirical_status = WhatIfCandidateResult::Unsupported;
    }
    else
    {
        result.empirical_status = WhatIfCandidateResult::Disabled;
    }

    result.estimate_source = WhatIfCandidateResult::ApplicabilityOnly;
    result.estimated_marks = analysis.selected_marks;
    result.skip_ratio = 0.0;
    return result;
}

}
