#include <Storages/MergeTree/ScoredSearch/ScoredSearchUtils.h>

#include <algorithm>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Analyzer/TableExpressionModifiers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/HashTable/HashSet.h>
#include <Processors/LimitTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/ScoredSearch/IScorer.h>
#include <Storages/MergeTree/ScoredSearch/ScorerSource.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/VirtualColumnUtils.h>

namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
    extern const SettingsNonZeroUInt64 max_block_size;
}

QueryPlan buildBitmapSubquery(
    const MergeTreeData & source_merge_tree,
    RangesInDataPartsPtr ranges,
    MergeTreeData::MutationsSnapshotPtr mutations,
    ActionsDAG where_clause,
    const String & filter_column_name,
    const StorageSnapshotPtr & source_snapshot,
    const SelectQueryInfo & outer_query_info,
    ContextPtr context)
{
    QueryPlan subquery;

    SelectQueryInfo query_info;
    query_info.filter_actions_dag = std::make_shared<const ActionsDAG>(where_clause.clone());
    query_info.table_expression_modifiers = TableExpressionModifiers{};
    query_info.storage_limits = outer_query_info.storage_limits;

    Names subquery_column_names = where_clause.getRequiredColumnsNames();

    if (std::ranges::find(subquery_column_names, "_part_index") == subquery_column_names.end())
        subquery_column_names.push_back("_part_index");

    if (std::ranges::find(subquery_column_names, "_part_offset") == subquery_column_names.end())
        subquery_column_names.push_back("_part_offset");

    /// `readFromParts` reads the pinned `ranges` rather than a fresh part
    /// snapshot, preserving the part-index invariant with the scorer.
    MergeTreeDataSelectExecutor executor(source_merge_tree);
    auto read_step = executor.readFromParts(
        ranges,
        std::move(mutations),
        subquery_column_names,
        source_snapshot,
        query_info,
        context,
        context->getSettingsRef()[Setting::max_block_size],
        context->getSettingsRef()[Setting::max_threads]);

    if (!read_step)
        return subquery;

    subquery.addStep(std::move(read_step));

    /// Add explicit FilterStep to apply predicates that were not pushed to PREWHERE.
    auto filter_step = std::make_unique<FilterStep>(
        subquery.getCurrentHeader(),
        std::move(where_clause),
        filter_column_name,
        /*remove_filter_column=*/ true);

    filter_step->setStepDescription("WHERE (bitmap subquery)");
    subquery.addStep(std::move(filter_step));
    return subquery;
}


void buildScoredTopKPipeline(
    RangesInDataParts ranges_in_data_parts,
    std::shared_ptr<RowScorer> scorer,
    const std::optional<PerPartBitmaps> & bitmaps,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot,
    StorageMetadataPtr source_metadata,
    SharedHeader output_header,
    const SelectQueryInfo & query_info,
    size_t num_streams,
    ContextPtr context,
    QueryPipelineBuilder & pipeline)
{
    scorer->validate(context);

    if (ranges_in_data_parts.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(output_header)));
        return;
    }

    auto shared_parts = std::make_shared<ScorerSharedParts>();
    shared_parts->parts = std::move(ranges_in_data_parts);
    shared_parts->bitmaps = bitmaps;

    /// One worker per stream is enough: each part is claimed by exactly one
    /// worker, so more workers than parts would sit idle.
    const size_t num_workers = std::max<size_t>(1, std::min(num_streams, shared_parts->parts.size()));

    Pipes pipes;
    pipes.reserve(num_workers);

    for (size_t i = 0; i < num_workers; ++i)
    {
        pipes.emplace_back(std::make_shared<ScorerSource>(
            output_header,
            scorer,
            shared_parts,
            mutations_snapshot,
            source_metadata,
            context));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(query_info.storage_limits);

    pipeline.init(std::move(pipe));

    /// Sort by `_score` in the scorer's direction.
    /// Then by `__global_row_index` ASC as a deterministic tie-breaker for equal scores.
    SortDescription sort_description;
    const ScoreDirection direction = scorer->getSortDirection();

    sort_description.emplace_back(
        /*column_name=*/ "_score",
        /*direction=*/ direction == ScoreDirection::Ascending ? 1 : -1,
        /*nulls_direction=*/ 1);

    sort_description.emplace_back(
        /*column_name=*/ "__global_row_index",
        /*direction=*/ 1,
        /*nulls_direction=*/ 1);

    /// Each `ScorerSource` emits pre-sorted chunks, so no partial sort is inserted.
    /// `MergingSortedTransform` fans the N streams into a single output.
    pipeline.addTransform(std::make_shared<MergingSortedTransform>(
        pipeline.getSharedHeader(),
        pipeline.getNumStreams(),
        sort_description,
        context->getSettingsRef()[Setting::max_block_size],
        /*max_block_size_bytes=*/ 0,
        /*max_dynamic_subcolumns_=*/ std::nullopt,
        SortingQueueStrategy::Batch,
        scorer->getTopK()));

    /// `LimitTransform` applies the global top-K cap.
    pipeline.addTransform(std::make_shared<LimitTransform>(pipeline.getSharedHeader(), scorer->getTopK(), 0));
}

}
