#include <Storages/MergeTree/RuntimeFilterReadRangesRefiner.h>

#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>

namespace DB
{

RuntimeFilterReadRangesRefiner::RuntimeFilterReadRangesRefiner(
    StorageMetadataPtr metadata_snapshot_, ContextPtr context_, std::vector<RuntimeFilterIndexAnalysisDescriptor> descriptors_)
    : metadata_snapshot(std::move(metadata_snapshot_))
    , context(std::move(context_))
    , descriptors(std::move(descriptors_))
{
}

RuntimeFilterReadRangesRefiner::~RuntimeFilterReadRangesRefiner() = default;

void RuntimeFilterReadRangesRefiner::setFilter(const RuntimeFilterConstPtr & filter)
{
    std::call_once(set_filter_once, [&] { setFilterImpl(filter); });
}

void RuntimeFilterReadRangesRefiner::setFilterImpl(const RuntimeFilterConstPtr & filter)
{
    /// The seal payload is the leading filter; it only triggers the refinement. The predicate
    /// is built from ALL the descriptors (the primary key prefix covered by the gating join's
    /// filters), which are complete by now: their build transforms are upstream of the seal
    /// emitter. The shared builder makes `key IN (exact values)` else `key BETWEEN [min, max]`
    /// per filter, ANDed; a filter which recorded nothing usable is skipped (fail-open).
    chassert(filter && filter->isReady());

    ActionsDAG dag;
    const auto * predicate = buildRuntimeRangePredicate(*context->getRuntimeFilterLookup(), descriptors, dag, context);
    if (!predicate)
        return;
    dag.getOutputs() = {predicate};

    /// The KeyDescription overload: the condition must honor the per-column sort directions
    /// of the primary key, or a reverse-sorted key would be analyzed as ascending and the
    /// refinement would prune wrong marks.
    ActionsDAGWithInversionPushDown inverted_dag(predicate, context, /*boolean_context=*/true);
    condition = std::make_shared<const KeyCondition>(inverted_dag, context, metadata_snapshot->getPrimaryKey());
}

MarkRanges RuntimeFilterReadRangesRefiner::refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const
{
    if (!condition)
        return ranges;

    RangesInDataPart part(info.data_part_info->getDataPart(), info.parent_part, info.part_index_in_query, info.part_starting_offset_in_query);
    part.ranges = std::move(ranges);

    return MergeTreeDataSelectExecutor::markRangesFromPKRange(
        part,
        metadata_snapshot,
        *condition,
        /*part_offset_condition=*/nullptr,
        /*total_offset_condition=*/nullptr,
        /*exact_ranges=*/nullptr,
        /*pk_to_minmax_slot=*/nullptr,
        context->getSettingsRef(),
        getLogger("RuntimeFilterReadRangesRefiner"));
}

}
