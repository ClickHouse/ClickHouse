#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Access/Common/QuotaDefs.h>
#include <Access/EnabledQuota.h>
#include <Access/QuotaUsage.h>
#include <Core/Settings.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopK.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool rows_before_aggregation;
}

namespace QueryPlanOptimizations
{

namespace
{

/// Whether every evaluation of the expressions gives the same values: a pass evaluates them again over other
/// granules and blocks. Of the sets of `IN`, also those in lambdas, only a set of literals is part of the expressions:
/// a set of a table changes with the inserts into it, and a set of a subquery is built by a pipeline of the query
/// outside the passes.
bool isRepeatable(const ActionsDAG & dag)
{
    if (isSensitiveToEvaluationCount(dag) || dag.hasArrayJoin())
        return false;
    bool only_literal_sets = true;
    forEachFutureSet(dag, [&](const FutureSetPtr & set)
        { only_literal_sets &= typeid_cast<const FutureSetFromTuple *>(set.get()) != nullptr; });
    return only_literal_sets;
}

/// Whether reading the same ranges again yields the same rows: no filter of the read is random or stateful, no
/// mutation applies on the fly, and no column is computed from its default in a part that lacks it.
bool readIsRepeatable(const ReadFromMergeTree & read)
{
    if (const auto & mutations = read.getMutationsSnapshot();
        mutations && (mutations->hasDataMutations() || mutations->hasMetadataMutations()))
        return false;
    if (const auto & prewhere = read.getPrewhereInfo(); prewhere && !isRepeatable(prewhere->prewhere_actions))
        return false;
    if (const auto & filter = read.getRowLevelFilter(); filter && !isRepeatable(filter->actions))
        return false;

    const auto & columns = read.getStorageMetadata()->getColumns();
    for (const auto & name : read.getAllColumnNames())
    {
        if (!columns.getDefault(name))
            continue;
        for (const auto & part : read.getAnalyzedResult()->parts_with_ranges)
            if (!part.data_part->tryGetColumn(name))
                return false;
    }
    return true;
}

/// Whether the read is a plain one of the parts it selected, which a copy reads the same way. Under the aggregation of
/// a top-K (see `findAggregationTopK`), a read is never in order, serves no vector search or top-K filter, and does not
/// read for parallel replicas, whose aggregations are partial.
bool isPlainRead(const ReadFromMergeTree & read)
{
    if (read.isQueryWithFinal() || read.isQueryWithSampling() || !read.getIndexReadTasks().empty())
        return false;

    /// A `STREAM` read selects its parts while reading, a masking policy hides the values that the primary index
    /// holds, and the delete bitmap of a unique key lies outside the parts that the passes share.
    return !read.getQueryInfo().isStream() && !read.getMergeTreeData().hasEnabledMaskingPolicies(read.getContext())
        && !read.getStorageMetadata()->hasUniqueKey();
}

/// Whether nothing measures or limits the volume that the read reads, which the passes increase.
bool readVolumeIsUnlimited(const ReadFromMergeTree & read)
{
    if (const auto & storage_limits = read.getQueryInfo().storage_limits)
    {
        for (const auto & limits : *storage_limits)
        {
            const auto & local = limits.local_limits;
            const auto & speed = local.speed_limits;
            if (local.size_limits.hasLimits() || limits.leaf_limits.hasLimits() || local.timeout_overflow_mode == OverflowMode::BREAK
                || speed.min_execution_rps || speed.max_execution_rps || speed.min_execution_bps || speed.max_execution_bps
                || speed.max_estimated_execution_time.totalMicroseconds() != 0)
                return false;
        }
    }
    if (const auto quota = read.getContext()->getQuota())
        for (const auto & usage : quota->getAllUsage())
            for (const auto & interval : usage.intervals)
                if (interval.max[static_cast<size_t>(QuotaType::READ_ROWS)] || interval.max[static_cast<size_t>(QuotaType::READ_BYTES)])
                    return false;
    return true;
}

}

void tryEnableMultiPassTopKAggregation(QueryPlan::Node & node)
{
    /// A top-K of the groups of a final aggregation by keys (see `findAggregationTopK`), by a lone `count`, `uniqExact`
    /// or `uniqExactIf`, which never exceed the rows of a group (see `isRowBoundedCount`). The candidate pass needs the
    /// descending order: only it lets the rows bound the other keys. A limit of the groups would count fewer groups in
    /// the passes than in the original aggregation, and in a merge of aggregate states a row stands for several rows of
    /// the table.
    const auto top_k = findAggregationTopK(node);
    if (!top_k || top_k->ascending)
        return;
    auto & aggregating = *top_k->aggregating;
    const auto & params = aggregating.getParams();
    if (!isRowBoundedCount(params.aggregates[top_k->rank_index]) || params.max_rows_to_group_by || params.only_merge)
        return;

    /// Every pass reads the input again, so the input must be a read of a `MergeTree` table under expressions and
    /// filters that give the same rows every time.
    std::vector<const IQueryPlanStep *> input_steps;
    QueryPlan::Node * child = top_k->aggregating_node->children.front();
    while (const auto * expression = expressionOfInputStep(*child->step))
    {
        if (!isRepeatable(*expression))
            return;
        input_steps.push_back(child->step.get());
        child = child->children.front();
    }
    std::ranges::reverse(input_steps);

    auto * read = typeid_cast<ReadFromMergeTree *>(child->step.get());
    if (!read || !isPlainRead(*read) || !readVolumeIsUnlimited(*read))
        return;

    /// The passes aggregate other rows than the original aggregation, which the count of the rows before the
    /// aggregation would include.
    if (read->getContext()->getSettingsRef()[Setting::rows_before_aggregation])
        return;

    /// The passes read the ranges of this analysis.
    if (!read->getAnalyzedResult())
        read->setAnalyzedResult(read->selectRangesToRead());
    const auto & analysis = read->getAnalyzedResult();
    if (!analysis->isUsable() || analysis->readFromProjection() || !readIsRepeatable(*read))
        return;

    if (auto input = makeMultiPassTopKInput(aggregating, *read, input_steps, top_k->limit, top_k->rank_index))
        aggregating.enableMultiPassTopK(std::move(input));
}

}

}
