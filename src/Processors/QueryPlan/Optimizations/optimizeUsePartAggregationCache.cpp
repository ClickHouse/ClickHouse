#include <Processors/QueryPlan/Optimizations/optimizeUsePartAggregationCache.h>

#include <Interpreters/Cache/PartAggregationCache.h>
#include <Interpreters/Cache/PartAggregationCachePopulator.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/SipHash.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/PartAggregationCacheSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_part_aggregation_cache;
    extern const SettingsBool enable_reads_from_part_aggregation_cache;
    extern const SettingsBool enable_writes_to_part_aggregation_cache;
}

namespace QueryPlanOptimizations
{

static QueryPlan::Node * findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (typeid_cast<ReadFromMergeTree *>(step))
        return &node;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return findReadingStep(*node.children.front());

    return nullptr;
}

/// Collect all ExpressionStep/FilterStep actions between AggregatingStep and ReadFromMergeTree.
/// Returned in bottom-up order (ReadFromMergeTree → AggregatingStep).
static std::vector<IntermediateStepAction> collectIntermediateActions(QueryPlan::Node & node)
{
    std::vector<IntermediateStepAction> actions;
    QueryPlan::Node * current = &node;

    while (current)
    {
        IQueryPlanStep * step = current->step.get();

        if (typeid_cast<ReadFromMergeTree *>(step))
            break;

        if (auto * expr = typeid_cast<ExpressionStep *>(step))
            actions.push_back({std::make_shared<ExpressionActions>(expr->getExpression().clone()), {}, false});
        else if (auto * filter = typeid_cast<FilterStep *>(step))
            actions.push_back({std::make_shared<ExpressionActions>(filter->getExpression().clone()),
                filter->getFilterColumnName(), filter->removesFilterColumn()});

        if (current->children.size() != 1)
            break;
        current = current->children.front();
    }

    std::reverse(actions.begin(), actions.end());
    return actions;
}

static const ActionsDAG * findFilterDAG(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * filter = typeid_cast<FilterStep *>(step))
        return &filter->getExpression();
    if (node.children.size() == 1)
        return findFilterDAG(*node.children.front());
    return nullptr;
}

void optimizeUsePartAggregationCache(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes)
{
    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return;

    if (node.children.size() != 1)
        return;

    if (aggregating->isGroupingSets() || aggregating->inOrder())
        return;

    if (!aggregating->getFinal())
        return;

    if (!aggregating->canUseProjection())
        return;

    QueryPlan::Node * reading_node = findReadingStep(*node.children.front());
    if (!reading_node)
        return;

    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());
    if (!reading)
        return;

    auto context = reading->getContext();
    if (!context)
        return;

    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::allow_experimental_part_aggregation_cache])
        return;

    auto cache = context->getPartAggregationCache();
    if (!cache)
        return;

    const auto & parts = reading->getParts();
    if (parts.empty())
        return;

    const auto & params = aggregating->getParams();

    auto intermediate_actions = collectIntermediateActions(*node.children.front());

    /// If ReadFromMergeTree has a prewhere/where filter, convert it to an IntermediateStepAction
    /// so the populator applies it when reading data.
    auto prewhere = reading->getPrewhereInfo();
    const ActionsDAG * filter_dag_for_hash = nullptr;
    if (prewhere)
    {
        filter_dag_for_hash = &prewhere->prewhere_actions;
        intermediate_actions.insert(intermediate_actions.begin(), IntermediateStepAction{
            std::make_shared<ExpressionActions>(prewhere->prewhere_actions.clone()),
            prewhere->prewhere_column_name,
            prewhere->remove_prewhere_column});
    }

    IASTHash query_hash = PartAggregationCache::calculateQueryHash(
        params.keys, params.aggregates, filter_dag_for_hash);

    /// Include the full intermediate ExpressionStep/FilterStep action DAGs in the hash.
    /// Hashing only output names is not enough: two filters can share output column
    /// names while computing different predicates, which would alias incompatible
    /// queries to the same cache key and return cached states from a different query.
    if (!intermediate_actions.empty())
    {
        SipHash extra_hash;
        extra_hash.update(query_hash.low64);
        extra_hash.update(query_hash.high64);
        for (const auto & action : intermediate_actions)
        {
            action.actions->getActionsDAG().updateHash(extra_hash);
            extra_hash.update(action.filter_column_name);
            extra_hash.update(action.remove_filter_column);
        }
        query_hash = getSipHash128AsPair(extra_hash);
    }

    auto storage_id = reading->getMergeTreeData().getStorageID();
    String table_id = storage_id.hasUUID() ? toString(storage_id.uuid) : storage_id.getFullTableName();

    bool enable_reads = settings[Setting::enable_reads_from_part_aggregation_cache];

    RangesInDataParts uncached_parts;
    std::vector<PartAggregationCache::EntryPtr> cached_entries;

    for (const auto & part : parts)
    {
        PartAggregationCache::Key key{query_hash, table_id, part.data_part->name};
        auto entry = enable_reads ? cache->get(key) : nullptr;

        if (entry)
            cached_entries.push_back(std::move(entry));
        else
            uncached_parts.push_back(part);
    }

    bool enable_writes = settings[Setting::enable_writes_to_part_aggregation_cache];

    /// Populate cache for uncached parts (both cold and partially warm cache).
    if (enable_writes && !uncached_parts.empty())
    {
        const auto & aggregator_header = *aggregating->getInputHeaders().front();

        populatePartAggregationCache(
            cache, query_hash, table_id, uncached_parts, params,
            aggregator_header,
            reading->getMergeTreeData(),
            reading->getStorageSnapshot(),
            context,
            intermediate_actions);

        /// Re-check: move newly cached parts from uncached to cached.
        RangesInDataParts still_uncached;
        for (const auto & part : uncached_parts)
        {
            PartAggregationCache::Key key{query_hash, table_id, part.data_part->name};
            auto entry = enable_reads ? cache->get(key) : nullptr;
            if (entry)
                cached_entries.push_back(std::move(entry));
            else
                still_uncached.push_back(part);
        }
        uncached_parts = std::move(still_uncached);
    }

    if (cached_entries.empty())
        return;

    auto intermediate_header = std::make_shared<Block>(
        Aggregator::Params::getHeader(
            *reading->getOutputHeader(), params.only_merge, params.keys, params.aggregates, /* final = */ false));

    Pipe cached_pipe(std::make_shared<PartAggregationCacheSource>(
        *intermediate_header, std::move(cached_entries)));

    if (uncached_parts.empty())
    {
        auto & cached_source_node = nodes.emplace_back();
        cached_source_node.step = std::make_unique<ReadFromPreparedSource>(std::move(cached_pipe));
        cached_source_node.children = {};

        node.children.front() = &cached_source_node;
        aggregating->requestOnlyMergeForAggregateProjection(intermediate_header);
    }
    else
    {
        auto & cached_source_node = nodes.emplace_back();
        cached_source_node.step = std::make_unique<ReadFromPreparedSource>(std::move(cached_pipe));
        cached_source_node.children = {};

        auto analyzed = reading->getAnalyzedResult();
        if (!analyzed)
            return;
        auto new_result = std::make_shared<ReadFromMergeTree::AnalysisResult>(*analyzed);
        new_result->parts_with_ranges = std::move(uncached_parts);
        reading->setAnalyzedResult(std::move(new_result));

        auto projection_step = aggregating->convertToAggregatingProjection(intermediate_header);
        node.step = std::move(projection_step);
        node.children = {node.children.front(), &cached_source_node};
    }
}

}
}
