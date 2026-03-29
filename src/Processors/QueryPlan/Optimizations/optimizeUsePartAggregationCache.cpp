#include <Processors/QueryPlan/Optimizations/optimizeUsePartAggregationCache.h>

#include <Interpreters/Cache/PartAggregationCache.h>
#include <Interpreters/Cache/PartAggregationCachePopulator.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/SelectQueryInfo.h>
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
    extern const SettingsBool use_part_aggregation_cache;
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
            actions.push_back({std::make_shared<ExpressionActions>(expr->getExpression().clone()), {}});
        else if (auto * filter = typeid_cast<FilterStep *>(step))
            actions.push_back({std::make_shared<ExpressionActions>(filter->getExpression().clone()), filter->getFilterColumnName()});

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
    if (!settings[Setting::use_part_aggregation_cache])
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
            prewhere->prewhere_column_name});
    }

    IASTHash query_hash = PartAggregationCache::calculateQueryHash(
        params.keys, params.aggregates, filter_dag_for_hash);

    bool enable_reads = settings[Setting::enable_reads_from_part_aggregation_cache];

    RangesInDataParts uncached_parts;
    std::vector<PartAggregationCache::EntryPtr> cached_entries;

    for (const auto & part : parts)
    {
        PartAggregationCache::Key key{query_hash, part.data_part->name};
        auto entry = enable_reads ? cache->get(key) : nullptr;

        if (entry)
            cached_entries.push_back(std::move(entry));
        else
            uncached_parts.push_back(part);
    }

    bool enable_writes = settings[Setting::enable_writes_to_part_aggregation_cache];

    if (cached_entries.empty() && enable_writes)
    {
        const auto & aggregator_header = *aggregating->getInputHeaders().front();

        populatePartAggregationCache(
            cache, query_hash, parts, params,
            aggregator_header,
            reading->getMergeTreeData(),
            reading->getStorageSnapshot(),
            context,
            intermediate_actions);

        uncached_parts.clear();
        for (const auto & part : parts)
        {
            PartAggregationCache::Key key{query_hash, part.data_part->name};
            auto entry = cache->get(key);
            if (entry)
                cached_entries.push_back(std::move(entry));
            else
                uncached_parts.push_back(part);
        }
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
