#include <Processors/QueryPlan/Optimizations/optimizeUsePartAggregationCache.h>

#include <Interpreters/Cache/PartAggregationCache.h>
#include <Interpreters/Cache/PartAggregationCachePopulator.h>
#include <Interpreters/Context.h>
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

/// Walk down through Expression/Filter steps to find ReadFromMergeTree.
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

/// Collect filter DAGs between AggregatingStep and ReadFromMergeTree.
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
    /// Pattern: AggregatingStep -> [Expression/Filter]* -> ReadFromMergeTree
    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return;

    if (node.children.size() != 1)
        return;

    /// Don't apply to grouping sets or aggregation-in-order.
    if (aggregating->isGroupingSets() || aggregating->inOrder())
        return;

    /// Only works with final aggregation.
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

    /// Get context from the reading step.
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

    /// Compute query hash from GROUP BY keys, aggregate functions, and filter.
    const ActionsDAG * filter_dag = findFilterDAG(*node.children.front());
    IASTHash query_hash = PartAggregationCache::calculateQueryHash(
        params.keys, params.aggregates, filter_dag);

    bool enable_reads = settings[Setting::enable_reads_from_part_aggregation_cache];

    /// Partition parts into cached and uncached.
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

    /// If nothing is cached but writes are enabled, eagerly populate the cache
    /// by reading each part and aggregating per-part.
    if (cached_entries.empty() && enable_writes)
    {
        populatePartAggregationCache(
            cache, query_hash, parts, params,
            *reading->getOutputHeader(),
            reading->getMergeTreeData(),
            reading->getStorageSnapshot(),
            context);

        /// Re-check cache after population. All parts should now be cached.
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

    /// If still nothing is cached (population failed?), do not modify the pipeline.
    if (cached_entries.empty())
        return;

    /// Get the intermediate aggregation header (with AggregateFunction columns, final=false).
    auto intermediate_header = std::make_shared<Block>(
        Aggregator::Params::getHeader(
            *reading->getOutputHeader(), params.only_merge, params.keys, params.aggregates, /* final = */ false));

    /// Create a source for cached blocks.
    Pipe cached_pipe(std::make_shared<PartAggregationCacheSource>(
        *intermediate_header, std::move(cached_entries)));

    if (uncached_parts.empty())
    {
        /// All parts are cached. Replace the entire subtree with:
        /// ReadFromPreparedSource (cached) -> AggregatingStep (only_merge, final=true)
        auto & cached_source_node = nodes.emplace_back();
        cached_source_node.step = std::make_unique<ReadFromPreparedSource>(std::move(cached_pipe));
        cached_source_node.children = {};

        /// Replace the child of the aggregating node.
        node.children.front() = &cached_source_node;

        /// Tell the AggregatingStep to only merge (not aggregate from scratch).
        aggregating->requestOnlyMergeForAggregateProjection(intermediate_header);
    }
    else
    {
        /// Some parts cached, some not. Create a dual pipeline like AggregatingProjectionStep.
        /// Pipeline 1 (normal): ReadFromMergeTree (uncached parts) -> normal aggregation
        /// Pipeline 2 (cached): ReadFromPreparedSource -> merge-only aggregation
        /// Both merge into shared ManyAggregatedData -> finalize.
        auto & cached_source_node = nodes.emplace_back();
        cached_source_node.step = std::make_unique<ReadFromPreparedSource>(std::move(cached_pipe));
        cached_source_node.children = {};

        /// Update ReadFromMergeTree to only read uncached parts.
        auto analyzed = reading->getAnalyzedResult();
        if (!analyzed)
            return;
        auto new_result = std::make_shared<ReadFromMergeTree::AnalysisResult>(*analyzed);
        new_result->parts_with_ranges = std::move(uncached_parts);
        reading->setAnalyzedResult(std::move(new_result));

        /// Convert AggregatingStep to AggregatingProjectionStep (dual pipeline).
        auto projection_step = aggregating->convertToAggregatingProjection(intermediate_header);

        /// Replace the current node's step.
        node.step = std::move(projection_step);

        /// The AggregatingProjectionStep expects two children:
        /// [0] = normal parts pipeline (uncached), [1] = projection pipeline (cached)
        node.children = {node.children.front(), &cached_source_node};
    }
}

}
}
