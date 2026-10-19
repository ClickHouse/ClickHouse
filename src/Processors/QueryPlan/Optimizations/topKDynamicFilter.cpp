#include <Processors/QueryPlan/Optimizations/topKDynamicFilter.h>

#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionTopKFilter.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/TopKThresholdTracker.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

namespace DB::QueryPlanOptimizations
{

std::optional<String> findPassedThroughInput(const ActionsDAG & dag, const String & output_name)
{
    const auto * node = dag.tryFindInOutputs(output_name);
    if (!node)
        return std::nullopt;

    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();

    if (node->type != ActionsDAG::ActionType::INPUT)
        return std::nullopt;

    return node->result_name;
}

TopKThresholdTrackerPtr tryAttachTopKDynamicFilter(
    QueryPlan::Node * node,
    const TopKDynamicFilterConsumer & consumer,
    const Optimization::ExtraSettings & settings,
    QueryPlan::Nodes & nodes,
    const LoggerPtr & log)
{
    const int direction = consumer.directions.front();
    const int nulls_direction = consumer.nulls_directions.front();

    /// The name of the key column at the current step; it changes on the way down, as the steps
    /// may alias the column (`__table1.a` at the consumer is `a` at the read).
    String key_name = consumer.column_name;

    /// Descend through the steps that pass the key column through without computing anything
    /// over it, down to the reading step.
    FilterStep * closest_filter_step = nullptr;
    ReadFromMergeTree * read_step = nullptr;
    /// A filter that is deterministic only within one query (a join runtime filter `__applyFilter` pushed below
    /// the consumer, `now`) changes which rows reach the consumer, and so the boundary, but its contents are not
    /// part of the query condition cache key.
    bool has_non_deterministic_filter = false;
    /// The expressions between the read and the consumer, for the query condition cache salt below.
    std::vector<const ActionsDAG *> path_expressions;

    while (!read_step)
    {
        if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
        {
            /// The filter shrinks the blocks this step sees, so an expression that depends on its block
            /// (`rowNumberInBlock`, `runningDifference`, `rand`) could change the result.
            if (dependsOnItsBlock(expression_step->getExpression()))
            {
                LOG_TRACE(log, "No dynamic filter: the expression step depends on its block");
                return nullptr;
            }

            auto input_name = findPassedThroughInput(expression_step->getExpression(), key_name);
            if (!input_name)
            {
                LOG_TRACE(log, "No dynamic filter: the expression step does not pass the key {} through", key_name);
                return nullptr;
            }
            key_name = std::move(*input_name);
            path_expressions.push_back(&expression_step->getExpression());
        }
        else if (auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
        {
            /// The filter shrinks the blocks this step sees, so an expression that depends on its block
            /// (`rowNumberInBlock`, `runningDifference`, `rand`) could change the result.
            if (dependsOnItsBlock(filter_step->getExpression()))
            {
                LOG_TRACE(log, "No dynamic filter: the filter step depends on its block");
                return nullptr;
            }

            auto input_name = findPassedThroughInput(filter_step->getExpression(), key_name);
            if (!input_name)
            {
                LOG_TRACE(log, "No dynamic filter: the filter step does not pass the key {} through", key_name);
                return nullptr;
            }
            key_name = std::move(*input_name);
            closest_filter_step = filter_step;
            path_expressions.push_back(&filter_step->getExpression());

            const auto * filter_node = filter_step->getExpression().tryFindInOutputs(filter_step->getFilterColumnName());
            if (!filter_node || !VirtualColumnUtils::isDeterministicAllowingTopKFilter(filter_node))
                has_non_deterministic_filter = true;
        }
        else if (auto * read = typeid_cast<ReadFromMergeTree *>(node->step.get()))
        {
            read_step = read;
            break;
        }
        else
        {
            LOG_TRACE(log, "No dynamic filter: step {} between the consumer and the reading step", node->step->getName());
            return nullptr;
        }

        if (node->children.size() != 1)
            return nullptr;
        node = node->children.front();
    }

    /// FINAL deduplicates rows by the sorting key while reading; dropping a row before that can change which
    /// version of another row survives. Parallel replicas run the query text remotely, where nothing knows the tracker.
    if (read_step->isQueryWithFinal() || read_step->isParallelReadingFromReplicas())
    {
        LOG_TRACE(log, "No dynamic filter: the read is FINAL or from parallel replicas");
        return nullptr;
    }

    /// The read already filters by the boundary of another top-K.
    if (read_step->getTopKFilterInfo())
    {
        LOG_TRACE(log, "No dynamic filter: the read already has a top-K filter");
        return nullptr;
    }

    const auto & read_columns = read_step->getAllColumnNames();
    const auto & header = *read_step->getOutputHeader();

    /// A physical column of the table, not a virtual one: `__topKFilter` runs as a PREWHERE over the stored column.
    if (std::find(read_columns.begin(), read_columns.end(), key_name) == read_columns.end() || !header.has(key_name)
        || !read_step->getStorageMetadata()->getColumns().hasPhysical(key_name))
    {
        LOG_TRACE(log, "No dynamic filter: the key {} is not a physical column read from the table", key_name);
        return nullptr;
    }

    const auto & key_column = header.getByName(key_name);

    /// The same type restrictions as `tryOptimizeTopK`: `__topKFilter` cannot compare `Dynamic`, `Variant`
    /// and empty tuples, and comparing variable-length values row by row may cost more than it saves.
    const auto * key_tuple_type = typeid_cast<const DataTypeTuple *>(key_column.type.get());
    if (isDynamic(key_column.type) || isVariant(key_column.type) || (key_tuple_type && key_tuple_type->getElements().empty())
        || (!key_column.type->haveMaximumSizeOfValue() && !settings.use_top_k_dynamic_filtering_for_variable_length_types))
    {
        LOG_TRACE(log, "No dynamic filter: the key {} has type {}", key_name, key_column.type->getName());
        return nullptr;
    }

    SortColumnDescription key_sort_description(key_name, direction, nulls_direction);
    auto threshold_tracker = createTopKThresholdTracker(key_sort_description, *key_column.type);

    /// `where_clause = true` keeps `MergeTreeDataSelectExecutor` from narrowing the read up front to the granules
    /// that hold the `LIMIT` smallest rows (`getTopKMarks`): `LIMIT` rows may hold fewer than `LIMIT` distinct keys,
    /// and a group needs all of its rows. Only the boundary-driven filtering and granule skipping apply here.
    TopKFilterInfo info{
        key_name, key_column.type, consumer.num_ranked_columns, consumer.limit, direction, /*where_clause=*/ true, threshold_tracker,
        /*condition_hash=*/ 0};

    /// Built the way `installTopKDynamicFilter` builds it: the existing PREWHERE must not depend on its block, and
    /// `__topKFilter` must bind to the column the read produces, not to a computed node of the PREWHERE under its name.
    auto initial_header = read_step->getOutputHeader();
    auto new_prewhere_info = buildTopKDynamicFilterPrewhere(read_step->getPrewhereInfo(), info);
    if (!new_prewhere_info || initial_header->has(new_prewhere_info->prewhere_column_name))
    {
        LOG_TRACE(log, "No dynamic filter: the filter over {} cannot be combined with the PREWHERE of the read", key_name);
        return nullptr;
    }

    read_step->updatePrewhereInfo(new_prewhere_info);
    auto updated_header = read_step->getOutputHeader();

    if (!blocksHaveEqualStructure(*initial_header, *updated_header))
    {
        auto dag = ActionsDAG::makeConvertingActions(
            updated_header->getColumnsWithTypeAndName(),
            initial_header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            read_step->getContext());

        auto converting_step = std::make_unique<ExpressionStep>(updated_header, std::move(dag));
        auto & converting_node = nodes.emplace_back();
        converting_node.step = std::move(converting_step);

        /// The reading node takes the converting step and moves its own step into the new child node,
        /// so the parent does not have to be touched.
        node->children.push_back(&converting_node);
        std::swap(node->step, converting_node.step);
    }

    /// Salts the query condition cache key the way `tryOptimizeTopK` does, with the mark of the consumer. The boundary
    /// on the first ranked column depends on more than the column, so the consumer adds what else determines it, and
    /// the name of a column computed between the read and the consumer does not always identify its expression (a
    /// column of a subquery), so the salt also includes every expression between the read and the consumer.
    SipHash hash;
    hash.update(consumer.salt_mark);
    hash.update(info.column_name);
    const String type_name = info.data_type->getName();
    hash.update(type_name);
    hash.update(info.num_sort_columns);
    hash.update(info.limit_n);

    if (consumer.add_salt)
        consumer.add_salt(hash);

    hash.update(path_expressions.size());
    for (const auto * expression : path_expressions)
        expression->updateHash(hash);

    hash.update(consumer.directions.size());
    for (size_t i = 0; i < consumer.directions.size(); ++i)
    {
        hash.update(consumer.directions[i]);
        hash.update(consumer.nulls_directions[i]);
    }
    info.condition_hash = hash.get64();

    read_step->setTopKColumn(info);

    /// This pass runs after `disableTopKQueryConditionCacheUnderNonDeterministicFilters`, so it applies the same
    /// protection itself: under such a filter the boundary-dependent PREWHERE entries must be neither reused nor written.
    if (has_non_deterministic_filter)
        read_step->disableTopKPrewhereQueryConditionCache();

    /// `updateQueryConditionCache` tagged the WHERE filter with the hash of the plain predicate. Under the running
    /// filter the granules it sees are only those the boundary let through, so the entry must be salted with the
    /// top-K parameters (the same way `updateQueryConditionCache` salts a read stamped by `tryOptimizeTopK`), or
    /// dropped when the cache is not to be used for top-K reads at all or a filter above the read is not deterministic.
    if (closest_filter_step && closest_filter_step->hasConditionForQueryConditionCache())
    {
        if (settings.use_query_condition_cache_for_top_k && !has_non_deterministic_filter)
            closest_filter_step->saltConditionForQueryConditionCache(read_step->getTopKFilterInfo()->condition_hash);
        else
            closest_filter_step->resetConditionForQueryConditionCache();
    }

    LOG_TRACE(log, "Filtering and skipping granules of {} by the top-K boundary", key_name);
    return threshold_tracker;
}

}
