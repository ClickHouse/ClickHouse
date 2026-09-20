#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionTopKFilter.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/optimizePrewhere.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/TopKThresholdTracker.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

namespace DB::QueryPlanOptimizations
{

/// Returns the AggregatingStep if it is eligible for the top-K heap optimization.
static AggregatingStep * validateAggregatingStep(QueryPlan::Node * node)
{
    auto * aggregating_step = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating_step)
        return nullptr;

    if (aggregating_step->isGroupingSets())
        return nullptr;

    if (aggregating_step->inOrder())
        return nullptr;

    const auto & params = aggregating_step->getParams();

    if (params.top_k)
        return nullptr;

    if (params.only_merge)
        return nullptr;

    if (params.overflow_row)
        return nullptr;

    if (params.max_rows_to_group_by > 0)
        return nullptr;

    if (params.keys.empty())
        return nullptr;

    return aggregating_step;
}


/// Links the top-K heap of the aggregation to the `ReadFromMergeTree` step below it.
///
/// The heap admits only keys not worse than its boundary once it holds `LIMIT` of them, so a row whose
/// first ranked key column is beyond the boundary can never reach the result. When that column is a
/// table column that travels to the aggregation untouched, the reading step can act on the boundary
/// too, as `ORDER BY column LIMIT n` does with `__topKFilter` (see `tryOptimizeTopK`): a PREWHERE with
/// the running boundary drops such rows before the other columns are read, and the boundary skips
/// whole granules through the primary key or a `minmax` skip index on the column. Returns the tracker
/// the heap must publish into, or null when the plan shape does not allow it.
///
/// Runs at the end of plan optimization, after PREWHERE was moved and index analysis ran, so unlike
/// `tryOptimizeTopK` it conjoins its filter with the existing PREWHERE and re-salts the query condition
/// cache key of the WHERE filter, which `updateQueryConditionCache` set before the read became a top-K read.
static TopKThresholdTrackerPtr tryAttachDynamicFilter(
    QueryPlan::Node * aggregating_node,
    const AggregatingStep & aggregating_step,
    size_t limit,
    size_t num_key_columns,
    int direction,
    int nulls_direction,
    const Optimization::ExtraSettings & settings,
    QueryPlan::Nodes & nodes)
{
    if (!settings.enable_group_by_top_k_dynamic_filtering)
        return nullptr;

    if (aggregating_node->children.size() != 1)
        return nullptr;

    const String & key_name = aggregating_step.getParams().keys.front();

    /// Descend through the steps that pass the key column through unchanged, by name and without
    /// rewriting it (`isSortKeyPassThrough`), down to the reading step.
    QueryPlan::Node * node = aggregating_node->children.front();
    FilterStep * closest_filter_step = nullptr;
    ReadFromMergeTree * read_step = nullptr;

    while (!read_step)
    {
        if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
        {
            if (!isSortKeyPassThrough(expression_step->getExpression(), key_name))
                return nullptr;
        }
        else if (auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
        {
            if (!isSortKeyPassThrough(filter_step->getExpression(), key_name))
                return nullptr;
            closest_filter_step = filter_step;
        }
        else if (auto * read = typeid_cast<ReadFromMergeTree *>(node->step.get()))
        {
            read_step = read;
            break;
        }
        else
            return nullptr;

        if (node->children.size() != 1)
            return nullptr;
        node = node->children.front();
    }

    /// FINAL deduplicates rows by the sorting key while reading; dropping a row before that can change which
    /// version of another row survives. Parallel replicas run the query text remotely, where nothing knows the tracker.
    if (read_step->isQueryWithFinal() || read_step->isParallelReadingFromReplicas())
        return nullptr;

    const auto & read_columns = read_step->getAllColumnNames();
    if (std::find(read_columns.begin(), read_columns.end(), key_name) == read_columns.end())
        return nullptr;

    /// A physical column of the table, not a virtual one: `__topKFilter` runs as a PREWHERE over the stored column.
    if (!read_step->getStorageMetadata()->getColumns().hasPhysical(key_name))
        return nullptr;

    const auto & header = *read_step->getOutputHeader();
    if (!header.has(key_name))
        return nullptr;
    const auto & key_column = header.getByName(key_name);

    /// The same type restrictions as `tryOptimizeTopK`: `__topKFilter` cannot compare `Dynamic`, `Variant`
    /// and empty tuples, and comparing variable-length values row by row may cost more than it saves.
    const auto * key_tuple_type = typeid_cast<const DataTypeTuple *>(key_column.type.get());
    if (isDynamic(key_column.type) || isVariant(key_column.type) || (key_tuple_type && key_tuple_type->getElements().empty()))
        return nullptr;
    if (!key_column.type->haveMaximumSizeOfValue() && !settings.use_top_k_dynamic_filtering_for_variable_length_types)
        return nullptr;

    SortColumnDescription key_sort_description(key_name, direction, nulls_direction);
    auto threshold_tracker = std::make_shared<TopKThresholdTracker>(key_sort_description);

    auto new_prewhere_info = std::make_shared<PrewhereInfo>();
    new_prewhere_info->prewhere_actions = ActionsDAG({NameAndTypePair(key_name, key_column.type)});
    auto filter_function = createInternalFunctionTopKFilterResolver(threshold_tracker);
    const auto & prewhere_node = new_prewhere_info->prewhere_actions.addFunction(
        filter_function, {new_prewhere_info->prewhere_actions.getInputs().front()}, {});
    new_prewhere_info->prewhere_actions.getOutputs().push_back(&prewhere_node);
    new_prewhere_info->prewhere_column_name = prewhere_node.result_name;
    new_prewhere_info->remove_prewhere_column = true;
    new_prewhere_info->need_filter = true;

    auto initial_header = read_step->getOutputHeader();
    read_step->updatePrewhereInfo(mergePrewhereInfos(read_step->getPrewhereInfo(), std::move(new_prewhere_info)));
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

    /// `where_clause = true` keeps `MergeTreeDataSelectExecutor` from narrowing the read up front to the granules
    /// that hold the `LIMIT` smallest rows (`getTopKMarks`): a group needs all of its rows, and `LIMIT` rows may
    /// hold fewer than `LIMIT` distinct keys. Only the boundary-driven filtering and granule skipping apply here.
    TopKFilterInfo info{key_name, key_column.type, num_key_columns, limit, direction, /*where_clause=*/ true, threshold_tracker, /*condition_hash=*/ 0};

    /// Salts the query condition cache key the way `tryOptimizeTopK` does, with an extra mark so that a
    /// `GROUP BY key LIMIT n` read never shares entries with an `ORDER BY key LIMIT n` read over the same table.
    SipHash hash;
    hash.update(std::string_view("group_by_top_k"));
    hash.update(info.column_name);
    const String type_name = info.data_type->getName();
    hash.update(type_name);
    hash.update(info.num_sort_columns);
    hash.update(info.limit_n);
    hash.update(info.direction);
    hash.update(nulls_direction);
    info.condition_hash = hash.get64();

    read_step->setTopKColumn(info);

    /// `updateQueryConditionCache` tagged the WHERE filter with the hash of the plain predicate. Under the running
    /// filter the granules it sees are only those the boundary let through, so the entry must be salted with the
    /// top-K parameters (the same way `updateQueryConditionCache` salts a read stamped by `tryOptimizeTopK`), or
    /// dropped when the cache is not to be used for top-K reads at all.
    if (closest_filter_step && closest_filter_step->hasConditionForQueryConditionCache())
    {
        if (settings.use_query_condition_cache_for_top_k)
            closest_filter_step->saltConditionForQueryConditionCache(read_step->getTopKFilterInfo()->condition_hash);
        else
            closest_filter_step->resetConditionForQueryConditionCache();
    }

    LOG_TRACE(getLogger("optimizeGroupByTopK"), "Filtering and skipping granules of {} by the top-K boundary of the aggregation", key_name);
    return threshold_tracker;
}

size_t tryOptimizeGroupByTopK(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    if (!settings.enable_group_by_top_k_optimization)
        return 0;

    if (settings.make_distributed_plan || settings.serialize_query_plan)
        return 0;

    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return 0;

    if (limit_step->withTies())
        return 0;

    if (limit_step->alwaysReadTillEnd())
        return 0;

    size_t limit = limit_step->getLimitForSorting();
    if (limit < 1)
        return 0;

    if (settings.max_limit_for_top_k_optimization != 0 && limit > settings.max_limit_for_top_k_optimization)
        return 0;

    if (limit > Aggregator::Params::TopKParams::max_k)
    {
        LOG_DEBUG(
            getLogger("optimizeGroupByTopK"),
            "Skipping GROUP BY top-K optimization: the requested heap size {} is larger than the maximum {}",
            limit, Aggregator::Params::TopKParams::max_k);
        return 0;
    }

    if (parent_node->children.size() != 1)
        return 0;

    auto * next_node = parent_node->children.front();

    auto * sorting_step = typeid_cast<SortingStep *>(next_node->step.get());
    if (sorting_step)
    {
        if (sorting_step->getType() != SortingStep::Type::Full)
            return 0;
        if (next_node->children.size() != 1)
            return 0;
        next_node = next_node->children.front();
    }

    QueryPlan::Node * node_above_aggregation = parent_node;
    const ExpressionStep * expression_step = typeid_cast<const ExpressionStep *>(next_node->step.get());
    if (expression_step)
    {
        if (next_node->children.size() != 1)
            return 0;

        /// An arrayJoin between the aggregation and the limit changes row
        /// multiplicity: it can produce zero rows for a group, so the smallest
        /// N groups no longer guarantee N output rows and pruning loses groups
        /// the limit still needs.
        if (expression_step->getExpression().hasArrayJoin())
            return 0;

        node_above_aggregation = next_node;
        next_node = next_node->children.front();
    }

    auto * aggregating_step = validateAggregatingStep(next_node);
    if (!aggregating_step)
        return 0;

    QueryPlan::Node * aggregating_node = next_node;
    const auto & params = aggregating_step->getParams();

    if (!settings.is_explain && params.stats_collecting_params.isCollectionAndUseEnabled())
    {
        const auto hint = getHashTablesStatistics<AggregationEntry>().getSizeHint(params.stats_collecting_params);
        if (hint && static_cast<Float64>(hint->sum_of_sizes) <= static_cast<Float64>(limit) * 1.5)
            return 0;
    }

    std::vector<int> directions;
    std::vector<int> nulls_directions;
    size_t num_key_columns = 0;

    if (sorting_step)
    {
        const auto & sort_description = sorting_step->getSortDescription();
        if (sort_description.empty())
            return 0;

        num_key_columns = std::min(sort_description.size(), params.keys.size());

        directions.reserve(num_key_columns);
        nulls_directions.reserve(num_key_columns);

        for (size_t i = 0; i < num_key_columns; ++i)
        {
            if (sort_description[i].column_name != params.keys[i])
                return 0;

            if (expression_step && !isSortKeyPassThrough(expression_step->getExpression(), params.keys[i]))
                return 0;

            if (sort_description[i].collator)
                return 0;

            directions.push_back(sort_description[i].direction);
            nulls_directions.push_back(sort_description[i].nulls_direction);
        }
    }
    else
    {
        num_key_columns = params.keys.size();
        directions.assign(num_key_columns, 1);
        nulls_directions.assign(num_key_columns, 1);

        SortDescription sort_description;
        sort_description.reserve(num_key_columns);

        for (const auto & key : params.keys)
            sort_description.emplace_back(key, /*direction=*/ 1, /*nulls_direction=*/ 1);

        auto synthesized_sort = std::make_unique<SortingStep>(
            aggregating_node->step->getOutputHeader(),
            std::move(sort_description),
            limit,
            SortingStep::Settings(settings.max_block_size));
        synthesized_sort->setStepDescription("Sorting for GROUP BY top-K", settings.max_step_description_length);

        auto & sort_node = nodes.emplace_back();

        sort_node.step = std::move(synthesized_sort);
        sort_node.children = {aggregating_node};
        chassert(node_above_aggregation->children.front() == aggregating_node);

        node_above_aggregation->children.front() = &sort_node;

        chassert(blocksHaveEqualStructure(
            *sort_node.step->getOutputHeader(), *aggregating_node->step->getOutputHeader()));
    }

    const bool synthetic_sort = sorting_step == nullptr;

    Aggregator::Params::TopKParams top_k_params{
        .k = limit,
        .directions = std::move(directions),
        .nulls_directions = std::move(nulls_directions),
        .key_columns = num_key_columns,
        .observation_rows = synthetic_sort ? 0 : settings.top_k_optimization_observation_rows,
    };

    top_k_params.threshold_tracker = tryAttachDynamicFilter(
        aggregating_node,
        *aggregating_step,
        limit,
        num_key_columns,
        top_k_params.directions.front(),
        top_k_params.nulls_directions.front(),
        settings,
        nodes);

    aggregating_step->applyTopKOptimization(std::move(top_k_params));

    return 0;
}
}
