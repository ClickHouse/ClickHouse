#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TableJoin.h>

#include <Parsers/ASTWindowDefinition.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>

#include <Storages/StorageMerge.h>

#include <Processors/QueryPlan/Optimizations/optimizeInOrderCommon.h>


namespace DB::QueryPlanOptimizations
{

static Permutation flattenPositions(const std::vector<Positions> & positions)
{
    Permutation res;
    res.reserve(positions.size());
    for (const auto & pos : positions)
        res.insert(res.end(), pos.begin(), pos.end());
    return res;
}

static size_t findPrefixMatchLength(const std::vector<Positions> & positions, const Permutation & flattened)
{
    size_t flatten_idx = 0;
    for (size_t i = 0; i < positions.size(); ++i)
    {
        for (size_t e : positions[i])
        {
            if (e != flattened[flatten_idx])
                return i;
            ++flatten_idx;
        }
    }
    return positions.size();
}

/// Choose order info that use longer prefix of sorting key.
/// Returns true if lhs is better than rhs.
static bool compareOrderInfos(const InputOrderInfoPtr & lhs, const InputOrderInfoPtr & rhs)
{
    size_t lhs_size = lhs ? lhs->used_prefix_of_sorting_key_size : 0;
    size_t rhs_size = rhs ? rhs->used_prefix_of_sorting_key_size : 0;
    return lhs_size >= rhs_size;
}

/// Cut order info to use only prefix of specified size
static void truncateToPrefixSize(StepInputOrder & order_info, size_t prefix_size)
{
    if (!order_info.input_order)
        return;

    chassert(order_info.target_sort_description.size() >= order_info.sort_description_for_merging.size());

    if (order_info.target_sort_description.size() <= prefix_size)
        return;

    order_info.target_sort_description.resize(prefix_size);
    order_info.sort_description_for_merging.resize(prefix_size);

    const auto & last_column_name = order_info.sort_description_for_merging[prefix_size - 1].column_name;

    chassert(order_info.input_order->sort_description_for_merging.size() == order_info.input_order->used_prefix_of_sorting_key_size);

    auto input_order = std::make_shared<InputOrderInfo>(*order_info.input_order);
    for (size_t i = 0; i < input_order->sort_description_for_merging.size(); ++i)
    {
        if (input_order->sort_description_for_merging[i].column_name == last_column_name)
        {
            input_order->used_prefix_of_sorting_key_size = i + 1;
            input_order->sort_description_for_merging.resize(input_order->used_prefix_of_sorting_key_size);
            break;
        }
    }
    order_info.input_order = std::move(input_order);
}

/// Choose order info that use longer prefix of sorting key and cut second one to use common prefix.
/// Returns permutation that should be applied to keys.
static Permutation findCommonOrderInfo(StepInputOrder & left_order_info, StepInputOrder & right_order_info)
{
    if (!left_order_info.input_order && !right_order_info.input_order)
    {
        LOG_TRACE(getLogger(), "Cannot read anything in order for join");
        return {};
    }

    if (!right_order_info.input_order)
    {
        LOG_TRACE(getLogger(), "Can read left stream in order for join");
        return flattenPositions(left_order_info.permutation);
    }

    if (!left_order_info.input_order)
    {
        LOG_TRACE(getLogger(), "Can read right stream in order for join");
        return flattenPositions(right_order_info.permutation);
    }

    LOG_TRACE(getLogger(), "Can read both streams in order for join");

    bool left_is_better = compareOrderInfos(left_order_info.input_order, right_order_info.input_order);
    auto & lhs = left_is_better ? left_order_info : right_order_info;
    auto & rhs = left_is_better ? right_order_info : left_order_info;

    Permutation result_permutation = flattenPositions(lhs.permutation);
    size_t prefix_size = findPrefixMatchLength(rhs.permutation, result_permutation);
    truncateToPrefixSize(rhs, prefix_size);
    return result_permutation;
}

static bool optimizeJoinInOrder(QueryPlan::Node & node, const std::shared_ptr<FullSortingMergeJoin> & join_ptr)
{
    auto * left_child_node = node.children[0];
    auto * right_child_node = node.children[1];
    if (left_child_node->children.size() != 1 || right_child_node->children.size() != 1)
        return false;

    const auto & key_names_left = join_ptr->getKeyNames(JoinTableSide::Left);
    StepStack steps_to_update_left;
    QueryPlan::Node * left_reading_node = findReadingStep(*left_child_node->children.front(), steps_to_update_left);
    auto left_order_info = buildInputOrderInfo(key_names_left, *left_child_node, left_reading_node);

    const auto & key_names_right = join_ptr->getKeyNames(JoinTableSide::Right);
    StepStack steps_to_update_right;
    QueryPlan::Node * right_reading_node = findReadingStep(*right_child_node->children.front(), steps_to_update_right);
    auto right_order_info = buildInputOrderInfo(key_names_right, *right_child_node, right_reading_node);

    auto keys_permuation = findCommonOrderInfo(left_order_info, right_order_info);
    if (keys_permuation.empty())
        return false;

    LOG_TRACE(getLogger(), "Applying permutation [{}] for join keys", fmt::join(keys_permuation, ", "));

    join_ptr->permuteKeys(keys_permuation);

    if (left_order_info.input_order)
    {
        join_ptr->setPrefixSortDesctiption(left_order_info.sort_description_for_merging, JoinTableSide::Left);
        requestInputOrderInfo(left_order_info.input_order, left_reading_node->step);
        updateStepsDataStreams(steps_to_update_left);
    }

    if (right_order_info.input_order)
    {
        join_ptr->setPrefixSortDesctiption(right_order_info.sort_description_for_merging, JoinTableSide::Right);
        requestInputOrderInfo(right_order_info.input_order, right_reading_node->step);
        updateStepsDataStreams(steps_to_update_right);
    }
    return true;
}

void applyOrderForJoin(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (node.children.size() != 2)
        return;

    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step)
        return;

    auto join_ptr = std::dynamic_pointer_cast<FullSortingMergeJoin>(join_step->getJoin());
    if (!join_ptr)
        return;

    bool is_read_in_order_optimized = false;
    if (optimization_settings.join_in_order)
        is_read_in_order_optimized = optimizeJoinInOrder(node, join_ptr);

    auto insert_pre_step = [&nodes, &node](size_t idx, auto step)
    {
        auto & sort_node = nodes.emplace_back();
        sort_node.step = std::move(step);
        sort_node.children.push_back(node.children[idx]);
        node.children[idx] = &sort_node;
    };

    auto join_kind = join_ptr->getTableJoin().kind();
    bool kind_allows_filtering = isInner(join_kind) || isLeft(join_kind) || isRight(join_kind);

    auto has_non_const = [](const Block & block, const auto & keys)
    {
        for (const auto & key : keys)
        {
            const auto & column = block.getByName(key).column;
            if (column && !isColumnConst(*column))
                return true;
        }
        return false;
    };

    const auto & left_stream = node.children[0]->step->getOutputStream();
    const auto & right_stream = node.children[1]->step->getOutputStream();

    /// This optimization relies on the sorting that should buffer the whole stream before emitting any rows.
    /// It doesn't hold such a guarantee for streams with const keys.
    /// Note: it's also doesn't work with the read-in-order optimization.
    /// No checks here because read in order is not applied if we have `CreateSetAndFilterOnTheFlyStep` in the pipeline between the reading and sorting steps.
    bool has_non_const_keys = has_non_const(left_stream.header, join_ptr->getKeyNames(JoinTableSide::Left))
        && has_non_const(right_stream.header, join_ptr->getKeyNames(JoinTableSide::Right));

    size_t max_rows_in_filter_set = optimization_settings.max_rows_in_set_to_optimize_join;
    if (!is_read_in_order_optimized && max_rows_in_filter_set > 0 && kind_allows_filtering && has_non_const_keys)
    {
        auto crosswise_connection = CreateSetAndFilterOnTheFlyStep::createCrossConnection();
        auto add_create_set = [&](const DataStream & data_stream, const Names & key_names, JoinTableSide join_pos)
        {
            auto creating_set_step = std::make_unique<CreateSetAndFilterOnTheFlyStep>(
                data_stream, key_names, max_rows_in_filter_set, crosswise_connection, join_pos);
            creating_set_step->setStepDescription(fmt::format("Create set and filter {} joined stream", join_pos));

            auto * step_raw_ptr = creating_set_step.get();
            insert_pre_step(join_pos == JoinTableSide::Left ? 0 : 1, std::move(creating_set_step));
            return step_raw_ptr;
        };

        auto * left_set = add_create_set(left_stream, join_ptr->getKeyNames(JoinTableSide::Left), JoinTableSide::Left);
        auto * right_set = add_create_set(right_stream, join_ptr->getKeyNames(JoinTableSide::Right), JoinTableSide::Right);
        if (isInnerOrLeft(join_kind))
            right_set->setFiltering(left_set->getSet());
        if (isInnerOrRight(join_kind))
            left_set->setFiltering(right_set->getSet());
    }
    for (size_t i = 0; i < 2; ++i)
    {
        insert_pre_step(i, join_step->createSorting(JoinTableSide(i)));
    }
}

}
