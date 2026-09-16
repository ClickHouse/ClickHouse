#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Core/Joins.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Core/Names.h>
#include <Interpreters/JoinOperator.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace DB::QueryPlanOptimizations
{

namespace
{

using NodeSet = std::unordered_set<const ActionsDAG::Node *>;

NodeSet collectNullPropagatingInputs(const ActionsDAG::Node * node)
{
    switch (node->type)
    {
        case ActionsDAG::ActionType::INPUT:
        case ActionsDAG::ActionType::PLACEHOLDER:
            /// A column that is not `Nullable` here holds no NULL, so it rejects nothing.
            return isNullableOrLowCardinalityNullable(node->result_type) ? NodeSet{node} : NodeSet{};
        case ActionsDAG::ActionType::ALIAS:
            return !node->children.empty() ? collectNullPropagatingInputs(node->children.front()) : NodeSet{};
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (!node->function || !node->function->isNullPropagating(node->result_type))
                return {};
            NodeSet result;
            for (const auto * child : node->children)
                result.merge(collectNullPropagatingInputs(child));
            return result;
        }
        case ActionsDAG::ActionType::COLUMN:
        case ActionsDAG::ActionType::ARRAY_JOIN:
            return {};
    }

    return {};
}

/// Inputs that cannot be NULL in a row that passes `predicate`, because a NULL there would make the predicate NULL.
NodeSet collectNullRejectedInputs(const ActionsDAG::Node * predicate)
{
    if (predicate->type == ActionsDAG::ActionType::ALIAS && !predicate->children.empty())
        return collectNullRejectedInputs(predicate->children.front());

    if (predicate->type == ActionsDAG::ActionType::FUNCTION && predicate->function_base)
    {
        const auto & name = predicate->function_base->getName();

        if (name == "and")
        {
            NodeSet result;
            for (const auto * child : predicate->children)
                result.merge(collectNullRejectedInputs(child));
            return result;
        }

        if (name == "or" && !predicate->children.empty())
        {
            NodeSet result = collectNullRejectedInputs(predicate->children.front());
            for (size_t i = 1; i < predicate->children.size() && !result.empty(); ++i)
            {
                auto other = collectNullRejectedInputs(predicate->children[i]);
                std::erase_if(result, [&](const auto * input) { return !other.contains(input); });
            }
            return result;
        }

        if (name == "isNotNull" && predicate->children.size() == 1)
            return collectNullPropagatingInputs(predicate->children.front());
    }

    /// The predicate is rejecting wherever it becomes NULL.
    return collectNullPropagatingInputs(predicate);
}


/// Names produced by more than one output cannot be mapped to a specific propagated column.
NameSet duplicateOutputNames(const ActionsDAG::NodeRawConstPtrs & outputs)
{
    NameSet seen;
    NameSet ambiguous;
    for (const auto * output : outputs)
        if (!seen.insert(output->result_name).second)
            ambiguous.insert(output->result_name);
    return ambiguous;
}

/// Translate a constraint on the outputs of `dag` into a constraint on its inputs.
NameSet remapNullRejectedColumnsThroughActions(const ActionsDAG & dag, const NameSet & null_rejected_columns)
{
    const auto duplicate = duplicateOutputNames(dag.getOutputs());

    NameSet produced_columns;
    NameSet result;
    for (const auto * output : dag.getOutputs())
    {
        produced_columns.insert(output->result_name);
        if (!null_rejected_columns.contains(output->result_name) || duplicate.contains(output->result_name))
            continue;
        for (const auto * input : collectNullPropagatingInputs(output))
            result.insert(input->result_name);
    }

    /// `ActionsDAG::updateHeader` forwards an input column the DAG does not consume to the output header.
    NameSet consumed_columns;
    for (const auto * input : dag.getInputs())
        consumed_columns.insert(input->result_name);

    for (const auto & name : null_rejected_columns)
        if (!produced_columns.contains(name) && !consumed_columns.contains(name))
            result.insert(name);

    return result;
}

void collectNullRejectedColumnsFromFilter(const FilterStep & filter, NameSet & null_rejected_columns)
{
    const auto & dag = filter.getExpression();
    const auto * predicate = dag.tryFindInOutputs(filter.getFilterColumnName());
    if (!predicate)
        return;

    for (const auto * input : collectNullRejectedInputs(predicate))
        null_rejected_columns.insert(input->result_name);
}

void collectNullRejectedColumnsFromJoinConditions(const JoinStepLogical & join, NameSet & left, NameSet & right)
{
    const auto & actions = join.getActionsDAG();
    if (actions.hasStatefulFunctions() || dagContainsNonDeterministicFunction(actions))
        return;

    const auto & join_operator = join.getJoinOperator();
    /// The sides for which a null-rejecting condition proves the input column not NULL.
    auto [discards_left, discards_right] = [&]() -> std::pair<bool, bool>
    {
        const auto kind = join_operator.kind;
        switch (join_operator.strictness)
        {
            case JoinStrictness::Unspecified:
            case JoinStrictness::All:
            case JoinStrictness::Any:
            case JoinStrictness::Anti:
                return {isInnerOrRight(kind), isInnerOrLeft(kind)};
            case JoinStrictness::Semi:
                return {isLeftOrRight(kind), isLeftOrRight(kind)};
            default:
                return {false, false};
        }
    }();
    if (!discards_left && !discards_right)
        return;

    auto is_null_rejecting_operator = [](JoinConditionOperator op)
    {
        switch (op)
        {
            case JoinConditionOperator::Equals:
            case JoinConditionOperator::Less:
            case JoinConditionOperator::LessOrEquals:
            case JoinConditionOperator::Greater:
            case JoinConditionOperator::GreaterOrEquals:
                return true;
            default:
                return false;
        }
    };

    for (const auto & expression : join_operator.expression)
    {
        auto [op, lhs, rhs] = expression.asBinaryPredicate();
        if (!is_null_rejecting_operator(op))
            continue;

        for (const auto & operand : {lhs, rhs})
        {
            if (!operand)
                continue;

            NameSet * side = nullptr;
            if (discards_left && operand.fromLeft())
                side = &left;
            else if (discards_right && operand.fromRight())
                side = &right;

            if (!side)
                continue;

            for (const auto * input : collectNullPropagatingInputs(operand.getNode()))
                side->insert(input->result_name);
        }
    }
}

/// Split a constraint on the join's output header between its two inputs.
std::pair<NameSet, NameSet> splitNullRejectedColumnsOnJoin(const JoinStepLogical & join, const NameSet & null_rejected_columns)
{
    NameSet left;
    NameSet right;
    if (null_rejected_columns.empty())
        return {std::move(left), std::move(right)};

    const auto & outputs = join.getActionsDAG().getOutputs();
    const auto duplicate = duplicateOutputNames(outputs);

    for (const auto * output : outputs)
    {
        if (!null_rejected_columns.contains(output->result_name) || duplicate.contains(output->result_name))
            continue;

        /// Only a column this join pads with a real NULL can witness that the padded rows are gone.
        /// `addToNullableIfNeeded` wraps exactly the padded columns in `toNullable`, and a column that
        /// is `Nullable` already is padded with its type default, which is NULL.
        if (!isNullableOrLowCardinalityNullable(output->result_type))
            continue;

        JoinActionRef ref(output, join.getExpressionActions());
        if (ref.fromLeft())
            left.insert(output->result_name);
        else if (ref.fromRight())
            right.insert(output->result_name);
    }
    return {std::move(left), std::move(right)};
}

void convertJoinKind(JoinStepLogical & join, QueryPlan::Node & node, const NameSet & left_null_rejected_columns, const NameSet & right_null_rejected_columns)
{
    auto & join_operator = join.getJoinOperator();
    const auto kind = join_operator.kind;
    if (kind != JoinKind::Left && kind != JoinKind::Right && kind != JoinKind::Full)
        return;

    if (join_operator.strictness != JoinStrictness::All)
        return;

    /// A `JoinStepLogicalLookup` source expects a particular join kind.
    auto is_storage_join = [&]()
    {
        for (const auto * child : node.children)
        {
            while (!typeid_cast<JoinStepLogicalLookup *>(child->step.get()) && child->children.size() == 1)
                child = child->children.front();

            if (auto * lookup_step = typeid_cast<JoinStepLogicalLookup *>(child->step.get()))
                if (lookup_step->getPreparedJoinStorage().storage_join != nullptr)
                    return true;
        }
        return false;
    }();
    if (is_storage_join)
        return;

    /// A side is "safe" when the rows this join would null-extend on it cannot survive above.
    const bool left_stream_safe = !left_null_rejected_columns.empty();
    const bool right_stream_safe = !right_null_rejected_columns.empty();

    if (kind == JoinKind::Full)
    {
        if (left_stream_safe && right_stream_safe)
            join_operator.kind = JoinKind::Inner;
        else if (left_stream_safe)
            join_operator.kind = JoinKind::Left;
        else if (right_stream_safe)
            join_operator.kind = JoinKind::Right;
    }
    else if (kind == JoinKind::Left && right_stream_safe)
        join_operator.kind = JoinKind::Inner;
    else if (kind == JoinKind::Right && left_stream_safe)
        join_operator.kind = JoinKind::Inner;
}

void visit(QueryPlan::Node & root)
{
    struct Frame
    {
        QueryPlan::Node * node;
        NameSet null_rejected_columns;
    };

    std::vector<Frame> stack;
    stack.push_back({&root, {}});

    while (!stack.empty())
    {
        auto frame = std::move(stack.back());
        stack.pop_back();

        auto & node = *frame.node;
        auto & null_rejected_columns = frame.null_rejected_columns;

        if (!node.step || node.children.empty())
            continue;

        if (!node.step->hasOutputHeader())
        {
            for (auto * child : node.children)
                stack.push_back({child, {}});
            continue;
        }

        /// Keep only the names this header carries exactly once.
        std::unordered_map<String, size_t> occurrences;
        for (const auto & column : *node.step->getOutputHeader())
            ++occurrences[column.name];
        std::erase_if(null_rejected_columns, [&](const auto & name) { return occurrences[name] != 1; });

        const auto & step = *node.step;

        if (auto * join = typeid_cast<JoinStepLogical *>(node.step.get()); join && node.children.size() == 2)
        {
            if (isPaste(join->getJoinOperator().kind))
            {
                stack.push_back({node.children[0], {}});
                stack.push_back({node.children[1], {}});
                continue;
            }

            auto [left, right] = splitNullRejectedColumnsOnJoin(*join, null_rejected_columns);

            convertJoinKind(*join, node, left, right);

            /// NULLs on a side this join can still null-extend may be introduced here, so a constraint
            /// observed above it is not guaranteed below it.
            const auto kind = join->getJoinOperator().kind;
            if (kind == JoinKind::Left || kind == JoinKind::Full)
                right.clear();
            if (kind == JoinKind::Right || kind == JoinKind::Full)
                left.clear();

            collectNullRejectedColumnsFromJoinConditions(*join, left, right);

            stack.push_back({node.children[0], std::move(left)});
            stack.push_back({node.children[1], std::move(right)});
            continue;
        }

        if (const auto * filter = typeid_cast<const FilterStep *>(&step))
        {
            if (filter->getExpression().hasStatefulFunctions() || dagContainsNonDeterministicFunction(filter->getExpression()))
            {
                stack.push_back({node.children.front(), {}});
                continue;
            }

            auto child_null_rejected_columns = remapNullRejectedColumnsThroughActions(filter->getExpression(), null_rejected_columns);
            collectNullRejectedColumnsFromFilter(*filter, child_null_rejected_columns);
            stack.push_back({node.children.front(), std::move(child_null_rejected_columns)});
            continue;
        }

        if (const auto * expression = typeid_cast<const ExpressionStep *>(&step))
        {
            if (expression->getExpression().hasStatefulFunctions() || dagContainsNonDeterministicFunction(expression->getExpression()))
            {
                stack.push_back({node.children.front(), {}});
                continue;
            }

            stack.push_back({node.children.front(), remapNullRejectedColumnsThroughActions(expression->getExpression(), null_rejected_columns)});
            continue;
        }

        auto filterNullRejectedColumnsBy = [&](const Names & allowed)
        {
            NameSet result;
            for (const auto & name : allowed)
                if (null_rejected_columns.contains(name))
                    result.insert(name);
            return result;
        };

        /// The conditions below for propagating null rejected columns to inputs are a mirror
        /// of the conditions in `tryPushDownFilter` in `filterPushDown.cpp`.

        /// Only the grouping keys, because a constraint on them deletes the whole group.
        /// `GROUPING SETS` and `group_by_use_nulls` make the step emit NULL keys.
        if (const auto * aggregating = typeid_cast<const AggregatingStep *>(&step))
        {
            bool emits_null_keys = aggregating->isGroupingSets() || aggregating->isGroupByUseNulls();
            stack.push_back({node.children.front(), emits_null_keys ? NameSet{} : filterNullRejectedColumnsBy(aggregating->getParams().keys)});
            continue;
        }

        if (const auto * merging_aggregated = typeid_cast<const MergingAggregatedStep *>(&step))
        {
            bool emits_null_keys = merging_aggregated->isGroupingSets();
            stack.push_back({node.children.front(), emits_null_keys ? NameSet{} : filterNullRejectedColumnsBy(merging_aggregated->getParams().keys)});
            continue;
        }

        /// Only the PARTITION BY columns: dropping a whole partition changes no window value on a surviving row.
        if (const auto * window = typeid_cast<const WindowStep *>(&step))
        {
            Names partition_keys;
            for (const auto & sort_column : window->getWindowDescription().partition_by)
                partition_keys.push_back(sort_column.column_name);
            stack.push_back({node.children.front(), filterNullRejectedColumnsBy(partition_keys)});
            continue;
        }

        /// Only the LIMIT BY keys, and only when the step cannot empty a non-empty group.
        if (const auto * limit_by = typeid_cast<const LimitByStep *>(&step))
        {
            bool can_empty_group = limit_by->getGroupOffset() != 0 || limit_by->getGroupLength() == 0;
            stack.push_back({node.children.front(), can_empty_group ? NameSet{} : filterNullRejectedColumnsBy(limit_by->getColumns())});
            continue;
        }

        if (typeid_cast<const CreatingSetsStep *>(&step))
        {
            stack.push_back({node.children.front(), std::move(null_rejected_columns)});
            for (size_t i = 1; i < node.children.size(); ++i)
                stack.push_back({node.children[i], {}});
            continue;
        }

        /// Everything but the array-join columns.
        if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(&step))
        {
            for (const auto & name : array_join->getColumns())
                null_rejected_columns.erase(name);
            stack.push_back({node.children.front(), std::move(null_rejected_columns)});
            continue;
        }

        if (const auto * sorting = typeid_cast<const SortingStep *>(&step))
        {
            stack.push_back({node.children.front(), sorting->getLimit() ? NameSet{} : std::move(null_rejected_columns)});
            continue;
        }

        if (const auto * distinct = typeid_cast<const DistinctStep *>(&step))
        {
            stack.push_back({node.children.front(), distinct->getLimitHint() ? NameSet{} : std::move(null_rejected_columns)});
            continue;
        }

        /// Passthrough steps.
        if (typeid_cast<const DelayedCreatingSetsStep *>(&step)
            || typeid_cast<const BuildRuntimeFilterStep *>(&step)
            || typeid_cast<const CreateSetAndFilterOnTheFlyStep *>(&step))
        {
            stack.push_back({node.children.front(), std::move(null_rejected_columns)});
            continue;
        }

        /// Branches match the output by position, so a name carries over only when every branch header
        /// is structurally equal to the output.
        if (const auto * union_step = typeid_cast<const UnionStep *>(&step))
        {
            const auto & union_output = *union_step->getOutputHeader();
            bool all_branches_match = std::ranges::all_of(
                union_step->getInputHeaders(),
                [&](const auto & input_header) { return blocksHaveEqualStructure(*input_header, union_output); });

            for (auto * child : node.children)
                stack.push_back({child, all_branches_match ? null_rejected_columns : NameSet{}});
            continue;
        }

        /// Unrecognized step.
        for (auto * child : node.children)
            stack.push_back({child, {}});
    }
}

}

void convertOuterJoinToInnerJoinTransitively(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root)
{
    if (!optimization_settings.optimize_plan || !optimization_settings.convert_outer_join_to_inner_join_transitively)
        return;

    visit(root);
}

}
