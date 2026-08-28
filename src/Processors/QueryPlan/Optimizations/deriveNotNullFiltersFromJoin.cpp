#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionPlannerOnlyFilter.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

void collectDroppedNullableColumns(const JoinActionRef & side, NameSet & left_columns, NameSet & right_columns, bool drop_left, bool drop_right)
{
    const auto * node = side.getNode();
    /// TODO: support argument columns of a NULL-propagating expression (e.g. `key + 1`).
    if (node->type != ActionsDAG::ActionType::INPUT)
        return;

    if (!isNullableOrLowCardinalityNullable(node->result_type))
        return;

    if (side.fromLeft() && drop_left)
        left_columns.insert(node->result_name);
    else if (side.fromRight() && drop_right)
        right_columns.insert(node->result_name);
}

bool tryAddDerivedNotNullFilter(QueryPlan::Node & join_node, size_t child_index, const NameSet & columns, QueryPlan::Nodes & nodes)
{
    QueryPlan::Node * child_node = join_node.children[child_index];
    const auto header = child_node->step->getOutputHeader();
    ActionsDAG dag(header->getColumnsWithTypeAndName());

    auto is_not_null = FunctionFactory::instance().get("isNotNull", Context::getGlobalContextInstance());
    auto derived_filter_marker = createInternalFunctionPlannerOnlyFilterResolver();

    ActionsDAG::NodeRawConstPtrs conjuncts;
    for (const auto * input : dag.getInputs())
    {
        if (!columns.contains(input->result_name))
            continue;

        const auto & is_not_null_node = dag.addFunction(is_not_null, {input}, {});
        conjuncts.push_back(&dag.addFunction(derived_filter_marker, {&is_not_null_node}, {}));
    }

    if (conjuncts.empty())
        return false;

    const ActionsDAG::Node * filter_node = conjuncts.front();
    if (conjuncts.size() > 1)
    {
        auto func_and = FunctionFactory::instance().get("and", Context::getGlobalContextInstance());
        filter_node = &dag.addFunction(func_and, std::move(conjuncts), {});
    }
    dag.addOrReplaceInOutputs(*filter_node);

    auto step = std::make_unique<FilterStep>(header, std::move(dag), filter_node->result_name, /*remove_filter=*/true);
    step->setStepDescription("Derived NOT NULL filter from JOIN condition");

    auto & filter_step_node = nodes.emplace_back();
    filter_step_node.step = std::move(step);
    filter_step_node.children = {child_node};
    join_node.children[child_index] = &filter_step_node;

    return true;
}

}

size_t tryDeriveNotNullFiltersFromJoin(QueryPlan::Node * node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
    auto * join = typeid_cast<JoinStepLogical *>(node->step.get());
    if (!join || node->children.size() != 2)
        return 0;

    /// Which inputs of the join have their non-matching rows dropped. Only those sides may receive a derived filter.
    auto [drop_left, drop_right] = [&]() -> std::pair<bool, bool>
    {
        const auto & join_operator = join->getJoinOperator();
        const auto kind = join_operator.kind;

        switch (join_operator.strictness)
        {
            case JoinStrictness::All:
            case JoinStrictness::Any:
            case JoinStrictness::Anti:
                /// An ANTI join can not use null-extended join keys on the non-preserved side to filter any rows.
                return {isInnerOrRight(kind), isInnerOrLeft(kind)};
            case JoinStrictness::Semi:
                return {isLeftOrRight(kind), isLeftOrRight(kind)};
            default:
                return {false, false};
        }
    }();

    /// A JoinStepLogicalLookup child must stay directly below the join.
    if (typeid_cast<JoinStepLogicalLookup *>(node->children[0]->step.get()))
        drop_left = false;
    if (typeid_cast<JoinStepLogicalLookup *>(node->children[1]->step.get()))
        drop_right = false;

    if (!drop_left && !drop_right)
        return 0;

    /// Conditions that are always false when either argument is NULL.
    auto is_null_rejecting_condition = [](JoinConditionOperator op)
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

    NameSet left_columns;
    NameSet right_columns;
    for (const auto & expr : join->getJoinOperator().expression)
    {
        auto [op, lhs, rhs] = expr.asBinaryPredicate();
        if (!is_null_rejecting_condition(op))
            continue;

        collectDroppedNullableColumns(lhs, left_columns, right_columns, drop_left, drop_right);
        collectDroppedNullableColumns(rhs, left_columns, right_columns, drop_left, drop_right);
    }

    /// Ensure a NOT NULL filter is derived once per column.
    const auto & derived_left = join->notNullFiltersDerivedColumns(JoinTableSide::Left);
    const auto & derived_right = join->notNullFiltersDerivedColumns(JoinTableSide::Right);
    std::erase_if(left_columns, [&](const auto & name) { return derived_left.contains(name); });
    std::erase_if(right_columns, [&](const auto & name) { return derived_right.contains(name); });

    const bool added_left = !left_columns.empty() && tryAddDerivedNotNullFilter(*node, 0, left_columns, nodes);
    const bool added_right = !right_columns.empty() && tryAddDerivedNotNullFilter(*node, 1, right_columns, nodes);

    if (added_left)
        join->addNotNullFiltersDerivedColumns(JoinTableSide::Left, left_columns);
    if (added_right)
        join->addNotNullFiltersDerivedColumns(JoinTableSide::Right, right_columns);

    return added_left || added_right ? 2 : 0;
}

}
