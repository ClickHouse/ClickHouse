#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Which inputs of the join have their non-matching rows dropped. Only those sides may receive a derived filter.
std::pair<bool, bool> droppedSides(const JoinOperator & join_operator)
{
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
}

/// Conditions that are always false when either argument is NULL.
bool isNullRejectingCondition(JoinConditionOperator op)
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
}

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

bool tryAddDerivedNotNullFilter(QueryPlan::Node & node, const SharedHeader & header, const NameSet & columns, QueryPlan::Nodes & nodes)
{
    ActionsDAG dag(header->getColumnsWithTypeAndName());

    auto is_not_null = FunctionFactory::instance().get("isNotNull", Context::getGlobalContextInstance());
    auto derived_filter_marker = FunctionFactory::instance().get("__plannerOnlyFilter", Context::getGlobalContextInstance());

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

    return makeFilterNodeOnTopOf(
        node, std::move(dag), filter_node->result_name,
        /*remove_filer=*/true, nodes, makeDescription("Derived NOT NULL filter from JOIN condition"));
}

}

size_t tryDeriveNotNullFiltersFromJoin(QueryPlan::Node * node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
    auto * join = typeid_cast<JoinStepLogical *>(node->step.get());
    if (!join || node->children.size() != 2)
        return 0;

    /// Ensure NOT NULL filters are derived once per join.
    if (join->notNullFiltersDerived())
        return 0;
    join->setNotNullFiltersDerived(true);

    auto [drop_left, drop_right] = droppedSides(join->getJoinOperator());

    /// A JoinStepLogicalLookup child must stay directly below the join.
    if (typeid_cast<JoinStepLogicalLookup *>(node->children[0]->step.get()))
        drop_left = false;
    if (typeid_cast<JoinStepLogicalLookup *>(node->children[1]->step.get()))
        drop_right = false;

    if (!drop_left && !drop_right)
        return 0;

    NameSet left_columns;
    NameSet right_columns;
    for (const auto & expr : join->getJoinOperator().expression)
    {
        auto [op, lhs, rhs] = expr.asBinaryPredicate();
        if (!isNullRejectingCondition(op))
            continue;

        collectDroppedNullableColumns(lhs, left_columns, right_columns, drop_left, drop_right);
        collectDroppedNullableColumns(rhs, left_columns, right_columns, drop_left, drop_right);
    }

    bool added = false;
    if (!left_columns.empty())
        added |= tryAddDerivedNotNullFilter(*node->children[0], join->getInputHeaders()[0], left_columns, nodes);
    if (!right_columns.empty())
        added |= tryAddDerivedNotNullFilter(*node->children[1], join->getInputHeaders()[1], right_columns, nodes);

    return added ? 2 : 0;
}

}
