#include <algorithm>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/ColumnConst.h>
#include <Common/logger_useful.h>
#include <Common/Logger.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Joins.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/JoinUtils.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// `isNull` / `isNotNull` of a column, which are constant if the column is known not to be NULL.
bool isNullCheck(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base || node.children.size() != 1 || !isUInt8(node.result_type))
        return false;
    const auto & name = node.function_base->getName();
    return name == "isNull" || name == "isNotNull";
}

/// The node whose value `node` holds, which is NULL exactly when `node` is.
const ActionsDAG::Node * skipAliasAndToNullable(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS
           || (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->getName() == "toNullable"))
        node = node->children.front();
    return node;
}

}

/// This function is exposed for testing purposes
FilterResult filterResultForMatchedRows(
    ActionsDAG pre_actions_dag, const ActionsDAG & filter_dag, const String & filter_column_name, const NameSet & not_null_columns)
{
    /// If either DAG contains IN subquery sets that are not yet built we cannot evaluate the filter result
    if (dagContainsNonReadySet(filter_dag) || dagContainsNonReadySet(pre_actions_dag))
        return FilterResult::UNKNOWN;

    /// Bail out for functions that are not deterministic in the scope of a single query for the
    /// same reason as in `filterResultForNotMatchedRows`: a single plan-time evaluation of `rand`,
    /// `nowInBlock`, `rowNumberInAllBlocks`, `blockNumber`, etc. cannot stand in for the runtime
    /// per-row behaviour, and acting on it would silently change the result of the JOIN. Functions
    /// like `now` / `today` / `currentUser` ARE deterministic within a single query and do NOT
    /// trip the guard.
    if (dagContainsNonDeterministicFunction(filter_dag) || dagContainsNonDeterministicFunction(pre_actions_dag))
        return FilterResult::UNKNOWN;

    /// The filter reads the outputs of `pre_actions_dag` by name, so a name produced twice cannot be attributed.
    std::unordered_map<String, const ActionsDAG::Node *> not_null_outputs;
    for (const auto * output : pre_actions_dag.getOutputs())
    {
        if (!not_null_columns.contains(output->result_name))
            continue;
        if (auto [it, inserted] = not_null_outputs.emplace(output->result_name, output); !inserted)
            it->second = nullptr;
    }

    std::unordered_set<const ActionsDAG::Node *> not_null_nodes;
    for (const auto & [_, output] : not_null_outputs)
        if (output)
            not_null_nodes.insert(skipAliasAndToNullable(output));

    /// Not `ActionsDAG::merge`: it removes the actions left unused, which may free the nodes collected above.
    auto combined_dag = std::move(pre_actions_dag);
    combined_dag.mergeInplace(filter_dag.clone());
    ActionsDAG::IntermediateExecutionResult combined_dag_input;

    if (!not_null_nodes.empty())
    {
        for (const auto & node : combined_dag.getNodes())
        {
            if (!isNullCheck(node) || !not_null_nodes.contains(skipAliasAndToNullable(node.children.front())))
                continue;

            auto column = node.result_type->createColumnConst(1, UInt8(node.function_base->getName() == "isNotNull"));
            combined_dag_input.emplace(&node, ColumnWithTypeAndName{std::move(column), node.result_type, node.result_name});
        }
    }

    const auto * filter_node = combined_dag.tryFindInOutputs(filter_column_name);
    if (!filter_node)
        return FilterResult::UNKNOWN;

    return evaluateFilterResult(combined_dag_input, filter_node, /*allow_unknown_function_arguments=*/false);
}

namespace
{

/// Check if filter has a form like "column" or "NOT column", where column is an input
bool isSimpleFilter(const ActionsDAG::Node * filter_node, bool must_negate)
{
    /// Skip aliases
    while (filter_node->type == ActionsDAG::ActionType::ALIAS)
        filter_node = filter_node->children.front();

    switch (filter_node->type)
    {
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (must_negate && filter_node->function_base && filter_node->function_base->getName() == "not")
                return isSimpleFilter(filter_node->children.front(), false);
            return false;
        }
        case ActionsDAG::ActionType::INPUT:
            return !must_negate;
        default:
            return false;
    }
}

QueryPlanStepPtr convertToExpressionStep(FilterStep * filter_node)
{
    auto dag = std::move(filter_node->getExpression());
    if (filter_node->removesFilterColumn())
        dag.removeFromOutputs(filter_node->getFilterColumnName());
    auto new_expression_step = std::make_unique<ExpressionStep>(filter_node->getInputHeaders().front(), std::move(dag));
    new_expression_step->setStepDescription(*filter_node);
    return new_expression_step;
}

ActionsDAG getPreFilterActionsDAG(QueryPlan::Node * join_node, JoinStepLogical * join, JoinTableSide side_to_check)
{
    if (auto * expression_before_join_step = typeid_cast<ExpressionStep *>(join_node->children[side_to_check == JoinTableSide::Left ? 0 : 1]->step.get()))
    {
        ActionsDAG result_dag;
        if (side_to_check == JoinTableSide::Right)
        {
            result_dag = ActionsDAG(join->getInputHeaders().front()->getColumnsWithTypeAndName());
            result_dag.unite(expression_before_join_step->getExpression().clone());
        }
        else
        {
            result_dag = expression_before_join_step->getExpression().clone();
            result_dag.unite(ActionsDAG(join->getInputHeaders().back()->getColumnsWithTypeAndName()));
        }
        result_dag.mergeInplace(join->getActionsDAG().clone());
        return result_dag;
    }

    return join->getActionsDAG().clone();
};

/// Outputs of `join` holding a column of the `matched_side` input which is not NULL in a matched row: either its type
/// cannot contain NULL (only `join_use_nulls` makes the output `Nullable`), or the `ON` clause proves it, e.g. `r.k` for `ON l.k = r.k`.
NameSet notNullOutputsOfMatchedRows(const JoinStepLogical & join, JoinTableSide matched_side)
{
    NameSet left;
    NameSet right;
    collectNullRejectedColumnsFromJoinConditions(join, left, right);
    const auto & null_rejected_inputs = matched_side == JoinTableSide::Left ? left : right;

    NameSet result;
    for (const auto * output : join.getActionsDAG().getOutputs())
    {
        const auto * node = skipAliasAndToNullable(output);
        if (node->type != ActionsDAG::ActionType::INPUT
            || (canContainNull(*node->result_type) && !null_rejected_inputs.contains(node->result_name)))
            continue;

        JoinActionRef input(node, join.getExpressionActions());
        if (matched_side == JoinTableSide::Left ? input.fromLeft() : input.fromRight())
            result.insert(output->result_name);
    }

    return result;
}

}

size_t tryConvertAnyJoinToSemiOrAntiJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & /*settings*/)
{
    auto & parent = parent_node->step;
    auto * filter = typeid_cast<FilterStep *>(parent.get());
    if (!filter)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    auto & child = child_node->step;
    auto * join = typeid_cast<JoinStepLogical *>(child.get());
    if (!join || child_node->children.size() != 2)
        return 0;

    /// The Join engine requires its declared join kind and strictness to remain unchanged. A prepared
    /// key-value storage can fill a missing key with the type default on a direct lookup but with the
    /// column default when read as an ordinary stream, and which applies is decided after this pass.
    auto isPreparedJoinStorage = [](auto & step)
    {
        auto * lookup_step = typeid_cast<JoinStepLogicalLookup *>(step.get());
        return lookup_step && static_cast<bool>(lookup_step->getPreparedJoinStorage());
    };
    if (isPreparedJoinStorage(child_node->children.back()->step))
        return 0;

    /// ANY JOIN can become SEMI or ANTI. ALL JOIN can only become ANTI: it emits a row per match where SEMI emits one,
    /// but when the filter rejects every matched row, both keep exactly one row for each unmatched one.
    auto & join_operator = join->getJoinOperator();
    const auto strictness = join_operator.strictness;
    if (strictness != JoinStrictness::Any && strictness != JoinStrictness::All)
        return 0;

    if (!isLeftOrRight(join_operator.kind))
        return 0;

    /// Both conversions below are only worth doing if the join can still be executed afterwards.
    const auto & join_settings = join->getJoinSettings();
    const bool semi_can_be_executed = strictness == JoinStrictness::Any
        && JoinCommon::canBeExecutedByEnabledAlgorithm(join_settings, join_operator.kind, JoinStrictness::Semi);
    const bool anti_can_be_executed
        = JoinCommon::canBeExecutedByEnabledAlgorithm(join_settings, join_operator.kind, JoinStrictness::Anti);

    if (!semi_can_be_executed && !anti_can_be_executed)
        return 0;

    const auto & filter_dag = filter->getExpression();

    /// Not-built sets keep the join ANY: not every join algorithm can run SEMI.
    if (dagContainsNonReadySet(filter_dag))
        return 0;

    const auto & filter_column_name = filter->getFilterColumnName();

    /// The side which may have no match for a row of the other one, so its columns are filled with defaults.
    const auto matched_side = join_operator.kind == JoinKind::Left ? JoinTableSide::Right : JoinTableSide::Left;

    /// Only needed to prove the filter FALSE for matched rows by a NULL check of a column of the matched side.
    NameSet not_null_columns;
    if (anti_can_be_executed && std::ranges::any_of(filter_dag.getNodes(), isNullCheck))
        not_null_columns = notNullOutputsOfMatchedRows(*join, matched_side);

    if (strictness == JoinStrictness::All && not_null_columns.empty())
        return 0;

    auto result_for_matched_rows = filterResultForMatchedRows(
        getPreFilterActionsDAG(child_node, join, matched_side), filter_dag, filter_column_name, not_null_columns);

    if (!semi_can_be_executed && result_for_matched_rows != FilterResult::FALSE)
        return 0;

    const auto & matched_side_header = *join->getInputHeaders()[matched_side == JoinTableSide::Left ? 0 : 1];
    auto result_for_not_matched_rows = filterResultForNotMatchedRows(filter_dag, filter_column_name, matched_side_header);

    if (result_for_not_matched_rows == FilterResult::FALSE && semi_can_be_executed)
    {
        LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Converting ANY JOIN to SEMI JOIN");
        join_operator.strictness = JoinStrictness::Semi;
        if (result_for_matched_rows == FilterResult::TRUE && isSimpleFilter(filter_dag.tryFindInOutputs(filter_column_name), false))
        {
            /// Remove filter after SEMI JOIN because it's a constant expression that always evaluates to TRUE for matched rows
            LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Removing filter '{}' after SEMI JOIN because it's always TRUE for matched rows", filter_column_name);
            parent_node->step = convertToExpressionStep(filter);
        }
        return 2;
    }
    if (result_for_matched_rows == FilterResult::FALSE && anti_can_be_executed)
    {
        LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Converting {} JOIN to ANTI JOIN", toString(strictness));
        join_operator.strictness = JoinStrictness::Anti;
        if (result_for_not_matched_rows == FilterResult::TRUE && isSimpleFilter(filter_dag.tryFindInOutputs(filter_column_name), true))
        {
            /// Remove filter after ANTI JOIN because it's a constant expression that always evaluates to TRUE for not matched rows
            LOG_DEBUG(getLogger("QueryPlanConvertAnyJoinToSemiOrAntiJoin"), "Removing filter '{}' after ANTI JOIN because it's always TRUE for not matched rows", filter_column_name);
            parent_node->step = convertToExpressionStep(filter);
        }
        return 2;
    }
    return 0;
}

}
