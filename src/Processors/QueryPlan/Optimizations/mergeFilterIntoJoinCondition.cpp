#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Core/Joins.h>

#include <Functions/FunctionsLogical.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinInfo.h>

#include <Planner/Utils.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

#include <unordered_map>
#include <vector>

namespace DB::ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace DB::QueryPlanOptimizations
{

namespace
{

auto getInputNodes(const ActionsDAG & filter_dag, const Names & allowed_inputs_names)
{
    std::unordered_set<const ActionsDAG::Node *> allowed_nodes;

    std::unordered_map<std::string_view, std::list<const ActionsDAG::Node *>> inputs_map;
    for (const auto & input_node : filter_dag.getInputs())
        inputs_map[input_node->result_name].emplace_back(input_node);

    for (const auto & name : allowed_inputs_names)
    {
        auto & inputs_list = inputs_map[name];
        if (inputs_list.empty())
            continue;

        allowed_nodes.emplace(inputs_list.front());
        inputs_list.pop_front();
    }

    return allowed_nodes;
}

enum class ExpressionSide : uint8_t
{
    UNKNOWN = 0,
    LEFT,
    RIGHT,
};

std::unordered_set<const ActionsDAG::Node *> getExpressionInputs(const ActionsDAG::Node * expr)
{
    std::unordered_set<const ActionsDAG::Node *> result;

    std::unordered_set<const ActionsDAG::Node *> visited;
    ActionsDAG::NodeRawConstPtrs nodes_to_process = { expr };
    while (!nodes_to_process.empty())
    {
        const auto * current = nodes_to_process.back();
        nodes_to_process.pop_back();

        visited.insert(current);

        if (current->type == ActionsDAG::ActionType::INPUT)
        {
            result.insert(current);
        }
        else
        {
            for (const auto * child : current->children)
            {
                if (!visited.contains(child))
                    nodes_to_process.push_back(child);
            }
        }
    }
    return result;
}

ExpressionSide getExpressionSide(
    const ActionsDAG::Node * expr,
    const std::unordered_set<const ActionsDAG::Node *> & left_allowed_inputs,
    const std::unordered_set<const ActionsDAG::Node *> & right_allowed_inputs
)
{
    auto inputs = getExpressionInputs(expr);

    bool has_left = false;
    for (const auto * input : inputs)
    {
        if (left_allowed_inputs.contains(input))
        {
            has_left = true;
            break;
        }
    }

    bool has_right = false;
    for (const auto * input : inputs)
    {
        if (right_allowed_inputs.contains(input))
        {
            has_right = true;
            break;
        }
    }

    if (has_left && !has_right)
        return ExpressionSide::LEFT;
    else if (!has_left && has_right)
        return ExpressionSide::RIGHT;

    return ExpressionSide::UNKNOWN;
}

struct JoinConditionPart
{
    ActionsDAG left;
    ActionsDAG right;
};

using JoinConditionParts = std::vector<JoinConditionPart>;

JoinConditionPart createConditionPart(const ActionsDAG::Node * lhs, const ActionsDAG::Node * rhs)
{
    auto lhs_dag = ActionsDAG::cloneSubDAG({ lhs }, true);
    auto rhs_dag = ActionsDAG::cloneSubDAG({ rhs }, true);

    return JoinConditionPart{ .left = std::move(lhs_dag), .right = std::move(rhs_dag) };
};

const ActionsDAG::Node & createResultPredicate(
    ActionsDAG & filter_dag,
    const ActionsDAG::Node * original_predicate,
    const ActionsDAG::Node * new_predicate_expr)
{
    if (!original_predicate->result_type->equals(*new_predicate_expr->result_type))
    {
        return filter_dag.addCast(*new_predicate_expr, original_predicate->result_type, original_predicate->result_name);
    }
    else
    {
        return filter_dag.addAlias(*new_predicate_expr, original_predicate->result_name);
    }
};


std::pair<JoinConditionParts, bool> extractActionsForJoinCondition(
    ActionsDAG & filter_dag,
    const std::string & filter_name,
    const Names & left_stream_available_columns,
    const Names & right_stream_available_columns
)
{
    auto * predicate = const_cast<ActionsDAG::Node *>(filter_dag.tryFindInOutputs(filter_name));
    if (!predicate)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Output nodes for ActionsDAG do not contain filter column name {}. DAG:\n{}",
            filter_name,
            filter_dag.dumpDAG());

    /// If condition is constant let's do nothing.
    /// It means there is nothing to push down or optimization was already applied.
    if (predicate->type == ActionsDAG::ActionType::COLUMN)
        return {};

    auto left_stream_allowed_nodes = getInputNodes(filter_dag, left_stream_available_columns);
    auto right_stream_allowed_nodes = getInputNodes(filter_dag, right_stream_available_columns);

    /// Extract all conjuncts from filter expression
    auto conjuncts_list = getConjunctsList(predicate);

    JoinConditionParts result;
    std::unordered_set<const ActionsDAG::Node *> conjuncts_to_replace;
    ActionsDAG::NodeRawConstPtrs rejected_conjuncts;
    rejected_conjuncts.reserve(conjuncts_list.size());

    for (const auto * conjunct : conjuncts_list)
    {
        bool is_equality = conjunct->type == ActionsDAG::ActionType::FUNCTION && conjunct->function_base->getName() == "equals";
        if (is_equality)
        {
            const auto * lhs = conjunct->children[0];
            const auto * rhs = conjunct->children[1];

            /// We need to check if arguments are coming from different sides of JOIN
            auto lhs_side = getExpressionSide(lhs, left_stream_allowed_nodes, right_stream_allowed_nodes);
            auto rhs_side = getExpressionSide(rhs, left_stream_allowed_nodes, right_stream_allowed_nodes);

            if (lhs_side == ExpressionSide::LEFT && rhs_side == ExpressionSide::RIGHT)
            {
                result.emplace_back(createConditionPart(lhs, rhs));
                conjuncts_to_replace.insert(conjunct);
                continue;
            }
            else if (rhs_side == ExpressionSide::LEFT && lhs_side == ExpressionSide::RIGHT)
            {
                result.emplace_back(createConditionPart(rhs, lhs));
                conjuncts_to_replace.insert(conjunct);
                continue;
            }
        }
        rejected_conjuncts.push_back(conjunct);
    }

    bool trivial_filter = rejected_conjuncts.empty();
    if (!result.empty())
    {
        /// There's a non-empty list of extracted condition parts.
        /// After JOIN step these equalities will always evaluate to true.
        for (const auto * & output : filter_dag.getOutputs())
        {
            auto it = conjuncts_to_replace.find(output);
            if (it != conjuncts_to_replace.end())
            {
                output = &filter_dag.addColumn(ColumnWithTypeAndName(
                    output->result_type->createColumnConst(1, 1),
                    output->result_type,
                    output->result_name));
            }
        }

        if (rejected_conjuncts.size() == 1)
        {
            filter_dag.addOrReplaceInOutputs(createResultPredicate(filter_dag, predicate, rejected_conjuncts.front()));
        }
        else if (rejected_conjuncts.size() > 1)
        {
            FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
            filter_dag.addOrReplaceInOutputs(createResultPredicate(
                filter_dag,
                predicate,
                &filter_dag.addFunction(func_builder_and, std::move(rejected_conjuncts), {})));
        }

        filter_dag.removeUnusedActions(/*allow_remove_inputs=*/false);
    }

    return { std::move(result), trivial_filter };
}

}

size_t tryMergeFilterIntoJoinCondition(QueryPlan::Node * parent_node, QueryPlan::Nodes &  /*nodes*/, const Optimization::ExtraSettings &)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * filter_step = typeid_cast<FilterStep *>(parent.get());
    auto * join_step = typeid_cast<JoinStepLogical *>(child.get());

    if (!filter_step || !join_step)
        return 0;

    const auto & join_expressions = join_step->getExpressionActions();
    auto & join_info = join_step->getJoinInfo();

    auto kind = join_info.kind;
    if (kind != JoinKind::Inner && kind != JoinKind::Cross && kind != JoinKind::Comma)
        return 0;

    /// Pushing filter condition into the JOIN can affect the result in case of ANY join.
    /// In ClickHouse all JOINs return columns of both tables, but for SEMI, ANTI joins
    /// it works as ANY join.
    auto strictness = join_info.strictness;
    if (strictness != JoinStrictness::Unspecified && strictness != JoinStrictness::All)
        return 0;

    const auto & join_header = child->getOutputHeader();
    const auto & left_stream_header = child->getInputHeaders().front();
    const auto & right_stream_header = child->getInputHeaders().back();

    auto get_available_columns = [&join_header](const Block & input_header)
    {
        Names available_input_columns_for_filter;
        available_input_columns_for_filter.reserve(input_header.columns());

        for (const auto & input_column : input_header.getColumnsWithTypeAndName())
        {
            if (!join_header.has(input_column.name))
                continue;

            /// Skip if type is changed. Push down expression expect equal types.
            if (!input_column.type->equals(*join_header.getByName(input_column.name).type))
                continue;

            available_input_columns_for_filter.push_back(input_column.name);
        }

        return available_input_columns_for_filter;
    };

    auto left_stream_available_columns = get_available_columns(left_stream_header);
    auto right_stream_available_columns = get_available_columns(right_stream_header);

    auto & filter_dag = filter_step->getExpression();
    auto [equality_predicates, trivial_filter] = extractActionsForJoinCondition(
        filter_dag,
        filter_step->getFilterColumnName(),
        left_stream_available_columns,
        right_stream_available_columns);

    if (equality_predicates.empty())
        return 0;

    for (auto & predicate : equality_predicates)
    {
        auto lhs_node_name = predicate.left.getOutputs()[0]->result_name;
        auto rhs_node_name = predicate.right.getOutputs()[0]->result_name;

        join_expressions.left_pre_join_actions->mergeNodes(std::move(predicate.left));
        join_expressions.right_pre_join_actions->mergeNodes(std::move(predicate.right));

        join_info.expression.condition.predicates.emplace_back(JoinPredicate{
            .left_node = JoinActionRef(&join_expressions.left_pre_join_actions->findInOutputs(lhs_node_name), join_expressions.left_pre_join_actions.get()),
            .right_node = JoinActionRef(&join_expressions.right_pre_join_actions->findInOutputs(rhs_node_name), join_expressions.right_pre_join_actions.get()),
            .op = PredicateOperator::Equals
        });
    }

    if (kind == JoinKind::Cross || kind == JoinKind::Comma)
        join_info.kind = JoinKind::Inner;

    /// Remove FilterStep if filter expression is always true
    if (trivial_filter)
    {
        if (filter_step->removesFilterColumn())
            filter_dag.removeUnusedResult(filter_step->getFilterColumnName());
        parent_node->step = std::make_unique<ExpressionStep>(filter_step->getInputHeaders().front(), std::move(filter_dag));
    }

    return 2;
}

}
