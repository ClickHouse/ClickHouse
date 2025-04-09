#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Common/logger_useful.h>
#include <Common/Logger.h>

#include <Core/Joins.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinInfo.h>

#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

namespace DB::QueryPlanOptimizations
{

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
    if (join_info.kind == JoinKind::Full)
        return 0;

    auto strictness = join_info.strictness;
    if (strictness == JoinStrictness::Anti || strictness == JoinStrictness::Asof)
        return 0;

    const auto & join_header = child->getOutputHeader();
    const auto & left_stream_header = child->getInputHeaders().front();
    const auto & right_stream_header = child->getInputHeaders().back();

    auto get_available_columns = [&join_header](const Block & input_header)
    {
        Names available_input_columns_for_filter;
        const auto & input_columns_names = input_header.getNames();

        for (const auto & name : input_columns_names)
        {
            if (!join_header.has(name))
                continue;

            /// Skip if type is changed. Push down expression expect equal types.
            if (!input_header.getByName(name).type->equals(*join_header.getByName(name).type))
                continue;

            available_input_columns_for_filter.push_back(name);
        }

        return available_input_columns_for_filter;
    };

    auto left_stream_available_columns = get_available_columns(left_stream_header);
    auto right_stream_available_columns = get_available_columns(right_stream_header);

    LOG_DEBUG(getLogger(__func__), "Trying to merge into JOIN({}):\n{}", join_step->getStepDescription(), filter_step->getExpression().dumpDAG());
    LOG_DEBUG(getLogger(__func__), "Left header:\n{}", toString(left_stream_available_columns));
    LOG_DEBUG(getLogger(__func__), "Right header:\n{}", toString(right_stream_available_columns));

    auto equality_predicates = filter_step->getExpression().splitActionsForJoinCondition(
        filter_step->getFilterColumnName(),
        left_stream_available_columns,
        right_stream_available_columns);

    LOG_DEBUG(getLogger(__func__), "Equalities: {}", equality_predicates.size());
    if (equality_predicates.empty())
        return 0;

    for (auto & predicate :equality_predicates)
    {
        auto lhs_node_name = predicate.first.getOutputs()[0]->result_name;
        auto rhs_node_name = predicate.second.getOutputs()[0]->result_name;

        join_expressions.left_pre_join_actions->mergeNodes(std::move(predicate.first));
        join_expressions.right_pre_join_actions->mergeNodes(std::move(predicate.second));

        join_info.expression.condition.predicates.emplace_back(JoinPredicate{
            .left_node = JoinActionRef(&join_expressions.left_pre_join_actions->findInOutputs(lhs_node_name), join_expressions.left_pre_join_actions.get()),
            .right_node = JoinActionRef(&join_expressions.right_pre_join_actions->findInOutputs(rhs_node_name), join_expressions.right_pre_join_actions.get()),
            .op = PredicateOperator::Equals
        });
    }

    if (join_info.kind == JoinKind::Cross)
        join_info.kind = JoinKind::Inner;
    return 2;
}

}
