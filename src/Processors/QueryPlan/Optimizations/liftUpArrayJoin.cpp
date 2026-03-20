#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Functions/FunctionFactory.h>

namespace DB::QueryPlanOptimizations
{

size_t tryLiftUpArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;
    auto * expression_step = typeid_cast<ExpressionStep *>(parent.get());
    auto * filter_step = typeid_cast<FilterStep *>(parent.get());
    auto * array_join_step = typeid_cast<ArrayJoinStep *>(child.get());

    if (!(expression_step || filter_step) || !array_join_step)
        return 0;

    const auto & array_join_columns = array_join_step->getColumns();
    const auto & expression = expression_step ? expression_step->getExpression()
                                              : filter_step->getExpression();

    bool skip_throwing_functions = !array_join_step->isUnaligned() && array_join_columns.size() > 1;
    auto split_actions = expression.splitActionsBeforeArrayJoin(array_join_columns, skip_throwing_functions);

    /// No actions can be moved before ARRAY JOIN.
    if (split_actions.first.trivial())
        return 0;

    /// Add new expression step before ARRAY JOIN.
    /// Expression/Filter -> ArrayJoin -> Something
    auto & node = nodes.emplace_back();
    node.children.swap(child_node->children);
    child_node->children.emplace_back(&node);
    /// Expression/Filter -> ArrayJoin -> node -> Something

    /// Aligned ARRAY JOIN with multiple columns is handled by keeping the throwing functions above the ARRAY JOIN instead of adding a filter.
    /// This ensures the array size mismatch invariant is checked before the throwing functions.
    bool needs_filter = !skip_throwing_functions && !array_join_step->isLeft() && split_actions.first.hasThrowingFunctions();
    if (needs_filter)
    {
        /// Insert filter for empty arrays below the expression
        /// Expression/Filter -> ArrayJoin -> node -> Something
        auto & filter_node = nodes.emplace_back();
        filter_node.children.swap(node.children);
        node.children.emplace_back(&filter_node);
        /// Expression/Filter -> ArrayJoin -> node -> filter_node -> Something

        const auto & source_header = filter_node.children.at(0)->step->getOutputHeader();
        ActionsDAG filter_dag(source_header->getColumnsWithTypeAndName());
        auto not_empty_func = FunctionFactory::instance().get("notEmpty", nullptr);

        auto it = array_join_columns.begin();
        const auto * filter_condition = &filter_dag.addFunction(not_empty_func, {&filter_dag.findInOutputs(*it)}, "__array_not_empty" + *it);

        /// In unaligned case any non-empty array keeps the row
        if (array_join_step->isUnaligned())
        {
            auto or_func = FunctionFactory::instance().get("or", nullptr);
            for (++it; it != array_join_columns.end(); ++it)
            {
                const auto * not_empty
                    = &filter_dag.addFunction(not_empty_func, {&filter_dag.findInOutputs(*it)}, "__array_not_empty" + *it);
                filter_condition = &filter_dag.addFunction(or_func, {filter_condition, not_empty}, "__array_not_empty");
            }
        }

        filter_dag.addOrReplaceInOutputs(*filter_condition);
        filter_node.step = std::make_unique<FilterStep>(source_header, std::move(filter_dag), filter_condition->result_name, true);
    }

    node.step = std::make_unique<ExpressionStep>(node.children.at(0)->step->getOutputHeader(),
                                                 std::move(split_actions.first));
    node.step->setStepDescription(*parent);
    array_join_step->updateInputHeader(node.step->getOutputHeader());

    QueryPlanStepPtr new_step;
    if (expression_step)
        new_step = std::make_unique<ExpressionStep>(array_join_step->getOutputHeader(), std::move(split_actions.second));
    else
        new_step = std::make_unique<FilterStep>(array_join_step->getOutputHeader(), std::move(split_actions.second),
                                              filter_step->getFilterColumnName(), filter_step->removesFilterColumn());

    new_step->setStepDescription(fmt::format("{} [split]", parent->getStepDescription()), settings.max_step_description_length);
    parent = std::move(new_step);
    return needs_filter ? 4 : 3;
}

}
