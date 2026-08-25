#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoin.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

/// Lower an `arrayJoin` function inside an Expression/Filter into a real ArrayJoinStep, so it goes through
/// the same machinery as the ARRAY JOIN clause (lazy replication, and the fuseFilterIntoArrayJoin pass).
/// Peels one array join at a time; the driver re-applies the pass for the rest, which is required because
/// independent array joins compose as a cross product and each needs its own step.
size_t tryLowerArrayJoinFunction(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    if (!settings.lower_array_join_function || parent_node->children.size() != 1)
        return 0;

    auto & parent = parent_node->step;
    auto * expression_step = typeid_cast<ExpressionStep *>(parent.get());
    auto * filter_step = typeid_cast<FilterStep *>(parent.get());
    if (!expression_step && !filter_step)
        return 0;

    auto & dag = expression_step ? expression_step->getExpression() : filter_step->getExpression();
    if (!dag.hasArrayJoin())
        return 0;

    /// Extraction splits one block into several, so a stateful function above would see different block
    /// boundaries. mergeExpressions guards the same arrayJoin + stateful pair; bail here for the same reason.
    if (dag.hasStatefulFunctions())
        return 0;

    auto extracted = dag.extractFirstArrayJoin();
    if (!extracted)
        return 0;

    auto * child_node = parent_node->children.front();

    /// Expression/Filter(after) -> ArrayJoin -> Expression(before) -> child
    auto & before_node = nodes.emplace_back();
    before_node.children.push_back(child_node);
    before_node.step = std::make_unique<ExpressionStep>(child_node->step->getOutputHeader(), std::move(extracted->before));
    before_node.step->setStepDescription("Before ARRAY JOIN");

    auto & array_join_node = nodes.emplace_back();
    array_join_node.children.push_back(&before_node);
    array_join_node.step = std::make_unique<ArrayJoinStep>(
        before_node.step->getOutputHeader(),
        ArrayJoin{Names{extracted->array_join_column_name}, /*is_left=*/false},
        /*is_unaligned=*/false,
        settings.max_block_size,
        settings.enable_lazy_columns_replication);
    array_join_node.step->setStepDescription("ARRAY JOIN");

    const auto & array_join_output = array_join_node.step->getOutputHeader();
    QueryPlanStepPtr after_step;
    if (filter_step)
        after_step = std::make_unique<FilterStep>(
            array_join_output, std::move(extracted->after), filter_step->getFilterColumnName(), filter_step->removesFilterColumn());
    else
        after_step = std::make_unique<ExpressionStep>(array_join_output, std::move(extracted->after));
    after_step->setStepDescription(*parent);

    parent = std::move(after_step);
    parent_node->children = {&array_join_node};
    return 3;
}

}
