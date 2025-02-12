#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>

namespace DB::QueryPlanOptimizations
{

size_t tryConvertJoinToIn(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes[[maybe_unused]], const Optimization::ExtraSettings & /*settings*/)
{
    auto & parent = parent_node->step;
    auto * join = typeid_cast<JoinStepLogical *>(parent.get());
    if (!join)
        return 0;
    auto & join_info = join->getJoinInfo();
    if (join_info.strictness != JoinStrictness::All)
        return 0;
    /// Todo: investigate
    if (join->getJoinSettings().join_use_nulls)
        return 0;

    const auto & left_input_header = join->getInputHeaders().front();
    const auto & right_input_header = join->getInputHeaders().back();
    const auto & output = join->getOutputHeader();

    bool left = false;
    bool right = false;
    for (const auto & column_with_type_and_name : output)
    {
        left |= left_input_header.has(column_with_type_and_name.name);
        right |= right_input_header.has(column_with_type_and_name.name);
    }

    if (left && right)
        return 0;

    std::cout<<"can optimize"<<std::endl;

    // addCreatingSetsStep(QueryPlan & query_plan, PreparedSetsPtr prepared_sets, ContextPtr context);

    return 0;
}

}
