#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>

namespace DB::QueryPlanOptimizations
{

size_t tryPushDownLimit(QueryPlan::Node * node, QueryPlan::Nodes &)
{
    auto * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (!filter_step)
        return 0;

    QueryPlan::Node * child_node = node->children.front();
    auto & child = child_node->step;

    if (const auto * adding_const_column = typeid_cast<const AddingConstColumnStep *>(child.get()))
    {

    }
}

}
