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

size_t tryConvertJoinToSubquery(QueryPlan::Node * parent_node[[maybe_unused]], QueryPlan::Nodes & nodes[[maybe_unused]], const Optimization::ExtraSettings & /*settings*/)
{
    return 0;
}

}
