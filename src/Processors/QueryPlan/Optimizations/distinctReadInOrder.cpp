#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB::QueryPlanOptimizations
{

/// 
size_t tryDistinctReadInOrder(QueryPlan::Node * , QueryPlan::Nodes & )
{
    /// check if storage already read in order
    
    /// find DISTINCT

    /// check if DISTINCT has the same columns as we read from storage

    /// check if sorting is preserved for the DISTINCt columns throught plan
    /// if so, set storage to read in order

    return 0;
}

}
