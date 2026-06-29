#include <Processors/QueryPlan/ParallelReplicasSplitStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Interpreters/ClusterProxy/executeQuery.h>

namespace DB
{
namespace QueryPlanOptimizations
{

void applyParallelReplicas(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & );

void applyParallelReplicas(QueryPlan::Node & , QueryPlan::Nodes & , const QueryPlanOptimizationSettings & )
{

}


}

}
