#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

namespace QueryPlanOptimizations
{

/// Move LimitStep down if possible.
void tryPushDownLimit(QueryPlanStepPtr & parent, QueryPlan::Node * child_node);

/// Split FilterStep into chain `ExpressionStep -> FilterStep`, where FilterStep contains minimal number of nodes.
bool trySplitFilter(QueryPlan::Node * node, QueryPlan::Nodes & nodes);

/// Replace chain `ExpressionStep -> ExpressionStep` to single ExpressionStep
/// Replace chain `FilterStep -> ExpressionStep` to single FilterStep
bool tryMergeExpressions(QueryPlan::Node * parent_node, QueryPlan::Node * child_node);

/// Move ARRAY JOIN up if possible.
void tryLiftUpArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Node * child_node, QueryPlan::Nodes & nodes);

}

}
