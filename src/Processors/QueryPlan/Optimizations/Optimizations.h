#include <Processors/QueryPlan/QueryPlan.h>
#include <array>

namespace DB
{

namespace QueryPlanOptimizations
{

/// This is the main function which optimizes the whole QueryPlan tree.
void optimizeTree(QueryPlan::Node & root, QueryPlan::Nodes & nodes);

/// Optimization is a function applied to QueryPlan::Node.
/// It can read and update subtree of specified node.
/// It return true if some change of thee happened.
/// New nodes should be added to QueryPlan::Nodes list.
/// It is not needed to remove old nodes from the list.
///
/// Optimization must guarantee that:
///  * the structure of tree is correct
///  * no more then `read_depth` layers of subtree was read
///  * no more then `update_depth` layers of subtree was updated
struct Optimization
{
    using Function = bool (*)(QueryPlan::Node *, QueryPlan::Nodes &);
    const Function run = nullptr;
    const size_t read_depth;
    const size_t update_depth;
};

/// Move ARRAY JOIN up if possible.
bool tryLiftUpArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes);

/// Move LimitStep down if possible.
bool tryPushDownLimit(QueryPlan::Node * parent_node, QueryPlan::Nodes &);

/// Split FilterStep into chain `ExpressionStep -> FilterStep`, where FilterStep contains minimal number of nodes.
bool trySplitFilter(QueryPlan::Node * node, QueryPlan::Nodes & nodes);

/// Replace chain `ExpressionStep -> ExpressionStep` to single ExpressionStep
/// Replace chain `FilterStep -> ExpressionStep` to single FilterStep
bool tryMergeExpressions(QueryPlan::Node * parent_node, QueryPlan::Nodes &);

inline const auto & getOptimizations()
{
    static const std::array<Optimization, 4> optimizations =
    {{
        {tryLiftUpArrayJoin, 2, 3},
        {tryPushDownLimit, 2, 2},
        {trySplitFilter, 1, 2},
        {tryMergeExpressions, 2, 1},
     }};

    return optimizations;
}

}

}
