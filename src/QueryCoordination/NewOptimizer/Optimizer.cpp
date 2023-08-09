#include <QueryCoordination/NewOptimizer/Optimizer.h>
#include <QueryCoordination/NewOptimizer/Memo.h>

namespace DB
{

QueryPlan Optimizer::optimize(QueryPlan && plan)
{
    /// rewrite plan by rule

    /// init Memo by plan
    Memo memo(std::move(plan));

    /// logical equivalent transform
    memo.transform();

    /// enforce properties
}

}
