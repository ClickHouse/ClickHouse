#include <QueryCoordination/NewOptimizer/Optimizer.h>
#include <QueryCoordination/NewOptimizer/Memo.h>

namespace DB
{

QueryPlan Optimizer::optimize(QueryPlan && plan, ContextPtr context)
{
    /// rewrite plan by rule

    /// init Memo by plan
    Memo memo(std::move(plan), context);

    /// logical equivalent transform
    memo.transform();

    /// enforce properties
    memo.enforce();

    QueryPlan res_plan;
    memo.extractPlan();

    return res_plan;
}

}
