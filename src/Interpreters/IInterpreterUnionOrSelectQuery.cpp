#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <Interpreters/QueryLog.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

void IInterpreterUnionOrSelectQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const
{
    elem.query_kind = "Select";
}


QueryPipelineBuilder IInterpreterUnionOrSelectQuery::buildQueryPipeline()
{
    QueryPlan query_plan;

    buildQueryPlan(query_plan);

    return std::move(*query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context)));
}

}
