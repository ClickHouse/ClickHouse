#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <Interpreters/QueryLog.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

IInterpreterUnionOrSelectQuery::IInterpreterUnionOrSelectQuery(const ASTPtr & query_ptr_, ContextPtr context_, const SelectQueryOptions & options_)
    : query_ptr(query_ptr_)
    , context(Context::createCopy(context_))
    , options(options_)
    , max_streams(context->getSettingsRef().max_threads)
{
    if (options.shard_num)
        context->addLocalScalar(
            "_shard_num",
            Block{{DataTypeUInt32().createColumnConst(1, *options.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}});
    if (options.shard_count)
        context->addLocalScalar(
            "_shard_count",
            Block{{DataTypeUInt32().createColumnConst(1, *options.shard_count), std::make_shared<DataTypeUInt32>(), "_shard_count"}});
    if (options.shard_host_name)
        context->addLocalScalar(
            "_shard_host_name",
            Block{{DataTypeString().createColumnConst(1, *options.shard_host_name), std::make_shared<DataTypeString>(), "_shard_host_name"}});
    if (options.shard_port)
        context->addLocalScalar(
            "_shard_port",
            Block{{DataTypeUInt16().createColumnConst(1, *options.shard_port), std::make_shared<DataTypeUInt16>(), "_shard_port"}});
}

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
