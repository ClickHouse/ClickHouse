#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>

#include <Common/checkStackSize.h>
#include <Core/Settings.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ConvertingActions.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t shard_num,
    size_t shard_count,
    bool has_missing_objects,
    bool build_logical_plan,
    const std::string & default_database)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();
    auto new_context = Context::createCopy(context);

    if (build_logical_plan && !default_database.empty())
        new_context->setCurrentDatabase(default_database);

    /// Do not push down limit to local plan, as it will break `rows_before_limit_at_least` counter.
    if (!build_logical_plan && processed_stage == QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit)
        processed_stage = QueryProcessingStage::WithMergeableStateAfterAggregation;

    /// Do not apply AST optimizations, because query
    /// is already optimized and some optimizations
    /// can be applied only for non-distributed tables
    /// and we can produce query, inconsistent with remote plans.
    auto select_query_options = SelectQueryOptions(processed_stage)
        .setShardInfo(static_cast<UInt32>(shard_num), static_cast<UInt32>(shard_count))
        .ignoreASTOptimizations();

    select_query_options.build_logical_plan = build_logical_plan;

    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        /// For Analyzer, identifier in GROUP BY/ORDER BY/LIMIT BY lists has been resolved to
        /// ConstantNode in QueryTree if it is an alias of a constant, so we should not replace
        /// ConstantNode with ProjectionNode again(https://github.com/ClickHouse/ClickHouse/issues/62289).
        new_context->setSetting("enable_positional_arguments", Field(false));
        auto interpreter = InterpreterSelectQueryAnalyzer(query_ast, new_context, select_query_options);
        query_plan = std::make_unique<QueryPlan>(std::move(interpreter).extractQueryPlan());
    }
    else
    {
        auto interpreter = InterpreterSelectQuery(query_ast, new_context, select_query_options);
        interpreter.buildQueryPlan(*query_plan);
    }

    addConvertingActions(*query_plan, header, new_context, has_missing_objects);
    return query_plan;
}

}
