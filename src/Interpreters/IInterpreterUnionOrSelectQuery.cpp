#include <Interpreters/IInterpreterUnionOrSelectQuery.h>

#include <Core/Settings.h>
#include <Interpreters/QueryLog.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/FilterStep.h>


namespace DB
{
namespace Setting
{
    extern const SettingsString additional_result_filter;
    extern const SettingsUInt64 max_bytes_to_read;
    extern const SettingsUInt64 max_bytes_to_read_leaf;
    extern const SettingsSeconds max_estimated_execution_time;
    extern const SettingsUInt64 max_execution_speed;
    extern const SettingsUInt64 max_execution_speed_bytes;
    extern const SettingsSeconds max_execution_time;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_rows_to_read;
    extern const SettingsUInt64 max_rows_to_read_leaf;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 min_execution_speed;
    extern const SettingsUInt64 min_execution_speed_bytes;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsOverflowMode read_overflow_mode_leaf;
    extern const SettingsSeconds timeout_before_checking_execution_speed;
    extern const SettingsOverflowMode timeout_overflow_mode;
}

IInterpreterUnionOrSelectQuery::IInterpreterUnionOrSelectQuery(
    const ASTPtr & query_ptr_, const ContextMutablePtr & context_, const SelectQueryOptions & options_)
    : query_ptr(query_ptr_), context(context_), options(options_), max_streams(context->getSettingsRef()[Setting::max_threads])
{
    /// FIXME All code here will work with the old analyzer, however for views over Distributed tables
    /// it's possible that new analyzer will be enabled in ::getQueryProcessingStage method
    /// of the underlying storage when all other parts of infrastructure are not ready for it
    /// (built with old analyzer).
    context->setSetting("allow_experimental_analyzer", false);

    if (options.shard_num)
        context->addSpecialScalar(
                "_shard_num",
                Block{{DataTypeUInt32().createColumnConst(1, *options.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}});
    if (options.shard_count)
        context->addSpecialScalar(
                "_shard_count",
                Block{{DataTypeUInt32().createColumnConst(1, *options.shard_count), std::make_shared<DataTypeUInt32>(), "_shard_count"}});
}

QueryPipelineBuilder IInterpreterUnionOrSelectQuery::buildQueryPipeline()
{
    QueryPlan query_plan;
    return buildQueryPipeline(query_plan);
}

QueryPipelineBuilder IInterpreterUnionOrSelectQuery::buildQueryPipeline(QueryPlan & query_plan)
{
    buildQueryPlan(query_plan);
    return std::move(*query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context)));
}

static StreamLocalLimits getLimitsForStorage(const Settings & settings, const SelectQueryOptions & options)
{
    StreamLocalLimits limits;
    limits.mode = LimitsMode::LIMITS_TOTAL;
    limits.size_limits = SizeLimits(settings[Setting::max_rows_to_read], settings[Setting::max_bytes_to_read], settings[Setting::read_overflow_mode]);
    limits.speed_limits.max_execution_time = settings[Setting::max_execution_time];
    limits.timeout_overflow_mode = settings[Setting::timeout_overflow_mode];

    /** Quota and minimal speed restrictions are checked on the initiating server of the request, and not on remote servers,
      *  because the initiating server has a summary of the execution of the request on all servers.
      *
      * But limits on data size to read and maximum execution time are reasonable to check both on initiator and
      *  additionally on each remote server, because these limits are checked per block of data processed,
      *  and remote servers may process way more blocks of data than are received by initiator.
      *
      * The limits to throttle maximum execution speed is also checked on all servers.
      */
    if (options.to_stage == QueryProcessingStage::Complete)
    {
        limits.speed_limits.min_execution_rps = settings[Setting::min_execution_speed];
        limits.speed_limits.min_execution_bps = settings[Setting::min_execution_speed_bytes];
    }

    limits.speed_limits.max_execution_rps = settings[Setting::max_execution_speed];
    limits.speed_limits.max_execution_bps = settings[Setting::max_execution_speed_bytes];
    limits.speed_limits.timeout_before_checking_execution_speed = settings[Setting::timeout_before_checking_execution_speed];
    limits.speed_limits.max_estimated_execution_time = settings[Setting::max_estimated_execution_time];

    return limits;
}

StorageLimits IInterpreterUnionOrSelectQuery::getStorageLimits(const Context & context, const SelectQueryOptions & options)
{
    const auto & settings = context.getSettingsRef();

    StreamLocalLimits limits;
    SizeLimits leaf_limits;

    /// Set the limits and quota for reading data, the speed and time of the query.
    if (!options.ignore_limits)
    {
        limits = getLimitsForStorage(settings, options);
        leaf_limits = SizeLimits(settings[Setting::max_rows_to_read_leaf], settings[Setting::max_bytes_to_read_leaf], settings[Setting::read_overflow_mode_leaf]);
    }

    return {limits, leaf_limits};
}

void IInterpreterUnionOrSelectQuery::setQuota(QueryPipeline & pipeline) const
{
    std::shared_ptr<const EnabledQuota> quota;

    if (!options.ignore_quota && (options.to_stage == QueryProcessingStage::Complete))
        quota = context->getQuota();

    pipeline.setQuota(quota);
}

static ASTPtr parseAdditionalPostFilter(const Context & context)
{
    const auto & settings = context.getSettingsRef();
    const String & filter = settings[Setting::additional_result_filter];
    if (filter.empty())
        return nullptr;

    ParserExpression parser;
    return parseQuery(
        parser,
        filter.data(),
        filter.data() + filter.size(),
        "additional filter",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
}

static ActionsDAG makeAdditionalPostFilter(ASTPtr & ast, ContextPtr context, const Block & header)
{
    auto syntax_result = TreeRewriter(context).analyze(ast, header.getNamesAndTypesList());
    String result_column_name = ast->getColumnName();
    auto dag = ExpressionAnalyzer(ast, syntax_result, context).getActionsDAG(false, false);
    const ActionsDAG::Node * result_node = &dag.findInOutputs(result_column_name);
    auto & outputs = dag.getOutputs();
    outputs.clear();
    outputs.reserve(dag.getInputs().size() + 1);
    for (const auto * node : dag.getInputs())
        outputs.push_back(node);

    outputs.push_back(result_node);

    return dag;
}

void IInterpreterUnionOrSelectQuery::addAdditionalPostFilter(QueryPlan & plan) const
{
    if (options.subquery_depth != 0)
        return;

    auto ast = parseAdditionalPostFilter(*context);
    if (!ast)
        return;

    auto dag = makeAdditionalPostFilter(ast, context, plan.getCurrentDataStream().header);
    std::string filter_name = dag.getOutputs().back()->result_name;
    auto filter_step = std::make_unique<FilterStep>(
        plan.getCurrentDataStream(), std::move(dag), std::move(filter_name), true);
    filter_step->setStepDescription("Additional result filter");
    plan.addStep(std::move(filter_step));
}

void IInterpreterUnionOrSelectQuery::addStorageLimits(const StorageLimitsList & limits)
{
    for (const auto & val : limits)
        storage_limits.push_back(val);
}

}
