#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/ApplyWithAliasVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectQueryCoordination.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ReplaceDistributedTableNameVisitor.h>
#include <Optimizer/CostBasedOptimizer.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryCoordination/Coordinator.h>
#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryCoordination/Fragments/FragmentBuilder.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int INCORRECT_QUERY;
}

namespace
{

bool optimizeTrivialCount(const ASTPtr & query, const Settings & settings)
{
    bool optimize_trivial_count = false;
    if (auto * select_with_union_query = query->as<ASTSelectWithUnionQuery>())
    {
        for (const auto & union_child_query : select_with_union_query->list_of_selects->children)
        {
            auto * select_query = union_child_query->as<ASTSelectQuery>();
            /// Skip union t1 union t2 union t3 and except queries
            if (!select_query)
                continue;

            optimize_trivial_count = settings.optimize_trivial_count_query && !select_query->where() && !select_query->prewhere()
                && !select_query->groupBy() && !select_query->having() && !select_query->sampleSize() && !select_query->sampleOffset()
                && !select_query->final();

            if (optimize_trivial_count)
            {
                bool found_function_count = false;
                bool has_other_agg_function = false;
                for (const auto & expr : select_query->select()->as<ASTExpressionList>()->children)
                {
                    if (auto * func = expr->as<ASTFunction>())
                    {
                        if (AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
                        {
                            if (Poco::toLower(func->name) != "count")
                            {
                                has_other_agg_function = true;
                                break;
                            }
                            else
                                found_function_count = true;
                        }
                    }
                }
                optimize_trivial_count = found_function_count && !has_other_agg_function;
            }
        }
    }
    return optimize_trivial_count;
}

void addSettingToQuery(ASTPtr & query, const String & name, const Field & value)
{
    if (auto * select_query = query->as<ASTSelectQuery>())
    {
        if (select_query->settings())
        {
            if (auto * set = select_query->settings()->as<ASTSetQuery>())
                set->changes.setSetting(name, value);
        }
        else
        {
            auto set = std::make_shared<ASTSetQuery>();
            set->changes.setSetting(name, value);
            select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(set));
        }
    }
    else if (auto * union_query = query->as<ASTSelectWithUnionQuery>())
    {
        for (auto & select : union_query->list_of_selects->children)
            addSettingToQuery(select, name, value);
    }
}

}

InterpreterSelectQueryCoordination::InterpreterSelectQueryCoordination(
    const ASTPtr & query_ptr_, ContextPtr context_, const SelectQueryOptions & options_)
    : InterpreterSelectQueryCoordination(query_ptr_, Context::createCopy(context_), options_)
{
    log = &Poco::Logger::get("InterpreterSelectQueryCoordination");
}

InterpreterSelectQueryCoordination::InterpreterSelectQueryCoordination(
    const ASTPtr & query_ptr_, ContextMutablePtr context_, const SelectQueryOptions & options_)
    : query_ptr(query_ptr_), context(context_), options(options_), log(&Poco::Logger::get("InterpreterSelectQueryCoordination"))
{
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY && !options_.is_subquery)
    {
        auto cloned_query = query_ptr->clone();
        auto setting_changes = setIncompatibleSettings();

        /// Expand CET in advance
        if (!options.is_subquery)
        {
            if (context->getSettingsRef().enable_global_with_statement)
                ApplyWithAliasVisitor().visit(cloned_query);
            ApplyWithSubqueryVisitor().visit(cloned_query);
        }

        ReplaceDistributedTableNameVisitor visitor(context);
        visitor.visit(cloned_query);

        if (visitor.has_table_function || visitor.has_local_table)
            context->setDistributedForQueryCoord(false);

        String cluster_name;
        if (visitor.has_distributed_table)
        {
            cluster_name = visitor.clusters[0]->getName();
            /// remote() cluster_name is empty
            if (cluster_name.empty())
                context->setDistributedForQueryCoord(false);

            for (size_t i = 1; i < visitor.clusters.size(); ++i)
                /// multiple cluster
                if (cluster_name != visitor.clusters[i]->getName())
                    context->setDistributedForQueryCoord(false);

            if (context->addQueryCoordinationMetaInfo(cluster_name, visitor.storages, visitor.sharding_keys))
                context->setDistributedForQueryCoord(true);
            else
                context->setDistributedForQueryCoord(false); /// maybe union query
        }

        /// TODO remove the code block when we send query plan instead of SQL to nodes.
        if (visitor.has_non_merge_tree_table)
            context->setDistributedForQueryCoord(false);

        /// Temporarily disable query coordination for trivial count optimization.
        /// The judgment for optimize_trivial_count is not strict, but it is sufficient.
        /// TODO remove the code block when we send query plan instead of SQL to nodes.
        if (optimizeTrivialCount(query_ptr, context->getSettingsRef()))
            context->setDistributedForQueryCoord(false);

        if (context->isDistributedForQueryCoord())
            query_ptr = cloned_query;
        else
            /// restore changed settings
            for (const auto & change : setting_changes)
                context->setSetting(change.name, change.value);

        context->setSetting("allow_experimental_query_coordination", context->isDistributedForQueryCoord());
        LOG_DEBUG(log, "query_coordination_enabled = {}", query_coordination_enabled);
    }
    else
    {
        context->setDistributedForQueryCoord(true);
    }

    query_coordination_enabled = context->isDistributedForQueryCoord();
    LOG_DEBUG(log, "query_coordination_enabled = {}", query_coordination_enabled);
}

/// Disable use_index_for_in_with_subqueries
SettingsChanges InterpreterSelectQueryCoordination::setIncompatibleSettings()
{
    SettingsChanges changes;
    if (context->getSettings().use_index_for_in_with_subqueries)
    {
        context->getSettings().use_index_for_in_with_subqueries = false;
        changes.setSetting("use_index_for_in_with_subqueries", true);
    }
    return changes;
}

static String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return {};

    WriteBufferFromOwnString buf;
    IAST::FormatSettings ast_format_settings(buf, /*one_line*/ true);
    ast_format_settings.hilite = false;
    ast_format_settings.always_quote_identifiers = true;
    ast->format(ast_format_settings);
    return buf.str();
}

void InterpreterSelectQueryCoordination::buildQueryPlanIfNeeded()
{
    if (plan.isInitialized())
        return;
    if (context->getSettingsRef().allow_experimental_analyzer)
        plan = InterpreterSelectQueryAnalyzer(query_ptr, context, options).extractQueryPlan();
    else if (query_ptr->as<ASTSelectQuery>())
        InterpreterSelectQuery(query_ptr, context, options).buildQueryPlan(plan);
    else
        InterpreterSelectWithUnionQuery(query_ptr, context, options).buildQueryPlan(plan);
}

void InterpreterSelectQueryCoordination::optimize()
{
    if (!plan.isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Try to optimize plan but it is not initialized.");

    /// Optimized by RBO rules
    plan.optimize(QueryPlanOptimizationSettings::fromContext(context));

    /// Optimized by CBO optimizer
    if (query_coordination_enabled)
    {
        Stopwatch watch;
        CostBasedOptimizer optimizer;
        plan = optimizer.optimize(std::move(plan), context);
        LOG_DEBUG(log, "CBO optimization time costs {}ms", watch.elapsedMilliseconds());
    }
}

void InterpreterSelectQueryCoordination::buildFragments()
{
    if (query_coordination_enabled)
    {
        FragmentBuilder builder(plan, context);
        FragmentPtr root_fragment = builder.build();

        std::queue<FragmentPtr> queue;
        queue.push(root_fragment);

        while (!queue.empty())
        {
            auto fragment = queue.front();
            queue.pop();
            fragments.emplace_back(fragment);
            for (const auto & child : fragment->getChildren())
                queue.push(child);
        }
    }
}

void InterpreterSelectQueryCoordination::explainFragment(WriteBufferFromOwnString & buf, const Fragment::ExplainFragmentOptions & options_)
{
    if (!query_coordination_enabled)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "EXPLAIN FRAGMENT but query coordination is not enabled.");

    buildQueryPlanIfNeeded();
    optimize();
    buildFragments();

    if (fragments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "EXPLAIN FRAGMENT but there is no fragments.");

    fragments.front()->dump(buf, options_);
}

void InterpreterSelectQueryCoordination::explain(
    WriteBufferFromOwnString & buf, const QueryPlan::ExplainPlanOptions & options_, bool json, bool optimize_)
{
    buildQueryPlanIfNeeded();

    if (optimize_)
        optimize();

    if (json)
    {
        /// Add extra layers to make plan look more like from postgres.
        auto plan_map = std::make_unique<JSONBuilder::JSONMap>();

        plan_map->add("Plan", plan.explainPlan(options_));
        auto plan_array = std::make_unique<JSONBuilder::JSONArray>();
        plan_array->add(std::move(plan_map));

        auto format_settings = getFormatSettings(getContext());
        format_settings.json.quote_64bit_integers = false;

        JSONBuilder::FormatSettings json_format_settings{.settings = format_settings};
        JSONBuilder::FormatContext format_context{.out = buf};

        plan_array->format(json_format_settings, format_context);
    }
    else
    {
        plan.explainPlan(buf, options_);
    }
}


BlockIO InterpreterSelectQueryCoordination::execute()
{
    BlockIO res;

    buildQueryPlanIfNeeded();
    optimize();

    if (query_coordination_enabled)
    {
        buildFragments();

        /// save fragments wait for be scheduled
        res.query_coord_state.fragments = fragments;

        /// schedule fragments
        if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        {
            Coordinator coord(fragments, context, formattedAST(query_ptr));
            coord.schedulePrepareDistributedPipelines();

            /// local already be scheduled
            res.query_coord_state.pipelines = std::move(coord.pipelines);
            res.query_coord_state.remote_host_connection = coord.getRemoteHostConnection();
            res.pipeline = res.query_coord_state.pipelines.detachRootPipeline();

            /// TODO quota only use to root pipeline?
            if (!options.ignore_quota)
                res.pipeline.setQuota(context->getQuota());
        }
    }
    else
    {
        auto builder = plan.buildQueryPipeline(
            QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));

        res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        if (!options.ignore_quota)
            res.pipeline.setQuota(context->getQuota());
    }

    return res;
}


void registerInterpreterSelectQueryCoordination(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterSelectQueryCoordination>(args.query, args.context, args.options);
    };
    factory.registerInterpreter("InterpreterSelectQueryCoordination", create_fn);
}

}
