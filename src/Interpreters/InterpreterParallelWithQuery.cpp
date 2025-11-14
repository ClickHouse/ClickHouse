#include <Interpreters/InterpreterParallelWithQuery.h>

#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>


namespace CurrentMetrics
{
    extern const Metric ParallelWithQueryThreads;
    extern const Metric ParallelWithQueryActiveThreads;
    extern const Metric ParallelWithQueryScheduledThreads;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace Setting
{
    extern const SettingsMaxThreads max_threads;
}


InterpreterParallelWithQuery::InterpreterParallelWithQuery(const ASTPtr & query_, ContextPtr context_)
    : WithContext(context_), query(query_), log(getLogger("ParallelWithQuery"))
{
}

BlockIO InterpreterParallelWithQuery::execute()
{
    const auto & parallel_with_query = query->as<const ASTParallelWithQuery &>();
    const auto & subqueries = parallel_with_query.children;

    const auto & settings = getContext()->getSettingsRef();
    size_t max_threads = settings[Setting::max_threads];

    LOG_TRACE(log, "Executing {} subqueries using {} threads", subqueries.size(), max_threads);

    if (max_threads > 1)
    {
        /// Create a thread pool to call the interpreters of all the subqueries in parallel.
        thread_pool = std::make_unique<ThreadPool>(CurrentMetrics::ParallelWithQueryThreads,
                                                   CurrentMetrics::ParallelWithQueryActiveThreads,
                                                   CurrentMetrics::ParallelWithQueryScheduledThreads,
                                                   max_threads);
        runner = std::make_unique<ThreadPoolCallbackRunnerLocal<void>>(*thread_pool, ThreadName::PARALLEL_WITH_QUERY);
    }

    /// Call the interpreters of all the subqueries - it may produce some pipelines which we combine together into
    /// one combined pipeline.
    executeSubqueries(subqueries);

    if (runner)
        runner->waitForAllToFinishAndRethrowFirstError();

    /// We don't need our thread pool anymore.
    /// (Function executeCombinedPipeline() below will use its own thread pool.)
    runner.reset();
    thread_pool.reset();

    /// Execute the combined pipeline if now we have it.
    executeCombinedPipeline();

    return {};
}


void InterpreterParallelWithQuery::executeSubqueries(const ASTs & subqueries)
{
    auto error_found = std::make_shared<std::atomic<bool>>(false);

    for (const auto & subquery : subqueries)
    {
        if (*error_found)
            break;

        try
        {
            ContextMutablePtr subquery_context = Context::createCopy(context);
            subquery_context->makeQueryContext();
            subquery_context->setCurrentQueryId({});

            auto callback = [this, subquery, subquery_context, error_found]
            {
                if (*error_found)
                    return;
                try
                {
                    executeSubquery(subquery, subquery_context);
                }
                catch (...)
                {
                    *error_found = true;
                    throw;
                }
            };

            if (runner)
                (*runner)(callback);
            else
                callback();
        }
        catch (...)
        {
            *error_found = true;
            throw;
        }
    }
}


void InterpreterParallelWithQuery::executeSubquery(ASTPtr subquery, ContextMutablePtr subquery_context)
{
    auto query_io = executeQuery(subquery->formatWithSecretsOneLine(), subquery_context, QueryFlags{ .internal = true }).second;
    auto & pipeline = query_io.pipeline;

    if (!pipeline.initialized())
    {
        /// The subquery interpreter (called by executeQuery()) has already done all the work.
        return;
    }

    if (!pipeline.completed())
    {
        /// We allow only queries without input and output to be combined using PARALLEL WITH clause.
        String reason;
        if (pipeline.pushing() && pipeline.pulling())
            reason = "has both input and output";
        else if (pipeline.pushing())
            reason = "has input";
        else if (pipeline.pulling())
            reason = "has output";
        chassert(!reason.empty());

        if (subquery->getQueryKind() == IAST::QueryKind::Select)
            reason += " (Use UNION to combine select queries)";

        throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "Query {} can't be combined with other queries using PARALLEL WITH clause because this query {}",
                        subquery->formatForLogging(), reason);
    }

    chassert(pipeline.completed());
    std::lock_guard lock{mutex};
    combined_pipeline.addCompletedPipeline(std::move(pipeline));

    /// TODO: Special processing for ON CLUSTER queries and also for queries related to a replicated database is required.
}


void InterpreterParallelWithQuery::executeCombinedPipeline()
{
    std::lock_guard lock{mutex};
    if (!combined_pipeline.initialized())
        return; /// `combined_pipeline` is empty, skipping

    CompletedPipelineExecutor executor(combined_pipeline);
    executor.execute();
}


void registerInterpreterParallelWithQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterParallelWithQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterParallelWithQuery", create_fn);
}

}
