#include <Interpreters/InterpreterParallelWithQuery.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/queryToString.h>
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

    LOG_TRACE(log, "Executing {} subqueries in {} threads", subqueries.size(), max_threads);

    ThreadPoolCallbackRunnerUnsafe<void> schedule;
    std::unique_ptr<ThreadPool> thread_pool;

    if (max_threads > 1)
    {
        thread_pool = std::make_unique<ThreadPool>(
            CurrentMetrics::ParallelWithQueryThreads,
            CurrentMetrics::ParallelWithQueryActiveThreads,
            CurrentMetrics::ParallelWithQueryScheduledThreads,
            max_threads);
        schedule = threadPoolCallbackRunnerUnsafe<void>(*thread_pool, "ParallelWithQry");
    }

    executeSubqueries(subqueries, schedule);
    waitFutures(/* throw_if_error = */ true);
    waitBigPipeline();

    return {};
}


void InterpreterParallelWithQuery::executeSubqueries(const ASTs & subqueries, ThreadPoolCallbackRunnerUnsafe<void> schedule)
{
    for (const auto & subquery : subqueries)
    {
        if (error_found)
            break;

        try
        {
            ContextMutablePtr subquery_context = Context::createCopy(context);
            subquery_context->makeQueryContext();

            auto callback = [this, subquery, subquery_context]
            {
                if (error_found)
                    return;
                try
                {
                    executeSubquery(subquery, subquery_context);
                }
                catch (...)
                {
                    error_found = true;
                    throw;
                }
            };

            if (schedule)
                futures.push_back(schedule(callback, Priority{}));
            else
                callback();
        }
        catch (...)
        {
            error_found = true;
            waitFutures(/* throw_if_error = */ false);
            throw;
        }
    }
}


void InterpreterParallelWithQuery::executeSubquery(ASTPtr subquery, ContextMutablePtr subquery_context)
{
    auto query_io = executeQuery(queryToString(subquery), subquery_context, QueryFlags{ .internal = true }).second;
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

        throw Exception(ErrorCodes::INCORRECT_QUERY, "Query {} can't be combined with other queries using PARALLEL WITH clause because this query {}",
                        subquery->formatForLogging(), reason);
    }

    chassert(pipeline.completed());
    std::lock_guard lock{mutex};
    big_pipeline.addCompletedPipeline(std::move(pipeline));

    /// TODO: Special processing for ON CLUSTER queries and also for queries related to a replicated database is required.
}


void InterpreterParallelWithQuery::waitFutures(bool throw_if_error)
{
    std::exception_ptr error;

    /// Wait for all tasks to finish.
    for (auto & future : futures)
    {
        try
        {
            future.get();
        }
        catch (...)
        {
            if (!error)
                error = std::current_exception();
        }
    }

    if (error)
    {
        if (throw_if_error)
            std::rethrow_exception(error);
        else
            tryLogException(error, log);
    }

    futures.clear();
}


void InterpreterParallelWithQuery::waitBigPipeline()
{
    std::lock_guard lock{mutex};
    if (!big_pipeline.initialized())
        return; /// `big_pipeline` is empty, skipping

    CompletedPipelineExecutor executor(big_pipeline);
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
