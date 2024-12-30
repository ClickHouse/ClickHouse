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
    extern const SettingsUInt64 parallel_with_query_max_threads;
}


InterpreterParallelWithQuery::InterpreterParallelWithQuery(const ASTPtr & query_, ContextPtr context_)
    : WithContext(context_), query(query_), log(getLogger("ParallelWithQuery"))
{
}

BlockIO InterpreterParallelWithQuery::execute()
{
    ASTParallelWithQuery & parallel_with_query = query->as<ASTParallelWithQuery &>();

    const auto & settings = getContext()->getSettingsRef();
    size_t max_threads = settings[Setting::parallel_with_query_max_threads];

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

    std::vector<std::future<void>> futures_to_wait;

    auto & subqueries = parallel_with_query.children;
    checkSubqueries(subqueries);
    executeSubqueries(subqueries, schedule);
    waitFutures(/* throw_if_error = */ true);

    return {};
}


void InterpreterParallelWithQuery::checkSubqueries(ASTs & subqueries)
{
    for (const auto & subquery : subqueries)
    {
        if (subquery->getQueryKind() == IAST::QueryKind::Select)
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                            "Select queries can't be combined using PARALLEL WITH clause. "
                            "Use UNION ALL to combine select queries");
        }
    }
}


void InterpreterParallelWithQuery::executeSubqueries(ASTs & subqueries, ThreadPoolCallbackRunnerUnsafe<void> schedule)
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
    /// TODO: Special processing for ON CLUSTER queries and also for queries related to a replicated database is required.
    auto query_io = executeQuery(queryToString(subquery), subquery_context, QueryFlags{ .internal = true }).second;
    auto & pipeline = query_io.pipeline;
    if (pipeline.initialized())
    {
        CompletedPipelineExecutor executor(pipeline);
        executor.execute();
    }
    else
    {
        /// pipeline.initialized() == false if it's a query without input and output.
    }
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

void registerInterpreterParallelWithQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterParallelWithQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterParallelWithQuery", create_fn);
}

}
