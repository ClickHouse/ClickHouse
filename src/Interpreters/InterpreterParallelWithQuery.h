#pragma once

#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTParallelWithQuery.h>


namespace DB
{

/// Executes multiple statements in parallel.
class InterpreterParallelWithQuery : public IInterpreter, WithContext
{
public:
    InterpreterParallelWithQuery(const ASTPtr & query_, ContextPtr context_);
    BlockIO execute() override;

private:
    void executeSubqueries(const ASTs & subqueries);
    void executeSubquery(ASTPtr subquery, ContextMutablePtr subquery_context);
    void executeCombinedPipeline();

    std::mutex mutex;

    ASTPtr query;
    LoggerPtr log;
    QueryPipeline combined_pipeline TSA_GUARDED_BY(mutex);

    /// Needed to hold query contexts and run onFinish/onException callback.
    std::vector<BlockIO> io_holders TSA_GUARDED_BY(mutex);

    /// Declared last so they are destroyed first: destroying `runner`/`thread_pool` joins the
    /// worker threads before the members they touch (`mutex`, `combined_pipeline`, `io_holders`)
    /// are torn down. This holds on every teardown path, including an exception thrown out of
    /// executeSubqueries() that skips the explicit reset() calls in execute().
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<ThreadPoolCallbackRunnerLocal<void>> runner;
};

}
