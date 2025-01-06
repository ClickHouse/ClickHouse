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
    void waitFutures(bool throw_if_error);
    void executeCombinedPipeline();

    ASTPtr query;
    LoggerPtr log;
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<ThreadPoolCallbackRunnerLocal<void>> runner;
    std::atomic<bool> error_found = false;
    QueryPipeline combined_pipeline TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

}
