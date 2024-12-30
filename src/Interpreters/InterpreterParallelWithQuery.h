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
    void checkSubqueries(ASTs & subqueries);
    void executeSubqueries(ASTs & queries, ThreadPoolCallbackRunnerUnsafe<void> schedule);
    void executeSubquery(ASTPtr subquery, ContextMutablePtr subquery_context);
    void waitFutures(bool throw_if_error);

    ASTPtr query;
    LoggerPtr log;
    std::vector<std::future<void>> futures;
    std::atomic<bool> error_found = false;
};

}
