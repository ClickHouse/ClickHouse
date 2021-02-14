#pragma once

#include <Parsers/IAST_fwd.h>
#include <memory>
#include <string>
#include <atomic>

namespace DB
{

class QueryProcess;
class Context;

/// Holder for QueryProcess
///
/// NOTE: Not compatible with PullingPipelineExecutor (since it does not create extra thread),
/// only PullingAsyncPipelineExecutor is supported.
class QueryProcessHolder
{
public:
    QueryProcessHolder(const ASTPtr & ast_, bool internal_, const std::shared_ptr<Context> context_);
    /// Logs the query
    ~QueryProcessHolder();

    /// Can be called from multiple threads
    /// (see PipelineExecutor/PullingAsyncPipelineExecutor)
    void initialize();

private:
    const std::shared_ptr<Context> context;
    std::shared_ptr<QueryProcess> query_process;
    std::atomic<bool> initialized = false;
};

}
