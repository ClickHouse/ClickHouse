#pragma once
#include <Storages/TableLockHolder.h>
#include <Processors/QueryPlan/QueryIdHolder.h>

namespace DB
{

class QueryPipelineBuilder;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class QueryPlan;
class Context;

struct PipelineResourcesHolder
{
    PipelineResourcesHolder();
    PipelineResourcesHolder(PipelineResourcesHolder &&) noexcept;
    ~PipelineResourcesHolder();
    /// Custom move assignment does not destroy data from lhs. It appends data from rhs to lhs.
    PipelineResourcesHolder& operator=(PipelineResourcesHolder &&) noexcept;

    /// Some processors may implicitly use Context or temporary Storage created by Interpreter.
    /// But lifetime of Streams is not nested in lifetime of Interpreters, so we have to store it here,
    /// because QueryPipeline is alive until query is finished.
    std::vector<std::shared_ptr<const Context>> interpreter_context;
    std::vector<StoragePtr> storage_holders;
    std::vector<TableLockHolder> table_locks;
    std::vector<std::unique_ptr<QueryPlan>> query_plans;
    std::shared_ptr<QueryIdHolder> query_id_holder;
};

}
