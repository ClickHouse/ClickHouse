#pragma once
#include <Storages/TableLockHolder.h>

namespace DB
{

class QueryPipelineBuilder;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class QueryPlan;
class Context;

struct QueryIdHolder;

struct QueryPlanResourceHolder
{
    QueryPlanResourceHolder();
    QueryPlanResourceHolder(QueryPlanResourceHolder &&) noexcept;
    ~QueryPlanResourceHolder();
    
    QueryPlanResourceHolder & operator=(QueryPlanResourceHolder &) = delete;

    /// Custom move assignment does not destroy data from lhs. It appends data from rhs to lhs.
    QueryPlanResourceHolder & operator=(QueryPlanResourceHolder &&) noexcept;
    QueryPlanResourceHolder & append(QueryPlanResourceHolder &&) noexcept;

    /// Some processors may implicitly use Context or temporary Storage created by Interpreter.
    /// But lifetime of Streams is not nested in lifetime of Interpreters, so we have to store it here,
    /// because QueryPipeline is alive until query is finished.
    std::vector<std::shared_ptr<const Context>> interpreter_context;
    std::vector<StoragePtr> storage_holders;
    std::vector<TableLockHolder> table_locks;
    std::vector<std::shared_ptr<QueryIdHolder>> query_id_holders;
};

}
