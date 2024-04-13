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

    QueryPlanResourceHolder(const QueryPlanResourceHolder &) = delete;
    QueryPlanResourceHolder & operator=(const QueryPlanResourceHolder &) = delete;

    QueryPlanResourceHolder(QueryPlanResourceHolder && rhs) noexcept;
    QueryPlanResourceHolder & operator=(QueryPlanResourceHolder && rhs);

    ~QueryPlanResourceHolder();

    /// Adds resources from rhs and removes them from rhs
    void add(QueryPlanResourceHolder && rhs) noexcept;

    /// Some processors may implicitly use Context or temporary Storage created by Interpreter.
    /// But lifetime of Streams is not nested in lifetime of Interpreters, so we have to store it here,
    /// because QueryPipeline is alive until query is finished.
    std::vector<std::shared_ptr<const Context>> interpreter_context;
    std::vector<StoragePtr> storage_holders;
    std::vector<TableLockHolder> table_locks;
    std::vector<std::shared_ptr<QueryIdHolder>> query_id_holders;
};

}
