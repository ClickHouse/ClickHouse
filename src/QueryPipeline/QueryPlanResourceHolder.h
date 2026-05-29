#pragma once
#include <Common/VectorWithMemoryTracking.h>
#include <Storages/TableLockHolder.h>
#include <memory>

namespace DB
{

class QueryPipelineBuilder;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class QueryPlan;
class Context;
struct QueryIdHolder;
class InsertDependenciesBuilder;
using InsertDependenciesBuilderConstPtr = std::shared_ptr<const InsertDependenciesBuilder>;

/// Base class for holding any other resources up till the end of query execution.
class ICustomResourceHolder
{
public:
    virtual ~ICustomResourceHolder() = default;
};

struct QueryPlanResourceHolder
{
    QueryPlanResourceHolder();
    QueryPlanResourceHolder(QueryPlanResourceHolder &&) noexcept;
    ~QueryPlanResourceHolder();

    QueryPlanResourceHolder & operator=(QueryPlanResourceHolder &) = delete;

    /// Custom move assignment does not destroy data from lhs. It appends data from rhs to lhs.
    /// Note: append (and thus this assignment) allocates through the memory-tracking containers,
    /// so it can throw MEMORY_LIMIT_EXCEEDED and must not be noexcept.
    QueryPlanResourceHolder & operator=(QueryPlanResourceHolder &&);
    QueryPlanResourceHolder & append(const QueryPlanResourceHolder & rhs);

    /// Some processors may implicitly use Context or temporary Storage created by Interpreter.
    /// But lifetime of Streams is not nested in lifetime of Interpreters, so we have to store it here,
    /// because QueryPipeline is alive until query is finished.
    VectorWithMemoryTracking<std::shared_ptr<const Context>> interpreter_context;
    VectorWithMemoryTracking<StoragePtr> storage_holders;
    VectorWithMemoryTracking<TableLockHolder> table_locks;
    VectorWithMemoryTracking<std::shared_ptr<QueryIdHolder>> query_id_holders;
    VectorWithMemoryTracking<InsertDependenciesBuilderConstPtr> insert_dependencies_holders;
    VectorWithMemoryTracking<std::shared_ptr<ICustomResourceHolder>> custom_resources;
};

}
