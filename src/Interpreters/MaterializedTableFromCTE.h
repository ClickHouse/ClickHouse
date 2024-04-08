#pragma once

#include <future>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

struct FutureTableFromCTE : public std::enable_shared_from_this<FutureTableFromCTE>
{
    FutureTableFromCTE() = default;
    /// Name of the table
    String name;
    /// The external table that will hold data from the CTE.
    StoragePtr external_table;
    /// The query plan that will produce data for the external_table.
    std::unique_ptr<QueryPlan> source;
    /// When multiple threads try to build the query plan, only one will succeed.
    std::atomic_bool get_permission_to_build_plan{false};
    /// If thread try to build query plan but it returns null, it will wait for this promise to be set.
    std::promise<bool> promise_to_materialize;
    std::shared_future<bool> fully_materialized{promise_to_materialize.get_future()};


    /// Build the query plan that will read from source and write to external_table.
    std::pair<std::unique_ptr<QueryPlan>, std::shared_future<bool>> buildPlanOrGetPromiseToMaterialize(ContextPtr context);
    void setFullyMaterialized() { promise_to_materialize.set_value(true); }
    void setPartiallyMaterialized() { promise_to_materialize.set_value(false); }
    void setExceptionWhileMaterializing(std::exception_ptr e) { promise_to_materialize.set_exception(e); }
};
using FutureTableFromCTEPtr = std::shared_ptr<FutureTableFromCTE>;
using FutureTablesFromCTE = std::vector<FutureTableFromCTEPtr>;

bool materializeFutureTablesIfNeeded(ContextPtr context, const FutureTablesFromCTE & required_future_tables);
}
