#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

struct FutureTableFromCTE
{
    FutureTableFromCTE() = default;
    /// Name of the table
    String name;
    /// The external table that will hold data from the CTE.
    StoragePtr external_table;
    /// The query plan that will produce data for the external_table.
    std::unique_ptr<QueryPlan> source;
    /// Build the query plan that will read from source and write to external_table.
    /// Expect the query plan to be execute after build.
    std::unique_ptr<QueryPlan> build(ContextPtr context);
    /// The table has already been built
    std::atomic_bool built{false};
};
using FutureTableFromCTEPtr = std::shared_ptr<FutureTableFromCTE>;
using FutureTablesFromCTE = std::map<String, FutureTableFromCTEPtr>;
}
