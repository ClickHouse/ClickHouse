#pragma once

#include "Processors/QueryPlan/QueryPlan.h"
#include "Storages/IStorage_fwd.h"

namespace DB
{

struct FutureTableFromCTE
{
    /// The external table that will hold data from the CTE.
    StoragePtr external_table;
    /// The query plan that will produce data for the external table.
    std::unique_ptr<QueryPlan> source;
    std::unique_ptr<QueryPlan> build(const ContextPtr & context);
};

using FutureTablesFromCTE = std::map<String, FutureTableFromCTE>;
}
