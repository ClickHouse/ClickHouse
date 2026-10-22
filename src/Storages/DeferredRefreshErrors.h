#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <base/defines.h>

#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace DB
{

class QueryStatus;

/// Errors of `updateExternalDynamicMetadataIfExists` on the tables that a `Merge` table or a materialized view reads,
/// kept per query, so that they fail only a query that reads such a table. A query is identified by its process list
/// element, which all its contexts share, including the ones made for `SQL SECURITY DEFINER`.
class DeferredRefreshErrors
{
public:
    /// Runs `refresh_table`; its exception is kept for `table` in the query of `context` instead of being thrown.
    void refresh(const ContextPtr & context, const StorageID & table, const std::function<void()> & refresh_table);

    /// Rethrows the exception kept for `table` in the query of `context`, if any.
    void rethrow(const ContextPtr & context, const StorageID & table) const;

private:
    struct QueryErrors
    {
        std::weak_ptr<const QueryStatus> query;
        std::unordered_map<StorageID, std::exception_ptr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual> errors;
    };

    mutable std::mutex mutex;
    std::vector<QueryErrors> queries TSA_GUARDED_BY(mutex);
};

}
