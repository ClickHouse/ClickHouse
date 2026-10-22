#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <base/defines.h>

#include <atomic>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
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
    void refresh(const ContextPtr & context, const StoragePtr & table, const std::function<void()> & refresh_table);

    /// Rethrows the exception kept for `table` in the query of `context`, if any.
    void rethrow(const ContextPtr & context, const StoragePtr & table) const;

private:
    /// Matches the same storage object, which a `RENAME` keeps, or a table with the same `StorageID::getShortName`,
    /// for a database that creates a new storage object on every lookup.
    struct KeptError
    {
        std::weak_ptr<const IStorage> storage;
        String short_name;
        std::exception_ptr error;
    };

    struct QueryErrors
    {
        std::weak_ptr<const QueryStatus> query;
        std::vector<KeptError> errors;
    };

    /// Called on every access, so the errors of a finished query are released by the next access.
    void removeFinishedQueries() const TSA_REQUIRES(mutex);

    mutable std::mutex mutex;
    mutable std::vector<QueryErrors> queries TSA_GUARDED_BY(mutex);
    /// `!queries.empty()`, readable without `mutex`.
    mutable std::atomic<bool> has_queries = false;
};

}
