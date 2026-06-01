#pragma once

#include <Common/SQLDefinedHandlers/SQLDefinedHandler.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlersMetadataStorage.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>

#include <boost/noncopyable.hpp>
#include <mutex>


namespace DB
{

class ASTCreateHandlerQuery;
class ASTDropHandlerQuery;

/// In-memory registry of SQL-defined HTTP handlers, backed by SQLDefinedHandlersMetadataStorage.
/// Mirrors NamedCollectionFactory: lazily loaded, kept in sync across replicas via a background task
/// when the storage is replicated (Keeper).
class SQLDefinedHandlersFactory : private boost::noncopyable
{
public:
    static SQLDefinedHandlersFactory & instance();

    ~SQLDefinedHandlersFactory();

    /// Returns an immutable snapshot of all handlers, ordered by name (matching priority).
    /// Cheap to call on the HTTP request path.
    SQLDefinedHandlersPtr getAll();

    bool exists(const std::string & handler_name);

    void createFromSQL(const ASTCreateHandlerQuery & query);
    void removeFromSQL(const ASTDropHandlerQuery & query);
    void updateFromSQL(const ASTCreateHandlerQuery & alter_query);

    void reloadFromSQL();

    void loadIfNot();

    void shutdown();

private:
    mutable std::mutex mutex;
    SQLDefinedHandlers loaded_handlers;
    SQLDefinedHandlersPtr snapshot;

    bool loaded = false;
    std::atomic<bool> shutdown_called = false;
    std::unique_ptr<SQLDefinedHandlersMetadataStorage> metadata_storage;
    BackgroundSchedulePoolTaskHolder update_task;

    const LoggerPtr log = getLogger("SQLDefinedHandlersFactory");

    bool loadIfNot(std::lock_guard<std::mutex> & lock);
    void rebuildSnapshot(std::lock_guard<std::mutex> & lock);

    /// Throws AMBIGUOUS_HANDLER if the candidate would ambiguously match the same request
    /// as an existing handler (exact/prefix URLs only; regexp cannot be checked).
    void checkAmbiguity(const SQLDefinedHandler & candidate, std::lock_guard<std::mutex> & lock) const;

    void updateFunc();
};

}
