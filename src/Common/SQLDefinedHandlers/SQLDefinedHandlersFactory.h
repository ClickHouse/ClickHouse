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
    /// How many times a replicated write re-reads the set and retries when it changed concurrently.
    static constexpr size_t max_replicated_write_attempts = 100;

    static SQLDefinedHandlersFactory & instance();

    ~SQLDefinedHandlersFactory();

    /// Returns an immutable snapshot of all handlers, ordered by name (matching priority).
    /// Cheap to call on the HTTP request path.
    SQLDefinedHandlersPtr getAll();

    bool exists(const std::string & handler_name);

    /// Whether handlers are backed by replicated (Keeper) storage and therefore kept in sync across
    /// replicas automatically. Used to strip a redundant ON CLUSTER clause (see removeOnClusterClauseIfNeeded).
    bool isReplicated();

    /// Whether the handler exists in the authoritative state (for replicated storage, read from Keeper and installed locally).
    bool prepareCreate(const String & handler_name);

    /// The stored CREATE HANDLER statement of the handler, read like `prepareCreate`. Throws HANDLER_DOESNT_EXIST.
    String prepareAlter(const String & handler_name);

    void createFromSQL(const ASTCreateHandlerQuery & query);
    void removeFromSQL(const ASTDropHandlerQuery & query);

    /// Stores `merged` only if the stored statement is still `expected_base`, otherwise writes nothing and returns false.
    bool updateFromSQL(const ASTCreateHandlerQuery & alter_query, const String & expected_base, const ASTCreateHandlerQuery & merged);

    void reloadFromSQL();

    void loadIfNot();

    /// Loads the handlers and, for replicated storage, re-reads them from Keeper, so that the definers registered in
    /// `DefinerDependencies` include the handlers stored on the other replicas. Used before dropping a user.
    void loadLatest();

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
    /// Loads the handlers, and re-reads them from replicated storage.
    void loadLatest(std::lock_guard<std::mutex> & lock);
    /// `reconcile_definers`: `loaded_handlers` is authoritative, so its definers replace the ones in `DefinerDependencies`.
    void rebuildSnapshot(std::lock_guard<std::mutex> & lock, bool reconcile_definers);

    /// Throws AMBIGUOUS_HANDLER if the candidate would ambiguously match the same request
    /// as one of the given handlers (exact/prefix URLs only; regexp cannot be checked).
    /// The handler with the same name as the candidate is ignored (a handler is never ambiguous with itself).
    static void checkAmbiguity(const SQLDefinedHandler & candidate, const SQLDefinedHandlers & handlers);

    /// Create/update a handler in replicated (Keeper) storage. The full set is re-read from Keeper and the
    /// ambiguity check is serialized with the persistent write via optimistic concurrency on the root
    /// version, so two replicas cannot concurrently commit overlapping handlers under different names.
    /// A Keeper write can commit and only then fail to report success (for example, the connection is lost
    /// after the `multi` has been applied), so a thrown error does not prove that nothing changed. Called on
    /// that uncertain path to reconcile the local view with the source of truth before the error is rethrown.
    void resyncAfterUncertainWrite(
        const std::string & handler_name, const SQLDefinedHandlerPtr & written, std::lock_guard<std::mutex> & lock);

    void createReplicated(const ASTCreateHandlerQuery & query, std::lock_guard<std::mutex> & lock);
    bool updateReplicated(
        const ASTCreateHandlerQuery & alter_query, const String & expected_base, const ASTCreateHandlerQuery & merged,
        std::lock_guard<std::mutex> & lock);
    void removeReplicated(const ASTDropHandlerQuery & query, std::lock_guard<std::mutex> & lock);

    void updateFunc();
};

}
