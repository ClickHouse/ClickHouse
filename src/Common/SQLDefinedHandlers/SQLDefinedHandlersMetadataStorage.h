#pragma once

#include <Common/SQLDefinedHandlers/SQLDefinedHandler.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class ASTCreateHandlerQuery;

/// Parses a stored CREATE HANDLER statement (an `ASTCreateHandlerQuery`) with unlimited parser depth and backtracks.
ASTPtr parseStoredHandlerStatement(const String & statement, const String & handler_location);

/// Persistent storage for SQL-defined HTTP handlers.
/// Mirrors NamedCollectionsMetadataStorage: handlers are stored either on local disk or in Keeper,
/// configured via the `query_rules_storage` config section.
class SQLDefinedHandlersMetadataStorage : private WithContext
{
public:
    static std::unique_ptr<SQLDefinedHandlersMetadataStorage> create(const ContextPtr & context);

    /// Read all stored handlers, parsed and ready to match.
    SQLDefinedHandlers getAll() const;
    /// Same, but also return a version token of the whole set (the Keeper root-node data version for
    /// replicated storage; 0 for local). Pass it to `store` to make the cross-replica
    /// "read set -> check ambiguity -> persist" sequence atomic via optimistic concurrency.
    SQLDefinedHandlers getAll(Int32 & version) const;

    /// Persist the canonical CREATE HANDLER statement of a handler. When `expected_root_version >= 0`
    /// (replicated optimistic write) the write commits only if the set was not changed since it was read
    /// at that version; otherwise nothing is written and false is returned so the caller can retry.
    /// Returns true if the handler was persisted.
    bool store(const std::string & handler_name, const String & create_statement, bool replace, Int32 expected_root_version = -1);

    void remove(const std::string & handler_name);
    bool removeIfExists(const std::string & handler_name);
    bool exists(const std::string & handler_name) const;

    void shutdown();

    /// Return true if an update was made (for replicated storage).
    bool waitUpdate();
    bool isReplicated() const;

    /// Mark the snapshot last read by `getAll()` as successfully loaded, so the background update check
    /// stops requesting a retry for it. Must be called only after the returned handlers have been fully
    /// applied; a no-op for non-replicated storage.
    void commitReload() const;

private:
    class IStorage;
    class LocalStorage;
    class ZooKeeperStorage;

    std::shared_ptr<IStorage> storage;

    SQLDefinedHandlersMetadataStorage(std::shared_ptr<IStorage> storage_, ContextPtr context_);

    std::vector<std::string> listHandlers() const;
    std::vector<std::string> listHandlers(Int32 & version) const;
    SQLDefinedHandlers readHandlers(const std::vector<std::string> & handler_names) const;
    SQLDefinedHandlerPtr readHandler(const std::string & handler_name) const;
};

}
