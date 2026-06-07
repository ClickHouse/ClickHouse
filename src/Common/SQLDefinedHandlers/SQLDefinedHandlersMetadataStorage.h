#pragma once

#include <Common/SQLDefinedHandlers/SQLDefinedHandler.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class ASTCreateHandlerQuery;

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

    /// Read the stored handler, apply an ALTER HANDLER query onto it, and return the merged handler.
    /// Does not persist anything.
    SQLDefinedHandlerPtr buildUpdatedHandler(const ASTCreateHandlerQuery & alter_query) const;
    /// Same, but applies the ALTER onto an already-read CREATE statement (used on the replicated path to
    /// merge against a version-consistent snapshot instead of issuing another read).
    SQLDefinedHandlerPtr buildUpdatedHandler(const String & base_create_statement, const ASTCreateHandlerQuery & alter_query) const;

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

private:
    class IStorage;
    class LocalStorage;
    class ZooKeeperStorage;

    std::shared_ptr<IStorage> storage;

    SQLDefinedHandlersMetadataStorage(std::shared_ptr<IStorage> storage_, ContextPtr context_);

    std::vector<std::string> listHandlers() const;
    std::vector<std::string> listHandlers(Int32 & version) const;
    SQLDefinedHandlerPtr readHandler(const std::string & handler_name) const;
};

}
