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

    /// Read the stored handler, apply an ALTER HANDLER query onto it, and return the merged handler.
    /// Does not persist anything.
    SQLDefinedHandlerPtr buildUpdatedHandler(const ASTCreateHandlerQuery & alter_query) const;

    /// Persist the canonical CREATE HANDLER statement of a handler.
    void store(const std::string & handler_name, const String & create_statement, bool replace);

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
    SQLDefinedHandlerPtr readHandler(const std::string & handler_name) const;
};

}
