#pragma once

#include <boost/noncopyable.hpp>
#include <Interpreters/Context_fwd.h>
#include <Common/HandlerURLType.h>
#include <Common/SharedMutex.h>
#include <Common/re2.h>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>


namespace DB
{

class ASTCreateHandlerQuery;
class ASTAlterHandlerQuery;

/// Represents a single SQL-defined HTTP handler
struct CustomHandlerDefinition
{
    std::string name;
    std::string url;
    HandlerURLType url_type = HandlerURLType::Exact;
    std::vector<std::string> methods; /// e.g. {"GET", "POST"}
    std::string query; /// The SQL query to execute
    std::shared_ptr<const re2::RE2> compiled_regex; /// Pre-compiled regex for HandlerURLType::Regexp
};

/// Factory that manages SQL-defined HTTP handlers.
/// Stores handler definitions in memory and persists to local disk.
/// ZooKeeper-based storage is planned for a follow-up.
class CustomHandlersFactory : boost::noncopyable
{
public:
    static CustomHandlersFactory & instance();

    bool exists(const std::string & handler_name) const;

    CustomHandlerDefinition get(const std::string & handler_name) const;

    std::optional<CustomHandlerDefinition> tryGet(const std::string & handler_name) const;

    /// Returns a pre-sorted (by name) snapshot of all handlers.
    /// The snapshot is rebuilt only on DDL changes, so this is cheap to call.
    std::vector<CustomHandlerDefinition> getSortedSnapshot() const;

    void create(const ASTCreateHandlerQuery & query);

    void alter(const ASTAlterHandlerQuery & query);

    void remove(const std::string & handler_name);

    bool removeIfExists(const std::string & handler_name);

    /// Initialize storage from config: reads `custom_handlers_storage` section
    /// to select local-disk or zookeeper backend. Falls back to local disk.
    void loadFromConfig(const ContextPtr & context);

private:
    mutable SharedMutex mutex;
    std::unordered_map<std::string, CustomHandlerDefinition> handlers;

    /// Pre-sorted snapshot of all handlers, rebuilt on every DDL change.
    std::vector<CustomHandlerDefinition> sorted_snapshot;

    /// Local-disk storage path (empty if not using local storage)
    std::string metadata_path;

    void loadFromDisk(const std::string & path);

    void saveToDisk(const std::string & handler_name, const std::string & content) const;
    void removeFromDisk(const std::string & handler_name) const;

    void persist(const std::string & handler_name) const;
    void unpersist(const std::string & handler_name) const;

    /// Must be called under exclusive lock after any change to `handlers`.
    void rebuildSortedSnapshot();

    /// Check that the new/altered handler does not create ambiguous URL matching.
    void checkAmbiguity(const CustomHandlerDefinition & def) const;

    CustomHandlerDefinition parseDefinition(const ASTCreateHandlerQuery & create_query) const;
    std::string serializeHandler(const std::string & handler_name) const;
};

}
