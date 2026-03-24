#pragma once

#include <boost/noncopyable.hpp>
#include <Interpreters/Context_fwd.h>
#include <Common/re2.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <mutex>
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
    std::string url_type; /// "exact", "prefix", or "regexp"
    std::vector<std::string> methods; /// e.g. {"GET", "POST"}
    std::string query; /// The SQL query to execute
    std::shared_ptr<const re2::RE2> compiled_regex; /// Pre-compiled regex for "regexp" url_type
};

/// Factory that manages SQL-defined HTTP handlers.
/// Stores handler definitions in memory and persists to local disk or ZooKeeper.
class CustomHandlersFactory : boost::noncopyable
{
public:
    static CustomHandlersFactory & instance();

    bool exists(const std::string & handler_name) const;

    CustomHandlerDefinition get(const std::string & handler_name) const;

    std::optional<CustomHandlerDefinition> tryGet(const std::string & handler_name) const;

    std::vector<CustomHandlerDefinition> getAll() const;

    void create(const ASTCreateHandlerQuery & query);

    void alter(const ASTAlterHandlerQuery & query);

    void remove(const std::string & handler_name);

    bool removeIfExists(const std::string & handler_name);

    /// Initialize storage from config: reads `custom_handlers_storage` section
    /// to select local-disk or zookeeper backend. Falls back to local disk.
    void loadFromConfig(const ContextPtr & context);

    void shutdown();

private:
    mutable std::mutex mutex;
    std::unordered_map<std::string, CustomHandlerDefinition> handlers;

    /// Local-disk storage path (empty if not using local storage)
    std::string metadata_path;

    /// ZooKeeper storage path (empty if not using zookeeper)
    std::string zk_path;
    ContextPtr global_context;

    void loadFromDisk(const std::string & path);
    void loadFromZooKeeper();

    void saveToDisk(const std::string & handler_name) const;
    void removeFromDisk(const std::string & handler_name) const;

    void saveToZooKeeper(const std::string & handler_name) const;
    void removeFromZooKeeper(const std::string & handler_name) const;

    void persist(const std::string & handler_name) const;
    void unpersist(const std::string & handler_name) const;

    CustomHandlerDefinition parseDefinition(const ASTCreateHandlerQuery & create_query) const;
    std::string serializeHandler(const std::string & handler_name) const;
};

}
