#pragma once

#include <Core/Types.h>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <optional>
#include <Poco/Path.h>
#include <boost/noncopyable.hpp>

namespace DB
{
class Connection;
class Context;

/// Loads and caches format schemas.
/// This class tries to load a format schema from a couple of sources, in the following order:
/// 1) the local directory specified by calling setDirectory(),
/// 2) from the remote server specified by calling setConnectionParameters().
/// After a format schema is loaded successfully it's cached in memory.
class FormatSchemaLoader : private boost::noncopyable
{
public:
    FormatSchemaLoader(const Context & context_);
    ~FormatSchemaLoader();

    /// Sets a local directory to search schemas.
    void setDirectory(const String & directory_, bool allow_paths_outside_ = false, bool allow_absolute_paths_ = false);

    /// Sets connection parameters to load schema from a remote server.
    void setConnectionParameters(const String & host_, UInt16 port_, const String & user_, const String & password_);

    /// Loads a schema (or retrieve a schema from the cache if it has been loaded before). Throws an exception if failed to load.
    String getSchema(const String & path);

    /// Loads a schema (or retrieve a schema from the cache if it has been loaded before). Returns false if failed to load.
    bool tryGetSchema(const String & path, String & schema);

    /// Finds all paths we have schemas for.
    Strings getAllPaths();

private:
    bool tryGetSchemaFromDirectory(const String & path, String & schema);
    bool tryGetSchemaFromConnection(const String & path, String & schema);
    void getAllPathsFromDirectory(Strings & all_paths);
    void getAllPathsFromConnection(Strings & all_paths);
    size_t sendQueryToConnection(const String & query, Strings & result);

    const Context & context;
    std::optional<Poco::Path> directory;
    bool allow_paths_outside = false;
    bool allow_absolute_paths = false;
    String host;
    UInt16 port;
    String user;
    String password;
    std::unique_ptr<Connection> connection;
    std::unordered_map<String, String> schemas_by_paths_cache;
    std::mutex cache_mutex;
    std::mutex connection_mutex;
};

}
