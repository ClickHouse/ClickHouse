#pragma once

#include <Interpreters/Context_fwd.h>

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace DB
{

/// Low-level blob backend for SQL `CREATE SHARD` / `CREATE CLUSTER` catalog rows (one `.sql` file or ZNode per definition).
class IClusterCatalogMetadataBackend
{
public:
    virtual ~IClusterCatalogMetadataBackend() = default;

    virtual bool exists(const std::string & relative_file_name) const = 0;

    /// Local: absolute file paths; Keeper: child node names (e.g. `escaped.sql`).
    virtual std::vector<std::string> list() const = 0;

    virtual std::string read(const std::string & relative_file_name) const = 0;

    virtual void write(const std::string & relative_file_name, const std::string & data, bool replace) = 0;

    virtual void remove(const std::string & relative_file_name) = 0;

    virtual bool removeIfExists(const std::string & relative_file_name) = 0;

    virtual bool isReplicated() const = 0;

    /// Blocks up to `timeout_ms`. Returns true if a refresh may be needed (children changed or first list).
    virtual bool waitUpdate(size_t timeout_ms) = 0;
};

/// Owns a `IClusterCatalogMetadataBackend` chosen from server config — same role as `NamedCollectionsMetadataStorage`
/// for named collections (`type`, `path`, encryption, Keeper, `update_timeout_ms` under `config_prefix`).
class ClusterCatalogMetadataStorage : private WithContext
{
public:
    /// `config_prefix` is typically `shards_catalog_storage` or `clusters_catalog_storage`.
    /// For `local`, `default_local_path` is used when `<prefix>.path` is absent.
    static std::unique_ptr<ClusterCatalogMetadataStorage> create(
        const ContextPtr & context, const std::string & config_prefix, const std::string & default_local_path);

    bool exists(const String & relative_file_name) const;
    std::vector<String> list() const;
    String read(const String & relative_file_name) const;
    void write(const String & relative_file_name, const String & data, bool replace);
    void remove(const String & relative_file_name);
    bool removeIfExists(const String & relative_file_name);

    bool isReplicated() const;

    /// Same contract as `NamedCollectionsMetadataStorage::waitUpdate` (uses `<config_prefix>.update_timeout_ms`).
    bool waitUpdate();

    void shutdown();

private:
    std::shared_ptr<IClusterCatalogMetadataBackend> backend;
    String config_prefix;

    ClusterCatalogMetadataStorage(
        ContextPtr context_, std::shared_ptr<IClusterCatalogMetadataBackend> backend_, String config_prefix_);
};

}
