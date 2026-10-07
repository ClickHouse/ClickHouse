#pragma once

#include <base/types.h>
#include <Common/ZooKeeper/Common.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

/// Support multi-level namespace names.
using IcebergNamespaceName = std::vector<String>;

/// What Keeper stores for a table: the current `metadata.json` on object storage.
struct IcebergTablePointer
{
    String uuid;
    String metadata_location;
    /// Znode version of the uuid node. Set by `getTable`, checked by `updateTable`.
    int32_t version = -1;
};

/// Storage backend of the native Iceberg REST catalog (RFC: issue #114697), backed by Keeper.
/// Work in progress, not ready for production use.
/// Layout under `root_path` (one root per warehouse):
///   <root>                                 data: format marker
///   <root>/namespaces/<level>              data: JSON object of namespace properties
///   <root>/namespaces/<level>/namespaces   child namespaces, same shape recursively
///   <root>/namespaces/<level>/tables       one child per table
///   <root>/namespaces/<level>/tables/<t>        data: table uuid, written once at create
///   <root>/namespaces/<level>/tables/<t>/<uuid> data: metadata location, the node a commit updates
/// A table commit replaces the uuid node data with a version check, so concurrent commits are serialized by Keeper.
/// Namespace levels and table names are encoded with `escapeForFileName`.
class KeeperIcebergRESTCatalogStore
{
public:
    KeeperIcebergRESTCatalogStore(zkutil::GetZooKeeper get_zookeeper_, String root_path_);

    /// Returns false if the namespace already exists. Any other failure throws.
    bool createNamespace(const IcebergNamespaceName & name, const std::map<String, String> & properties);

    bool namespaceExists(const IcebergNamespaceName & name) const;

    std::optional<std::map<String, String>> getNamespaceProperties(const IcebergNamespaceName & name) const;

    /// Lists direct children of `parent` or top-level namespaces when `parent` is empty.
    std::vector<IcebergNamespaceName> listNamespaces(const IcebergNamespaceName & parent) const;

    enum class CreateTableResult
    {
        Created,
        TableExists,
        NamespaceMissing,
    };

    CreateTableResult createTable(const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & pointer);

    std::optional<IcebergTablePointer> getTable(const IcebergNamespaceName & ns, const String & table) const;

    bool tableExists(const IcebergNamespaceName & ns, const String & table) const;

    /// Sorted. nullopt if the namespace does not exist.
    std::optional<Strings> listTables(const IcebergNamespaceName & ns) const;

    bool dropTable(const IcebergNamespaceName & ns, const String & table);

    enum class UpdateTableResult
    {
        Updated,
        /// The node changed since `expected` was read.
        VersionMismatch,
        TableMissing,
        /// The name exists, but it was dropped and re-created with another uuid.
        UuidMismatch,
    };

    /// Replaces the metadata location only if the uuid node still has `expected.version`.
    /// Throws `KeeperException` for session or hardware errors. The caller must treat those as an unknown outcome.
    UpdateTableResult updateTable(
        const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & expected, const IcebergTablePointer & new_pointer);

private:
    /// Returns the current session. Runs `initRoot` once per new session.
    zkutil::ZooKeeperPtr getZooKeeper() const;
    /// Creates the root nodes and checks the format marker.
    void initRoot(const zkutil::ZooKeeperPtr & zookeeper) const;

    String namespacePath(const IcebergNamespaceName & name) const;
    /// Node that holds the direct children of `name`. Empty `name` means the root list.
    String childNamespacesPath(const IcebergNamespaceName & name) const;
    String tablesPath(const IcebergNamespaceName & name) const;
    String tablePath(const IcebergNamespaceName & name, const String & table) const;
    String tableUuidPath(const IcebergNamespaceName & name, const String & table, const String & uuid) const;

    const String root_path;
    const zkutil::GetZooKeeper get_zookeeper;

    /// The session that passed `initRoot`. Requests wait here while a new session is initialized.
    mutable std::mutex mutex;
    mutable zkutil::ZooKeeperPtr initialized_zookeeper TSA_GUARDED_BY(mutex);
};

using KeeperIcebergRESTCatalogStorePtr = std::shared_ptr<KeeperIcebergRESTCatalogStore>;

}
