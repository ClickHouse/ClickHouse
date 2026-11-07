#pragma once

#include <base/types.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeperNodeVersion.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

namespace DB
{

/// Support multi-level namespace names.
using IcebergNamespaceName = std::vector<String>;
/// Joins the levels of a namespace in a REST path. A level must not contain it.
constexpr char NAMESPACE_LEVEL_SEPARATOR = '\x1F';

/// What Keeper stores for a table: the current `metadata.json` on object storage.
struct IcebergTablePointer
{
    String uuid;
    /// Full path of the current `metadata.json`. By convention it is under the `metadata/` directory of the table location, but Iceberg does not require that.
    String metadata_location;
    /// Znode version of the uuid node. Set by `getTable`, checked by `updateTable`.
    ZooKeeperNodeVersion version{};
};

struct UpdateNamespacePropertiesResult
{
    Strings updated;
    Strings removed;
    /// Keys from `removals` that were not present.
    Strings missing;
};

/// Storage backend of the native Iceberg REST catalog (RFC: issue #114697), backed by Keeper.
/// Work in progress, not ready for production use.
/// Layout under `root_path` (one root per warehouse):
///   <root>                                 data: format marker
///   <root>/namespaces/<level>              data: JSON object of namespace properties
///   <root>/namespaces/<level>/namespaces   child namespaces, same shape recursively
///   <root>/namespaces/<level>/tables       one child per table
///   <root>/namespaces/<level>/tables/<t>        data: JSON {"uuid": ..., "location": ...}, written once at create
///   <root>/namespaces/<level>/tables/<t>/<uuid> data: metadata location, the node a commit updates
///   <root>/locations                       data: empty. Its data version guards the index below.
///   <root>/locations/<location>            data: table uuid. One child per table root on object storage.
/// A table commit replaces the uuid node data with a version check, so concurrent commits are serialized by Keeper.
/// Create bumps the version of `locations` in the same multi that adds the index entry, so a create that
/// checked the index against a stale version fails and retries. This keeps table locations disjoint.
/// Namespace levels, table names and locations are encoded with `escapeForFileName`.
class KeeperIcebergRESTCatalogStore
{
public:
    KeeperIcebergRESTCatalogStore(zkutil::GetZooKeeper get_zookeeper_, String root_path_);

    /// Returns false if the namespace already exists. Any other failure throws.
    bool createNamespace(const IcebergNamespaceName & name, const std::map<String, String> & properties);

    bool namespaceExists(const IcebergNamespaceName & name) const;

    std::optional<std::map<String, String>> getNamespaceProperties(const IcebergNamespaceName & name) const;

    /// Applies `removals` first, then `updates`. nullopt if the namespace does not exist.
    std::optional<UpdateNamespacePropertiesResult> updateNamespaceProperties(
        const IcebergNamespaceName & name, const std::map<String, String> & updates, const std::set<String> & removals);

    /// Lists direct children of `parent` or top-level namespaces when `parent` is empty.
    std::vector<IcebergNamespaceName> listNamespaces(const IcebergNamespaceName & parent) const;

    enum class CreateTableResult
    {
        Created,
        TableExists,
        NamespaceMissing,
        /// `location` is equal to, inside, or above the location of another table.
        LocationOverlaps,
    };

    struct CreateTableOutcome
    {
        CreateTableResult result;
        /// Set when `result` is `LocationOverlaps`.
        String conflicting_location;
    };

    /// `table_root` comes without a trailing slash.
    CreateTableOutcome
    createTable(const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & pointer, const String & table_root);

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

    /// One attempt of `createTable`. nullopt means another create ran in between and the attempt must be repeated.
    std::optional<CreateTableOutcome> createTableAttempt(
        const zkutil::ZooKeeperPtr & zookeeper,
        const IcebergNamespaceName & ns,
        const String & table,
        const IcebergTablePointer & pointer,
        const String & table_root);

    String namespacePath(const IcebergNamespaceName & name) const;
    /// Node that holds the direct children of `name`. Empty `name` means the root list.
    String childNamespacesPath(const IcebergNamespaceName & name) const;
    String tablesPath(const IcebergNamespaceName & name) const;
    String tablePath(const IcebergNamespaceName & name, const String & table) const;
    String tableUuidPath(const IcebergNamespaceName & name, const String & table, const String & uuid) const;
    String locationPath(const String & location) const;

    const String root_path;
    /// Parent of one child per table location, used for the overlap check.
    const String locations_path;
    const zkutil::GetZooKeeper get_zookeeper;

    /// The session that passed `initRoot`. Requests wait here while a new session is initialized.
    mutable std::mutex mutex;
    mutable zkutil::ZooKeeperPtr initialized_zookeeper TSA_GUARDED_BY(mutex);
};

using KeeperIcebergRESTCatalogStorePtr = std::shared_ptr<KeeperIcebergRESTCatalogStore>;

}
