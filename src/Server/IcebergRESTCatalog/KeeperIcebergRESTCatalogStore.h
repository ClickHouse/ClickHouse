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

/// Checked for every table path the store builds. The handler maps `BAD_ARGUMENTS` to a 400 response.
void validateTableName(const String & name);

/// What Keeper stores for a table: the current `metadata.json` on object storage.
struct IcebergTablePointer
{
    String uuid;
    /// Full path of the current `metadata.json`. By convention it is under the `metadata/` directory of the table location, but Iceberg does not require that.
    String metadata_location;
    /// Znode version of the uuid node. Set by `getTable`, checked by `updateTables`.
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
/// Layout under `root_path`, which is `<warehouse node>/<warehouse uuid>`.
/// `<root>/namespaces`, `<root>/tables_by_uuid` and `<root>/locations` are created with the warehouse.
/// A warehouse dropped and created again gets a new uuid. So a request that still holds the old warehouse cannot write into the new one.
///   <root>/namespaces/<level>              data: JSON object of namespace properties
///   <root>/namespaces/<level>/namespaces   child namespaces, same shape recursively
///   <root>/namespaces/<level>/tables       one child per table
///   <root>/namespaces/<level>/tables/<t>   data: JSON {"uuid": ..., "location": ...}, written once at create
///   <root>/namespaces/<level>/tables/<t>/<uuid>  empty guard child, binds the name to the uuid
///   <root>/tables_by_uuid/<uuid>           data: metadata location, the node a commit updates
///   <root>/locations                       data: empty. Its data version guards the index below.
///   <root>/locations/<location>            data: empty. One child per table root on object storage.
/// A table commit replaces the uuid node data with a version check, so concurrent commits are serialized by Keeper.
/// The uuid node is outside the namespace tree, so a rename moves only the name node and the version keeps counting.
/// Drop and rename read the uuid from the name node and then remove the guard child in the same multi-request as the name node.
/// A table re-created under the same name has another guard child, so the multi-request fails instead of acting on the wrong table.
/// Create bumps the version of `locations` in the same multi that adds the index entry, so a create that
/// checked the index against a stale version fails and retries. This keeps table locations disjoint.
/// Namespace levels, table names and locations are encoded with `escapeForFileName`.
class KeeperIcebergRESTCatalogStore
{
public:
    KeeperIcebergRESTCatalogStore(zkutil::GetZooKeeper get_zookeeper_, String root_path_);

    enum class CreateNamespaceResult
    {
        Created,
        NamespaceExists,
        /// The warehouse or a parent namespace was dropped during the call.
        ParentMissing,
    };

    CreateNamespaceResult createNamespace(const IcebergNamespaceName & name, const std::map<String, String> & properties);

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

    /// Reason for which `createTable` would refuse `table_root` now, if any. Read-only, so a concurrent create can change the answer.
    std::optional<CreateTableOutcome>
    checkForLocationConflict(const IcebergNamespaceName & ns, const String & table, const String & table_root) const;

    std::optional<IcebergTablePointer> getTable(const IcebergNamespaceName & ns, const String & table) const;

    bool tableExists(const IcebergNamespaceName & ns, const String & table) const;

    /// Sorted. nullopt if the namespace does not exist.
    std::optional<Strings> listTables(const IcebergNamespaceName & ns) const;

    bool dropTable(const IcebergNamespaceName & ns, const String & table);

    enum class RenameTableResult
    {
        Renamed,
        TableMissing,
        /// The destination namespace does not exist.
        NamespaceMissing,
        TargetExists,
    };

    /// Moves the name node. The uuid node, the metadata location and the commit version stay the same.
    RenameTableResult renameTable(
        const IcebergNamespaceName & source_ns, const String & source, const IcebergNamespaceName & dest_ns, const String & dest);

    enum class UpdateTableResult
    {
        Updated,
        /// The node changed since `expected` was read.
        VersionMismatch,
        /// The table with the expected uuid was dropped. A new table with the same name and another uuid may exist.
        UuidNodeMissing,
    };

    struct TableUpdate
    {
        IcebergNamespaceName ns;
        String table;
        /// The set is applied only if the uuid node still has `expected.version`.
        IcebergTablePointer expected;
        /// An empty metadata location only checks that the node still has `expected.version`.
        IcebergTablePointer new_pointer;
    };

    struct UpdateTablesResult
    {
        UpdateTableResult result = UpdateTableResult::Updated;
        /// Position in `updates` of the table whose check or set failed. Unused when `result` is `Updated`.
        size_t failed_index = 0;
    };

    /// Moves every table pointer in one multi, so either every table moves or none does.
    /// Throws `KeeperException` for session or hardware errors. The caller must treat those as an unknown outcome.
    UpdateTablesResult updateTables(const std::vector<TableUpdate> & updates);

private:
    /// Returns the current session. Syncs the root once per new session.
    zkutil::ZooKeeperPtr getZooKeeper() const;

    /// One attempt of `createTable`. nullopt means another create ran in between and the attempt must be repeated.
    std::optional<CreateTableOutcome> createTableAttempt(
        const zkutil::ZooKeeperPtr & zookeeper,
        const IcebergNamespaceName & ns,
        const String & table,
        const IcebergTablePointer & pointer,
        const String & table_root);

    /// Reads all table locations and compares them with `table_root`. `stat`, if given, receives the version of `locations`.
    std::optional<CreateTableOutcome> checkForLocationConflict(
        const zkutil::ZooKeeperPtr & zookeeper,
        const String & table_path,
        const String & table_root,
        Coordination::Stat * stat = nullptr) const;

    String namespacePath(const IcebergNamespaceName & name) const;
    /// Node that holds the direct children of `name`. Empty `name` means the root list.
    String childNamespacesPath(const IcebergNamespaceName & name) const;
    String tablesPath(const IcebergNamespaceName & name) const;
    String tablePath(const IcebergNamespaceName & name, const String & table) const;
    String tableUuidPath(const String & uuid) const;
    String tableGuardPath(const IcebergNamespaceName & name, const String & table, const String & uuid) const;
    String locationPath(const String & location) const;

    const String root_path;
    /// Parent of one child per table location, used for the overlap check.
    const String locations_path;
    const zkutil::GetZooKeeper get_zookeeper;

    /// The session that was synced. Requests wait here while a new session is synced.
    mutable std::mutex mutex;
    mutable zkutil::ZooKeeperPtr synced_zookeeper TSA_GUARDED_BY(mutex);
};

using KeeperIcebergRESTCatalogStorePtr = std::shared_ptr<KeeperIcebergRESTCatalogStore>;

}
