#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/escapeForFileName.h>
#include <base/unit.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ICEBERG_CATALOG_CONCURRENT_UPDATE;
    extern const int INCORRECT_DATA;
}

namespace
{

constexpr size_t MAX_UPDATE_RETRIES = 10;

/// Keeper limits path depth and node data size. The handler maps `BAD_ARGUMENTS` to a 400 response.
constexpr size_t MAX_NAMESPACE_LEVELS = 16;
constexpr size_t MAX_NAMESPACE_LEVEL_LENGTH = 256;
constexpr size_t MAX_NAMESPACE_PROPERTIES_SIZE = 64_KiB;
constexpr size_t MAX_TABLE_NAME_LENGTH = 256;

void validateNamespace(const IcebergNamespaceName & name)
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace must have at least one level");
    if (name.size() > MAX_NAMESPACE_LEVELS)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace must have at most {} levels", MAX_NAMESPACE_LEVELS);
    for (const auto & level : name)
    {
        if (level.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace levels must be non-empty");
        if (level.size() > MAX_NAMESPACE_LEVEL_LENGTH)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace levels must be at most {} bytes", MAX_NAMESPACE_LEVEL_LENGTH);
        if (level.contains(NAMESPACE_LEVEL_SEPARATOR))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Namespace levels must not contain the unit separator 0x1F");
    }
}

}

void validateTableName(const String & name)
{
    if (name.empty() || name.size() > MAX_TABLE_NAME_LENGTH)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table name must be non-empty and at most {} bytes", MAX_TABLE_NAME_LENGTH);
}

namespace
{

void checkPropertiesSize(const std::map<String, String> & properties)
{
    size_t size = 0;
    for (const auto & [key, value] : properties)
        size += key.size() + value.size();
    if (size > MAX_NAMESPACE_PROPERTIES_SIZE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'properties' must be at most {} bytes in total", MAX_NAMESPACE_PROPERTIES_SIZE);
}

String propertiesToJSON(const std::map<String, String> & properties)
{
    Poco::JSON::Object json;
    for (const auto & [key, value] : properties)
        json.set(key, value);
    return IcebergRESTCatalog::toJSONString(json);
}

std::map<String, String> propertiesFromJSON(const String & data, const String & path)
{
    const auto json = IcebergRESTCatalog::parseJSONObject(data, fmt::format("Namespace properties at {}", path));

    std::map<String, String> properties;
    for (const auto & [key, value] : *json)
    {
        if (!value.isString())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Namespace property '{}' at {} is not a string", key, path);
        properties[key] = value.extract<String>();
    }
    return properties;
}

/// Data of a table node.
struct TableNode
{
    String uuid;
    /// Table location: the root under which the table writes its data and metadata files.
    String location;
};

String tableNodeToJSON(const TableNode & node)
{
    Poco::JSON::Object json;
    json.set("uuid", node.uuid);
    json.set("location", node.location);
    return IcebergRESTCatalog::toJSONString(json);
}

TableNode tableNodeFromJSON(const String & data, const String & path)
{
    const auto json = IcebergRESTCatalog::parseJSONObject(data, fmt::format("Table node at {}", path));
    for (const auto * field : {"uuid", "location"})
        if (!json->has(field) || !json->get(field).isString())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Table node at {} has no string field '{}'", path, field);
    return TableNode{.uuid = json->getValue<String>("uuid"), .location = json->getValue<String>("location")};
}

}

bool locationsOverlap(const String & a, const String & b)
{
    if (a == b)
        return true;
    if (a.size() < b.size())
        return b.starts_with(a) && b[a.size()] == '/';
    return a.starts_with(b) && a[b.size()] == '/';
}

KeeperIcebergRESTCatalogStore::KeeperIcebergRESTCatalogStore(zkutil::GetZooKeeper get_zookeeper_, String root_path_)
    : root_path(std::move(root_path_))
    , locations_path(root_path + "/locations")
    , get_zookeeper(std::move(get_zookeeper_))
{
}

zkutil::ZooKeeperPtr KeeperIcebergRESTCatalogStore::getZooKeeper() const
{
    /// Cheap on the hot path. Only a new session pays for `sync`.
    std::lock_guard lock(mutex);
    auto zookeeper = get_zookeeper();
    if (zookeeper != synced_zookeeper)
    {
        /// A new session may talk to another replica. Avoid reading stale state.
        zookeeper->sync(root_path);
        synced_zookeeper = zookeeper;
    }
    return zookeeper;
}

String KeeperIcebergRESTCatalogStore::namespacePath(const IcebergNamespaceName & name) const
{
    /// Every method builds its path here, so this is the one place that checks the name.
    validateNamespace(name);
    String path = root_path;
    for (const auto & level : name)
        path += "/namespaces/" + escapeForFileName(level);
    return path;
}

String KeeperIcebergRESTCatalogStore::childNamespacesPath(const IcebergNamespaceName & name) const
{
    if (name.empty())
        return root_path + "/namespaces";
    return namespacePath(name) + "/namespaces";
}

String KeeperIcebergRESTCatalogStore::tablesPath(const IcebergNamespaceName & name) const
{
    return namespacePath(name) + "/tables";
}

String KeeperIcebergRESTCatalogStore::tablePath(const IcebergNamespaceName & name, const String & table) const
{
    /// Every table method builds its path here, so this is the one place that checks the table name.
    validateTableName(table);
    return tablesPath(name) + "/" + escapeForFileName(table);
}

String KeeperIcebergRESTCatalogStore::tableUuidPath(const String & uuid) const
{
    /// The table name is not in this path, so a rename keeps the node and its version.
    return root_path + "/tables_by_uuid/" + uuid;
}

String KeeperIcebergRESTCatalogStore::tableGuardPath(const IcebergNamespaceName & name, const String & table, const String & uuid) const
{
    return tablePath(name, table) + "/" + uuid;
}

String KeeperIcebergRESTCatalogStore::locationPath(const String & location) const
{
    return locations_path + "/" + escapeForFileName(location);
}

KeeperIcebergRESTCatalogStore::CreateNamespaceResult
KeeperIcebergRESTCatalogStore::createNamespace(const IcebergNamespaceName & name, const std::map<String, String> & properties)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::createNamespace");
    /// Validates the whole name before any parent is created.
    const auto path = namespacePath(name);
    checkPropertiesSize(properties);
    auto zookeeper = getZooKeeper();

    /// Each namespace is three nodes. Create them atomically so a crash cannot leave a partial namespace.
    const auto try_create = [&](const String & node_path, const std::map<String, String> & props)
    {
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(node_path, propertiesToJSON(props), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(node_path + "/namespaces", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(node_path + "/tables", "", zkutil::CreateMode::Persistent));
        Coordination::Responses responses;
        const auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
            return CreateNamespaceResult::NamespaceExists;
        if (code == Coordination::Error::ZNONODE)
            return CreateNamespaceResult::ParentMissing;

        zkutil::KeeperMultiException::check(code, ops, responses);
        return CreateNamespaceResult::Created;
    };

    /// Keep the tree walkable from the root.
    for (size_t level = 1; level < name.size(); ++level)
    {
        const auto parent_path = namespacePath(IcebergNamespaceName(name.begin(), name.begin() + level));
        if (!zookeeper->exists(parent_path))
            try_create(parent_path, {});
    }

    return try_create(path, properties);
}

bool KeeperIcebergRESTCatalogStore::namespaceExists(const IcebergNamespaceName & name) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::namespaceExists");
    return getZooKeeper()->exists(namespacePath(name));
}

std::optional<std::map<String, String>> KeeperIcebergRESTCatalogStore::getNamespaceProperties(const IcebergNamespaceName & name) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::getNamespaceProperties");
    const auto path = namespacePath(name);
    String data;
    if (!getZooKeeper()->tryGet(path, data))
        return std::nullopt;
    return propertiesFromJSON(data, path);
}

std::optional<UpdateNamespacePropertiesResult> KeeperIcebergRESTCatalogStore::updateNamespaceProperties(
    const IcebergNamespaceName & name, const std::map<String, String> & updates, const std::set<String> & removals)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::updateNamespaceProperties");
    auto zookeeper = getZooKeeper();
    const auto path = namespacePath(name);

    /// The version check keeps concurrent updates from losing keys.
    for (size_t attempt = 0; attempt < MAX_UPDATE_RETRIES; ++attempt)
    {
        Coordination::Stat stat;
        String data;
        if (!zookeeper->tryGet(path, data, &stat))
            return std::nullopt;

        auto properties = propertiesFromJSON(data, path);
        UpdateNamespacePropertiesResult result;
        for (const auto & key : removals)
        {
            if (properties.erase(key) > 0)
                result.removed.push_back(key);
            else
                result.missing.push_back(key);
        }
        for (const auto & [key, value] : updates)
        {
            properties[key] = value;
            result.updated.push_back(key);
        }

        checkPropertiesSize(properties);

        const auto code = zookeeper->trySet(path, propertiesToJSON(properties), stat.version);
        if (code == Coordination::Error::ZOK)
            return result;
        if (code == Coordination::Error::ZNONODE)
            return std::nullopt;
        if (code != Coordination::Error::ZBADVERSION)
            throw zkutil::KeeperException::fromPath(code, path);
    }

    throw Exception(ErrorCodes::ICEBERG_CATALOG_CONCURRENT_UPDATE, "Cannot update properties of namespace at {}: too many concurrent updates", path);
}

std::vector<IcebergNamespaceName> KeeperIcebergRESTCatalogStore::listNamespaces(const IcebergNamespaceName & parent) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::listNamespaces");
    Strings children;
    const auto code = getZooKeeper()->tryGetChildren(childNamespacesPath(parent), children);
    if (code == Coordination::Error::ZNONODE)
        return {};
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, childNamespacesPath(parent));

    /// Keeper returns children in an unspecified order.
    std::sort(children.begin(), children.end());

    std::vector<IcebergNamespaceName> result;
    result.reserve(children.size());
    for (const auto & child : children)
    {
        auto name = parent;
        name.push_back(unescapeForFileName(child));
        result.push_back(std::move(name));
    }
    return result;
}

KeeperIcebergRESTCatalogStore::CreateTableOutcome KeeperIcebergRESTCatalogStore::createTable(
    const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & pointer, const String & table_root)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::createTable");
    auto zookeeper = getZooKeeper();

    /// Every table location is a child of the `locations` node.
    /// All children are read first. Then one transaction writes the table and bumps the version of `locations`.
    /// The version check fails if another create ran in between. Then the children are read again.
    for (size_t attempt = 0; attempt < MAX_UPDATE_RETRIES; ++attempt)
    {
        if (auto outcome = createTableAttempt(zookeeper, ns, table, pointer, table_root))
            return *outcome;
    }

    throw Exception(
        ErrorCodes::ICEBERG_CATALOG_CONCURRENT_UPDATE, "Cannot create table at {}: too many concurrent updates", tablePath(ns, table));
}

std::optional<KeeperIcebergRESTCatalogStore::CreateTableOutcome> KeeperIcebergRESTCatalogStore::createTableAttempt(
    const zkutil::ZooKeeperPtr & zookeeper,
    const IcebergNamespaceName & ns,
    const String & table,
    const IcebergTablePointer & pointer,
    const String & table_root)
{
    const auto table_path = tablePath(ns, table);

    /// Step 1. Check all table locations for a conflict and remember the version of `locations`.
    Coordination::Stat stat;
    if (auto conflict = checkForLocationConflict(zookeeper, table_path, table_root, &stat))
        return conflict;

    /// Step 2. Write everything in one transaction.
    Coordination::Requests ops;

    /// Bump the version of `locations`. Fails if a table was created since step 1.
    ops.emplace_back(zkutil::makeSetRequest(locations_path, "", stat.version));

    /// Location node. Named after the table location. Holds no data.
    const size_t location_op = ops.size();
    ops.emplace_back(zkutil::makeCreateRequest(locationPath(table_root), "", zkutil::CreateMode::Persistent));

    /// Table node. Holds the uuid and the table location.
    ops.emplace_back(zkutil::makeCreateRequest(
        table_path, tableNodeToJSON({.uuid = pointer.uuid, .location = table_root}), zkutil::CreateMode::Persistent));

    /// Guard child. Binds the name to the uuid.
    ops.emplace_back(zkutil::makeCreateRequest(tableGuardPath(ns, table, pointer.uuid), "", zkutil::CreateMode::Persistent));

    /// Uuid node. Holds the full path of the current `metadata.json`.
    ops.emplace_back(zkutil::makeCreateRequest(tableUuidPath(pointer.uuid), pointer.metadata_location, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    const auto multi_code = zookeeper->tryMulti(ops, responses);
    if (multi_code == Coordination::Error::ZOK)
        return CreateTableOutcome{.result = CreateTableResult::Created, .conflicting_location = {}};

    /// Step 3. The transaction failed. Find out why.

    /// Another create changed `locations`. The caller scans it again.
    if (multi_code == Coordination::Error::ZBADVERSION)
        return std::nullopt;

    /// The table node or the uuid node already exists. The uuid of a staged create comes from the client, so it can be taken.
    /// The location create cannot fail: the version check passed, so no location was added since step 1.
    if (multi_code == Coordination::Error::ZNODEEXISTS && zkutil::getFailedOpIndex(multi_code, responses) != location_op)
        return CreateTableOutcome{.result = CreateTableResult::TableExists, .conflicting_location = {}};

    /// The parent `tables` node is created with the namespace, and the other parents with the warehouse.
    /// If one is missing, the namespace or the warehouse is missing.
    if (multi_code == Coordination::Error::ZNONODE)
        return CreateTableOutcome{.result = CreateTableResult::NamespaceMissing, .conflicting_location = {}};

    /// Anything else is a Keeper error, such as a lost session.
    zkutil::KeeperMultiException::check(multi_code, ops, responses);
    UNREACHABLE();
}

std::optional<KeeperIcebergRESTCatalogStore::CreateTableOutcome> KeeperIcebergRESTCatalogStore::checkForLocationConflict(
    const IcebergNamespaceName & ns, const String & table, const String & table_root) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::checkForLocationConflict");
    return checkForLocationConflict(getZooKeeper(), tablePath(ns, table), table_root);
}

std::optional<KeeperIcebergRESTCatalogStore::CreateTableOutcome> KeeperIcebergRESTCatalogStore::checkForLocationConflict(
    const zkutil::ZooKeeperPtr & zookeeper, const String & table_path, const String & table_root, Coordination::Stat * stat) const
{
    Strings existing_table_locations;
    const auto code = zookeeper->tryGetChildren(locations_path, existing_table_locations, stat);
    /// The `locations` node is created with the warehouse. If it is missing, the warehouse was dropped.
    if (code == Coordination::Error::ZNONODE)
        return CreateTableOutcome{.result = CreateTableResult::NamespaceMissing, .conflicting_location = {}};
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, locations_path);

    /// Reject the new location if it is equal to, inside, or above an existing one.
    /// This is a linear scan over all tables of the warehouse. A faster structure is possible, but more complex and likely not needed.
    for (const auto & escaped_location : existing_table_locations)
    {
        auto existing_location = unescapeForFileName(escaped_location);
        if (!locationsOverlap(table_root, existing_location))
            continue;

        /// A repeated create of the same table overlaps its own location. The name conflict is reported first, as the REST spec expects.
        if (zookeeper->exists(table_path))
            return CreateTableOutcome{.result = CreateTableResult::TableExists, .conflicting_location = {}};

        return CreateTableOutcome{.result = CreateTableResult::LocationOverlaps, .conflicting_location = std::move(existing_location)};
    }

    return std::nullopt;
}

std::optional<IcebergTablePointer> KeeperIcebergRESTCatalogStore::getTable(const IcebergNamespaceName & ns, const String & table) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::getTable");
    auto zookeeper = getZooKeeper();
    /// The table node holds the uuid and the table location. Only the uuid is needed here.
    const auto path = tablePath(ns, table);
    String data;
    if (!zookeeper->tryGet(path, data))
        return std::nullopt;
    const auto uuid = tableNodeFromJSON(data, path).uuid;

    /// The uuid node holds the current `metadata.json` path. Missing here means the table was dropped between the two reads.
    String metadata_location;
    Coordination::Stat stat;
    if (!zookeeper->tryGet(tableUuidPath(uuid), metadata_location, &stat))
        return std::nullopt;

    return IcebergTablePointer{.uuid = uuid, .metadata_location = metadata_location, .version = ZooKeeperNodeVersion(stat.version)};
}

bool KeeperIcebergRESTCatalogStore::tableExists(const IcebergNamespaceName & ns, const String & table) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::tableExists");
    return getZooKeeper()->exists(tablePath(ns, table));
}

std::optional<Strings> KeeperIcebergRESTCatalogStore::listTables(const IcebergNamespaceName & ns) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::listTables");
    const auto path = tablesPath(ns);
    Strings children;
    const auto code = getZooKeeper()->tryGetChildren(path, children);
    if (code == Coordination::Error::ZNONODE)
        return std::nullopt;
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, path);

    Strings result;
    result.reserve(children.size());
    for (const auto & child : children)
        result.push_back(unescapeForFileName(child));
    /// Keeper returns children in an unspecified order.
    std::sort(result.begin(), result.end());
    return result;
}

bool KeeperIcebergRESTCatalogStore::dropTable(const IcebergNamespaceName & ns, const String & table)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::dropTable");
    auto zookeeper = getZooKeeper();

    /// The table node holds the uuid and the table location. Both are needed to find the other nodes.
    const auto path = tablePath(ns, table);
    String data;
    if (!zookeeper->tryGet(path, data))
        return false;
    const auto node = tableNodeFromJSON(data, path);

    /// Remove all nodes in one transaction.
    Coordination::Requests ops;

    /// Guard child first. It pins the name to the uuid read above. A table re-created under the same name has another guard child.
    ops.emplace_back(zkutil::makeRemoveRequest(tableGuardPath(ns, table, node.uuid), -1));

    /// Table node.
    ops.emplace_back(zkutil::makeRemoveRequest(path, -1));
    const size_t last_table_op = ops.size() - 1;

    /// Uuid node.
    ops.emplace_back(zkutil::makeRemoveRequest(tableUuidPath(node.uuid), -1));

    /// Location node. This frees the location for other tables.
    ops.emplace_back(zkutil::makeRemoveRequest(locationPath(node.location), -1));

    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZOK)
        return true;

    /// The guard child or the table node is already gone. The table was dropped or renamed concurrently.
    if (code == Coordination::Error::ZNONODE && zkutil::getFailedOpIndex(code, responses) <= last_table_op)
        return false;

    /// Anything else is a Keeper error, such as a lost session.
    zkutil::KeeperMultiException::check(code, ops, responses);
    UNREACHABLE();
}

KeeperIcebergRESTCatalogStore::RenameTableResult KeeperIcebergRESTCatalogStore::renameTable(
    const IcebergNamespaceName & source_ns, const String & source, const IcebergNamespaceName & dest_ns, const String & dest)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::renameTable");
    auto zookeeper = getZooKeeper();

    const auto source_path = tablePath(source_ns, source);
    for (size_t attempt = 0; attempt < MAX_UPDATE_RETRIES; ++attempt)
    {
        /// The table node holds the uuid and the table location. It moves as is, so the table keeps its location.
        String data;
        if (!zookeeper->tryGet(source_path, data))
            return RenameTableResult::TableMissing;
        const auto uuid = tableNodeFromJSON(data, source_path).uuid;

        /// Only the name node moves. The uuid node keeps its version, so a commit that read the table before the rename still conflicts.
        /// The guard child pins the name to the uuid read above. A table re-created under the same name has another guard child, so the source is read again.
        Coordination::Requests ops;
        const size_t guard_op = ops.size();
        ops.emplace_back(zkutil::makeRemoveRequest(tableGuardPath(source_ns, source, uuid), -1));
        ops.emplace_back(zkutil::makeRemoveRequest(source_path, -1));
        const size_t dest_table_op = ops.size();
        ops.emplace_back(zkutil::makeCreateRequest(tablePath(dest_ns, dest), data, zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(tableGuardPath(dest_ns, dest, uuid), "", zkutil::CreateMode::Persistent));

        Coordination::Responses responses;
        const auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZOK)
            return RenameTableResult::Renamed;
        if (code == Coordination::Error::ZNODEEXISTS)
            return RenameTableResult::TargetExists;
        /// The `tables` node is created with the namespace, so a missing parent of the destination means a missing namespace.
        if (code == Coordination::Error::ZNONODE)
        {
            const auto failed_op = zkutil::getFailedOpIndex(code, responses);
            if (failed_op == dest_table_op)
                return RenameTableResult::NamespaceMissing;
            if (failed_op == guard_op)
                continue;
            return RenameTableResult::TableMissing;
        }
        zkutil::KeeperMultiException::check(code, ops, responses);
    }

    throw Exception(
        ErrorCodes::ICEBERG_CATALOG_CONCURRENT_UPDATE,
        "Cannot rename table {} at {}: too many concurrent updates",
        source,
        namespacePath(source_ns));
}

KeeperIcebergRESTCatalogStore::UpdateTablesResult KeeperIcebergRESTCatalogStore::updateTables(const std::vector<TableUpdate> & updates)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::updateTables");
    auto zookeeper = getZooKeeper();
    /// The uuid node pins the table identity. A table re-created under the same name has another uuid, so the set misses it.
    Coordination::Requests ops;
    ops.reserve(updates.size());
    for (const auto & update : updates)
    {
        const auto path = tableUuidPath(update.expected.uuid);
        const auto version = update.expected.version.toInt32();
        if (update.new_pointer.metadata_location.empty())
            ops.emplace_back(zkutil::makeCheckRequest(path, version));
        else
            ops.emplace_back(zkutil::makeSetRequest(path, update.new_pointer.metadata_location, version));
    }

    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZOK)
        return {};

    /// Keeper stops at the first failed operation, so one index names the table that blocked the whole multi.
    const size_t index = zkutil::getFailedOpIndex(code, responses);
    if (code == Coordination::Error::ZBADVERSION)
        return {UpdateTableResult::VersionMismatch, index};
    if (code == Coordination::Error::ZNONODE)
        return {UpdateTableResult::UuidNodeMissing, index};
    zkutil::KeeperMultiException::check(code, ops, responses);
    return {};
}

}
