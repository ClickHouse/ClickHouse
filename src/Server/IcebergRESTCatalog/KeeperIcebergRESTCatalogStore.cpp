#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/escapeForFileName.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

/// Data of the root node. Bump on any layout change so old data is refused, not misread.
constexpr auto FORMAT_MARKER = "IcebergRESTCatalog\nformat_version: 1";

String propertiesToJSON(const std::map<String, String> & properties)
{
    Poco::JSON::Object json;
    for (const auto & [key, value] : properties)
        json.set(key, value);
    return toJSONString(json);
}

std::map<String, String> propertiesFromJSON(const String & data, const String & path)
{
    const auto json = parseJSONObject(data, fmt::format("Namespace properties at {}", path));

    std::map<String, String> properties;
    for (const auto & [key, value] : *json)
    {
        if (!value.isString())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Namespace property '{}' at {} is not a string", key, path);
        properties[key] = value.extract<String>();
    }
    return properties;
}

}

KeeperIcebergRESTCatalogStore::KeeperIcebergRESTCatalogStore(zkutil::GetZooKeeper get_zookeeper_, String root_path_)
    : root_path(std::move(root_path_))
    , get_zookeeper(std::move(get_zookeeper_))
{
}

zkutil::ZooKeeperPtr KeeperIcebergRESTCatalogStore::getZooKeeper() const
{
    /// Cheap on the hot path. Only a new session pays for `sync` and `initRoot`.
    std::lock_guard lock(mutex);
    auto zookeeper = get_zookeeper();
    if (zookeeper != initialized_zookeeper)
    {
        /// A new session may talk to another replica. Avoid reading stale state.
        zookeeper->sync(root_path);
        initRoot(zookeeper);
        /// Remember the session only after it passed the check, so a failed check is repeated.
        initialized_zookeeper = zookeeper;
    }
    return zookeeper;
}

void KeeperIcebergRESTCatalogStore::initRoot(const zkutil::ZooKeeperPtr & zookeeper) const
{
    zookeeper->createAncestors(root_path);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(root_path, FORMAT_MARKER, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(root_path + "/namespaces", "", zkutil::CreateMode::Persistent));
    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
        zkutil::KeeperMultiException::check(code, ops, responses);

    const auto marker = zookeeper->get(root_path);
    if (marker != FORMAT_MARKER)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Iceberg REST catalog data at {} has an unsupported format: expected {}, found {}",
            root_path,
            FORMAT_MARKER,
            marker);
}

String KeeperIcebergRESTCatalogStore::namespacePath(const IcebergNamespaceName & name) const
{
    String path = root_path;
    for (const auto & level : name)
        path += "/namespaces/" + escapeForFileName(level);
    return path;
}

String KeeperIcebergRESTCatalogStore::childNamespacesPath(const IcebergNamespaceName & name) const
{
    return namespacePath(name) + "/namespaces";
}

String KeeperIcebergRESTCatalogStore::tablesPath(const IcebergNamespaceName & name) const
{
    return namespacePath(name) + "/tables";
}

String KeeperIcebergRESTCatalogStore::tablePath(const IcebergNamespaceName & name, const String & table) const
{
    return tablesPath(name) + "/" + escapeForFileName(table);
}

String KeeperIcebergRESTCatalogStore::tableUuidPath(const IcebergNamespaceName & name, const String & table, const String & uuid) const
{
    return tablePath(name, table) + "/" + uuid;
}

bool KeeperIcebergRESTCatalogStore::createNamespace(const IcebergNamespaceName & name, const std::map<String, String> & properties)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::createNamespace");
    auto zookeeper = getZooKeeper();

    /// Each namespace is three nodes. Create them atomically so a crash cannot leave a partial namespace.
    const auto try_create = [&](const String & path, const std::map<String, String> & props)
    {
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(path, propertiesToJSON(props), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(path + "/namespaces", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(path + "/tables", "", zkutil::CreateMode::Persistent));
        Coordination::Responses responses;
        const auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZNODEEXISTS)
            return false;

        zkutil::KeeperMultiException::check(code, ops, responses);
        return true;
    };

    /// Keep the tree walkable from the root.
    for (size_t level = 1; level < name.size(); ++level)
    {
        const auto parent_path = namespacePath(IcebergNamespaceName(name.begin(), name.begin() + level));
        if (!zookeeper->exists(parent_path))
            try_create(parent_path, {});
    }

    return try_create(namespacePath(name), properties);
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

KeeperIcebergRESTCatalogStore::CreateTableResult
KeeperIcebergRESTCatalogStore::createTable(const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & pointer)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::createTable");
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(tablePath(ns, table), pointer.uuid, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(
        tableUuidPath(ns, table, pointer.uuid), pointer.metadata_location, zkutil::CreateMode::Persistent));
    Coordination::Responses responses;
    const auto code = getZooKeeper()->tryMulti(ops, responses);
    if (code == Coordination::Error::ZNODEEXISTS)
        return CreateTableResult::TableExists;
    /// The `tables` node is created with the namespace, so a missing parent means a missing namespace.
    if (code == Coordination::Error::ZNONODE)
        return CreateTableResult::NamespaceMissing;
    zkutil::KeeperMultiException::check(code, ops, responses);
    return CreateTableResult::Created;
}

std::optional<IcebergTablePointer> KeeperIcebergRESTCatalogStore::getTable(const IcebergNamespaceName & ns, const String & table) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::getTable");
    auto zookeeper = getZooKeeper();
    IcebergTablePointer pointer;
    if (!zookeeper->tryGet(tablePath(ns, table), pointer.uuid))
        return std::nullopt;
    /// Missing here means the table was dropped between the two reads.
    Coordination::Stat stat;
    if (!zookeeper->tryGet(tableUuidPath(ns, table, pointer.uuid), pointer.metadata_location, &stat))
        return std::nullopt;
    pointer.version = stat.version;
    return pointer;
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
    const auto path = tablePath(ns, table);
    String uuid;
    if (!zookeeper->tryGet(path, uuid))
        return false;

    /// Removing the uuid node first pins the table identity. A table re-created under the same name has another uuid.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(tableUuidPath(ns, table, uuid), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(path, -1));
    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZNONODE)
        return false;
    zkutil::KeeperMultiException::check(code, ops, responses);
    return true;
}

KeeperIcebergRESTCatalogStore::UpdateTableResult KeeperIcebergRESTCatalogStore::updateTable(
    const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & expected, const IcebergTablePointer & new_pointer)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::updateTable");
    /// The uuid node pins the table identity. A table re-created under the same name has another uuid, so the set misses it.
    auto zookeeper = getZooKeeper();
    const auto path = tableUuidPath(ns, table, expected.uuid);
    const auto code = zookeeper->trySet(path, new_pointer.metadata_location, expected.version);
    if (code == Coordination::Error::ZBADVERSION)
        return UpdateTableResult::VersionMismatch;
    if (code == Coordination::Error::ZNONODE)
        return zookeeper->exists(tablePath(ns, table)) ? UpdateTableResult::UuidMismatch : UpdateTableResult::TableMissing;
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, path);
    return UpdateTableResult::Updated;
}

}
