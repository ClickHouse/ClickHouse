#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/escapeForFileName.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <algorithm>
#include <sstream>

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

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
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

bool KeeperIcebergRESTCatalogStore::createNamespace(const IcebergNamespaceName & name, const std::map<String, String> & properties)
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::createNamespace");
    auto zookeeper = getZooKeeper();

    /// Keep the tree walkable from the root.
    for (size_t level = 1; level < name.size(); ++level)
    {
        const auto parent_path = namespacePath(IcebergNamespaceName(name.begin(), name.begin() + level));
        zookeeper->createIfNotExists(parent_path, propertiesToJSON({}));
        zookeeper->createIfNotExists(parent_path + "/namespaces", "");
        zookeeper->createIfNotExists(parent_path + "/tables", "");
    }

    const auto path = namespacePath(name);
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(path, propertiesToJSON(properties), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(path + "/namespaces", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(path + "/tables", "", zkutil::CreateMode::Persistent));
    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZNODEEXISTS)
        return false;

    zkutil::KeeperMultiException::check(code, ops, responses);
    return true;
}

bool KeeperIcebergRESTCatalogStore::namespaceExists(const IcebergNamespaceName & name) const
{
    auto component_guard = Coordination::setCurrentComponent("KeeperIcebergRESTCatalogStore::namespaceExists");
    return getZooKeeper()->exists(namespacePath(name));
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

}
