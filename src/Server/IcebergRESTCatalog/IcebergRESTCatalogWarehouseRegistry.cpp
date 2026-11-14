#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseRegistry.h>

#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

namespace CurrentMetrics
{
    extern const Metric IcebergRESTCatalogWarehouseCacheSize;
    extern const Metric IcebergRESTCatalogWarehouseCacheEntries;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int S3_ERROR;
}

IcebergRESTCatalogWarehouseRegistry::IcebergRESTCatalogWarehouseRegistry(ContextPtr context_)
    : context(std::move(context_))
    , keeper_config(getIcebergRESTCatalogKeeperConfig(*context))
    , log(getLogger("IcebergRESTCatalogWarehouseRegistry"))
    , warehouses(
          "LRU",
          CurrentMetrics::IcebergRESTCatalogWarehouseCacheSize,
          CurrentMetrics::IcebergRESTCatalogWarehouseCacheEntries,
          MAX_CACHED_WAREHOUSES,
          MAX_CACHED_WAREHOUSES,
          CacheBase<String, Entry>::DEFAULT_SIZE_RATIO)
{
}

IcebergRESTCatalogWarehousePtr IcebergRESTCatalogWarehouseRegistry::tryGet(const String & name) const
{
    /// Guard against requests like `GET /v1/config?warehouse=`.
    if (name.empty())
        return nullptr;

    auto component_guard = Coordination::setCurrentComponent("IcebergRESTCatalogWarehouseRegistry::tryGet");
    const auto path = keeper_config.warehousePath(name);
    String data;
    Coordination::Stat stat;
    /// A plain read may lag behind a write from another catalog server.
    /// TODO: Set a watch on the node, so a cached warehouse needs no Keeper request.
    auto zookeeper = keeper_config.getZooKeeper(*context);
    zookeeper->sync(path);
    const bool exists = zookeeper->tryGet(path, data, &stat);

    if (!exists)
    {
        warehouses.remove(name);
        return nullptr;
    }

    if (const auto cached = warehouses.get(name); cached && cached->mzxid == stat.mzxid)
        return cached->warehouse;

    const auto definition = IcebergRESTCatalogWarehouseDefinition::fromKeeperData(data, path);
    auto entry = std::make_shared<Entry>(Entry{createIcebergRESTCatalogWarehouse(context, name, definition, keeper_config), stat.mzxid});
    warehouses.set(name, entry);
    LOG_INFO(log, "Added warehouse {}", name);
    return entry->warehouse;
}

std::optional<IcebergRESTCatalogWarehouseDefinition> IcebergRESTCatalogWarehouseRegistry::tryGetDefinition(const String & name) const
{
    auto component_guard = Coordination::setCurrentComponent("IcebergRESTCatalogWarehouseRegistry::tryGetDefinition");
    const auto path = keeper_config.warehousePath(name);
    String data;
    if (!keeper_config.getZooKeeper(*context)->tryGet(path, data))
        return std::nullopt;
    return IcebergRESTCatalogWarehouseDefinition::fromKeeperData(data, path);
}

std::map<String, IcebergRESTCatalogWarehouseDefinition> IcebergRESTCatalogWarehouseRegistry::list() const
{
    auto component_guard = Coordination::setCurrentComponent("IcebergRESTCatalogWarehouseRegistry::list");
    auto zookeeper = keeper_config.getZooKeeper(*context);

    /// The parent node is created with the first warehouse.
    Strings children;
    const auto code = zookeeper->tryGetChildren(keeper_config.zookeeper_path, children);
    if (code == Coordination::Error::ZNONODE)
        return {};
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, keeper_config.zookeeper_path);

    std::map<String, IcebergRESTCatalogWarehouseDefinition> result;
    for (const auto & child : children)
    {
        const auto name = unescapeForFileName(child);
        /// A warehouse dropped after the listing is skipped.
        if (auto definition = tryGetDefinition(name))
            result.emplace(name, std::move(*definition));
    }
    return result;
}

void IcebergRESTCatalogWarehouseRegistry::checkStorageAccess(const String & name, const IcebergRESTCatalogWarehouseDefinition & definition) const
{
    try
    {
        createIcebergRESTCatalogWarehouse(context, name, definition, keeper_config)->checkStorageAccess();
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::S3_ERROR)
            throw;
        /// A wrong role or location is a client mistake. `BAD_ARGUMENTS` becomes a 400 response with the S3 message.
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The role cannot access the location of warehouse {}: {}", name, e.message());
    }
}

bool IcebergRESTCatalogWarehouseRegistry::create(const String & name, IcebergRESTCatalogWarehouseDefinition definition)
{
    /// Nothing is stored until the role has been assumed and the location has been read.
    checkStorageAccess(name, definition);

    auto component_guard = Coordination::setCurrentComponent("IcebergRESTCatalogWarehouseRegistry::create");
    auto zookeeper = keeper_config.getZooKeeper(*context);
    const auto path = keeper_config.warehousePath(name);
    zookeeper->createAncestors(path);

    definition.uuid = UUIDHelpers::generateV4();

    /// The warehouse node and its uuid subtree are created atomically, so a crash cannot leave a half-made warehouse.
    const auto root = path + "/" + toString(definition.uuid);
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(path, definition.toKeeperData(), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(root, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(root + "/namespaces", "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(root + "/tables_by_uuid", "", zkutil::CreateMode::Persistent));
    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZNODEEXISTS)
        return false;
    zkutil::KeeperMultiException::check(code, ops, responses);

    LOG_INFO(log, "Created warehouse {} at {}", name, definition.base_location);
    return true;
}

IcebergRESTCatalogWarehouseRegistry::ReplaceCredentialResult
IcebergRESTCatalogWarehouseRegistry::replaceCredential(const String & name, const IcebergRESTCatalogWarehouseCredential & credential)
{
    auto component_guard = Coordination::setCurrentComponent("IcebergRESTCatalogWarehouseRegistry::replaceCredential");
    auto zookeeper = keeper_config.getZooKeeper(*context);
    const auto path = keeper_config.warehousePath(name);

    String data;
    Coordination::Stat stat;
    if (!zookeeper->tryGet(path, data, &stat))
        return ReplaceCredentialResult::NotFound;

    auto definition = IcebergRESTCatalogWarehouseDefinition::fromKeeperData(data, path);
    definition.credential = credential;
    checkStorageAccess(name, definition);

    /// The version check keeps a concurrent change from being overwritten.
    /// The version restarts when the warehouse is dropped and created again. The uuid node check covers that case.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCheckRequest(path + "/" + toString(definition.uuid), -1));
    ops.emplace_back(zkutil::makeSetRequest(path, definition.toKeeperData(), stat.version));
    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZNONODE)
        return ReplaceCredentialResult::NotFound;
    if (code == Coordination::Error::ZBADVERSION)
        return ReplaceCredentialResult::Conflict;
    zkutil::KeeperMultiException::check(code, ops, responses);

    LOG_INFO(log, "Replaced the credential of warehouse {}", name);
    return ReplaceCredentialResult::Replaced;
}

IcebergRESTCatalogWarehouseRegistry::DropResult IcebergRESTCatalogWarehouseRegistry::drop(const String & name)
{
    auto component_guard = Coordination::setCurrentComponent("IcebergRESTCatalogWarehouseRegistry::drop");
    auto zookeeper = keeper_config.getZooKeeper(*context);
    const auto path = keeper_config.warehousePath(name);

    const auto definition = tryGetDefinition(name);
    if (!definition)
        return DropResult::NotFound;
    const auto root = path + "/" + toString(definition->uuid);

    /// Keeper refuses to remove a node with children, so a warehouse with namespaces fails on the first request and nothing is removed.
    /// A warehouse dropped and created again in between has another uuid, so the first request finds no node.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(root + "/namespaces", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(root + "/tables_by_uuid", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(root, -1));
    ops.emplace_back(zkutil::makeRemoveRequest(path, -1));
    Coordination::Responses responses;
    const auto code = zookeeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZNONODE)
        return DropResult::NotFound;
    if (code == Coordination::Error::ZNOTEMPTY)
        return DropResult::NotEmpty;
    zkutil::KeeperMultiException::check(code, ops, responses);

    warehouses.remove(name);
    LOG_INFO(log, "Dropped warehouse {}", name);
    return DropResult::Dropped;
}

}
