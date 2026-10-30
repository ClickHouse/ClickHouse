#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseRegistry.h>

#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

namespace DB
{

IcebergRESTCatalogWarehouseRegistry::IcebergRESTCatalogWarehouseRegistry(ContextPtr context_)
    : context(std::move(context_)), keeper_config(getIcebergRESTCatalogKeeperConfig(*context)), log(getLogger("IcebergRESTCatalogWarehouseRegistry"))
{
}

IcebergRESTCatalogWarehousePtr IcebergRESTCatalogWarehouseRegistry::tryGet(const String & name) const
{
    auto component_guard = Coordination::setCurrentComponent("IcebergRESTCatalogWarehouseRegistry::tryGet");
    const auto path = keeper_config.warehousePath(name);
    String data;
    Coordination::Stat stat;
    const bool exists = keeper_config.getZooKeeper(*context)->tryGet(path, data, &stat);

    std::lock_guard lock(mutex);
    auto it = warehouses.find(name);
    if (it != warehouses.end())
    {
        if (exists && it->second.mzxid == stat.mzxid)
            return it->second.warehouse;

        warehouses.erase(it);
        LOG_INFO(log, "Removed warehouse {}", name);
    }
    if (!exists)
        return nullptr;

    const auto definition = IcebergRESTCatalogWarehouseDefinition::fromKeeperData(data, path);
    auto warehouse = createIcebergRESTCatalogWarehouse(context, name, definition, keeper_config);
    warehouses.emplace(name, Entry{warehouse, stat.mzxid});
    LOG_INFO(log, "Added warehouse {}", name);
    return warehouse;
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
        const auto path = keeper_config.warehousePath(name);
        String data;
        /// Dropped between the two reads.
        if (!zookeeper->tryGet(path, data))
            continue;
        result.emplace(name, IcebergRESTCatalogWarehouseDefinition::fromKeeperData(data, path));
    }
    return result;
}

void IcebergRESTCatalogWarehouseRegistry::checkStorageAccess(const String & name, const IcebergRESTCatalogWarehouseDefinition & definition) const
{
    createIcebergRESTCatalogWarehouse(context, name, definition, keeper_config)->checkStorageAccess();
}

bool IcebergRESTCatalogWarehouseRegistry::create(const String & name, const IcebergRESTCatalogWarehouseDefinition & definition)
{
    auto component_guard = Coordination::setCurrentComponent("IcebergRESTCatalogWarehouseRegistry::create");
    auto zookeeper = keeper_config.getZooKeeper(*context);
    zookeeper->createAncestors(keeper_config.zookeeper_path);

    /// The root node and its `namespaces` child are created atomically, so a crash cannot leave a half-made warehouse.
    const auto path = keeper_config.warehousePath(name);
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeCreateRequest(path, definition.toKeeperData(), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(path + "/namespaces", "", zkutil::CreateMode::Persistent));
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
    const auto code = zookeeper->trySet(path, definition.toKeeperData(), stat.version);
    if (code == Coordination::Error::ZNONODE)
        return ReplaceCredentialResult::NotFound;
    if (code == Coordination::Error::ZBADVERSION)
        return ReplaceCredentialResult::Conflict;
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, path);

    LOG_INFO(log, "Replaced the credential of warehouse {}", name);
    return ReplaceCredentialResult::Replaced;
}

IcebergRESTCatalogWarehouseRegistry::DropResult IcebergRESTCatalogWarehouseRegistry::drop(const String & name)
{
    auto component_guard = Coordination::setCurrentComponent("IcebergRESTCatalogWarehouseRegistry::drop");
    const auto path = keeper_config.warehousePath(name);

    /// Keeper refuses to remove a node with children, so a warehouse with namespaces fails on the first request and nothing is removed.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(path + "/namespaces", -1));
    ops.emplace_back(zkutil::makeRemoveRequest(path, -1));
    Coordination::Responses responses;
    const auto code = keeper_config.getZooKeeper(*context)->tryMulti(ops, responses);
    if (code == Coordination::Error::ZNONODE)
        return DropResult::NotFound;
    if (code == Coordination::Error::ZNOTEMPTY)
        return DropResult::NotEmpty;
    zkutil::KeeperMultiException::check(code, ops, responses);

    {
        std::lock_guard lock(mutex);
        warehouses.erase(name);
    }
    LOG_INFO(log, "Dropped warehouse {}", name);
    return DropResult::Dropped;
}

}
