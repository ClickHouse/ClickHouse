#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseRegistry.h>

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
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

}
