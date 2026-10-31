#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseRegistry.h>

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

namespace CurrentMetrics
{
    extern const Metric IcebergRESTCatalogWarehouseCacheSize;
    extern const Metric IcebergRESTCatalogWarehouseCacheEntries;
}

namespace DB
{

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
    auto component_guard = Coordination::setCurrentComponent("IcebergRESTCatalogWarehouseRegistry::tryGet");
    const auto path = keeper_config.warehousePath(name);
    String data;
    Coordination::Stat stat;
    const bool exists = keeper_config.getZooKeeper(*context)->tryGet(path, data, &stat);

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

}
