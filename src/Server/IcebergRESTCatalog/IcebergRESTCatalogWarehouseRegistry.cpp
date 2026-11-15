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

}
