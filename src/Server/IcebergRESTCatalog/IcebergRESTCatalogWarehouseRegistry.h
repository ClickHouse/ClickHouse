#pragma once

#include <base/types.h>
#include <Common/CacheBase.h>
#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogKeeperConfig.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>

#include <memory>

namespace DB
{

/// The warehouses stored in Keeper. Every lookup reads the warehouse root node, so a warehouse
/// created, changed or dropped on any server is visible on the next request.
class IcebergRESTCatalogWarehouseRegistry
{
public:
    explicit IcebergRESTCatalogWarehouseRegistry(ContextPtr context_);

    /// Returns nullptr if the warehouse does not exist.
    IcebergRESTCatalogWarehousePtr tryGet(const String & name) const;

private:
    struct Entry
    {
        IcebergRESTCatalogWarehousePtr warehouse;
        /// Last modification of the root node the object was built from.
        /// Not `version`, because `version` restarts when the warehouse is dropped and created again.
        Int64 mzxid = 0;
    };

    const ContextPtr context;
    const IcebergRESTCatalogKeeperConfig keeper_config;
    const LoggerPtr log;

    /// Bounded, so dropped or idle warehouses do not keep their S3 clients forever.
    static constexpr size_t MAX_CACHED_WAREHOUSES = 50;
    mutable CacheBase<String, Entry> warehouses;
};

using IcebergRESTCatalogWarehouseRegistryPtr = std::shared_ptr<const IcebergRESTCatalogWarehouseRegistry>;

}
