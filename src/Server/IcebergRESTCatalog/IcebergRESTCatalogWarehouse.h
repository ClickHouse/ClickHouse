#pragma once

#include <base/types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>

#include <map>
#include <memory>

namespace DB
{

class KeeperIcebergRESTCatalogStore;
using KeeperIcebergRESTCatalogStorePtr = std::shared_ptr<KeeperIcebergRESTCatalogStore>;

/// One warehouse of the Iceberg REST catalog (RFC: issue #114697).
struct IcebergRESTCatalogWarehouse
{
    IcebergRESTCatalogWarehouse(
        String name_, String base_location_, KeeperIcebergRESTCatalogStorePtr store_, ObjectStoragePtr object_storage_);

    /// Also the REST `prefix`.
    const String name;
    /// Default storage prefix for tables.
    const String base_location;
    const KeeperIcebergRESTCatalogStorePtr store;
    const ObjectStoragePtr object_storage;

    bool ownsLocation(const String & location) const;
    /// Throws `INCORRECT_DATA`. Callers validate client input with `ownsLocation` first.
    String objectKey(const String & location) const;
};

using IcebergRESTCatalogWarehousePtr = std::shared_ptr<const IcebergRESTCatalogWarehouse>;

String stripTrailingSlashes(String location);

/// Experimental scaffolding. Currently, the server config holds the warehouse definition.
/// It is only read on startup.
///
/// TODO: Remove warehouses from config, store warehouses in Keeper, and manage them with SQL.
/// Have `tryGet` search in Keeper and remove the const map of warehouses.
class IcebergRESTCatalogWarehouses
{
public:
    using Map = std::map<String, IcebergRESTCatalogWarehousePtr>;

    explicit IcebergRESTCatalogWarehouses(Map warehouses_) : warehouses(std::move(warehouses_)) {}

    IcebergRESTCatalogWarehousePtr tryGet(const String & name) const
    {
        auto it = warehouses.find(name);
        if (it == warehouses.end())
            return nullptr;
        return it->second;
    }

private:
    const Map warehouses;
};

using IcebergRESTCatalogWarehousesPtr = std::shared_ptr<const IcebergRESTCatalogWarehouses>;

}
