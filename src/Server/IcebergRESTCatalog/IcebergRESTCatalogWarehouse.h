#pragma once

#include <base/types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#include <map>
#include <memory>

namespace DB
{

/// One warehouse of the Iceberg REST catalog (RFC: issue #114697).
/// Everything that belongs to a warehouse rather than to the server goes here.
struct IcebergRESTCatalogWarehouse
{
    IcebergRESTCatalogWarehouse(
        String name_, String base_location_, KeeperIcebergRESTCatalogStorePtr store_, ObjectStoragePtr object_storage_);

    /// Also the REST `prefix`.
    const String name;
    /// Storage prefix for tables created without an explicit location.
    const String base_location;
    const KeeperIcebergRESTCatalogStorePtr store;
    const ObjectStoragePtr object_storage;

    bool ownsLocation(const String & location) const;
    String objectKey(const String & location) const;
};

using IcebergRESTCatalogWarehousePtr = std::shared_ptr<const IcebergRESTCatalogWarehouse>;

String stripTrailingSlashes(String location);

/// Resolves a warehouse name to a warehouse. Shared by all request handlers.
///
/// Experimental scaffolding. The server config is the source of truth: the set of warehouses
/// is read once at startup and never changes while the server runs.
/// The plan is to store warehouses in Keeper and manage them with SQL. Then `find` will read
/// Keeper on each call, and this class will no longer hold a map.
class IcebergRESTCatalogWarehouses
{
public:
    using Map = std::map<String, IcebergRESTCatalogWarehousePtr>;

    explicit IcebergRESTCatalogWarehouses(Map warehouses_) : warehouses(std::move(warehouses_)) {}

    /// Returns nullptr for an unknown warehouse name.
    IcebergRESTCatalogWarehousePtr find(const String & name) const
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
