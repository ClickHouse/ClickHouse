#pragma once

#include <base/types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogKeeperConfig.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseDefinition.h>

#include <memory>

namespace DB
{

class KeeperIcebergRESTCatalogStore;
using KeeperIcebergRESTCatalogStorePtr = std::shared_ptr<KeeperIcebergRESTCatalogStore>;

/// One warehouse of the Iceberg REST catalog (RFC: issue #114697).
struct IcebergRESTCatalogWarehouse
{
    struct S3Location
    {
        String bucket;
        String key;
    };

    IcebergRESTCatalogWarehouse(
        String name_, String base_location_, KeeperIcebergRESTCatalogStorePtr store_, ObjectStoragePtr object_storage_);

    /// Also the REST `prefix`.
    const String name;
    /// Default storage prefix for tables.
    const String base_location;
    const KeeperIcebergRESTCatalogStorePtr store;
    const ObjectStoragePtr object_storage;

    void validateLocation(const String & location) const;
    String objectKey(const String & location) const;

private:
    void validateLocation(const S3Location & location) const;
};

using IcebergRESTCatalogWarehousePtr = std::shared_ptr<const IcebergRESTCatalogWarehouse>;

String stripTrailingSlashes(String location);

/// Builds the Keeper store and the S3 client of the warehouse.
IcebergRESTCatalogWarehousePtr createIcebergRESTCatalogWarehouse(
    ContextPtr context, const String & name, const IcebergRESTCatalogWarehouseDefinition & definition, const IcebergRESTCatalogKeeperConfig & keeper_config);

}
