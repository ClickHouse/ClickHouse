#pragma once

#include <base/types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#include <map>
#include <memory>

namespace DB
{

/// One warehouse of the Iceberg REST catalog (RFC: issue #114697).
class IcebergRESTCatalogWarehouse
{
public:
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
    const ObjectStoragePtr object_storage;

    void validateLocation(const String & location) const;
    String objectKey(const String & location) const;

    /// Catalog state. See `KeeperIcebergRESTCatalogStore` for the contracts.
    bool createNamespace(const IcebergNamespaceName & ns, const std::map<String, String> & properties) const;
    bool namespaceExists(const IcebergNamespaceName & ns) const;
    std::optional<std::map<String, String>> getNamespaceProperties(const IcebergNamespaceName & ns) const;
    std::optional<UpdateNamespacePropertiesResult> updateNamespaceProperties(
        const IcebergNamespaceName & ns, const std::map<String, String> & updates, const std::set<String> & removals) const;
    std::vector<IcebergNamespaceName> listNamespaces(const IcebergNamespaceName & parent) const;

    using CreateTableResult = KeeperIcebergRESTCatalogStore::CreateTableResult;
    CreateTableResult createTable(const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & pointer) const;
    std::optional<IcebergTablePointer> getTable(const IcebergNamespaceName & ns, const String & table) const;
    bool tableExists(const IcebergNamespaceName & ns, const String & table) const;
    std::optional<Strings> listTables(const IcebergNamespaceName & ns) const;
    bool dropTable(const IcebergNamespaceName & ns, const String & table) const;
    using UpdateTableResult = KeeperIcebergRESTCatalogStore::UpdateTableResult;
    UpdateTableResult updateTable(
        const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & expected, const IcebergTablePointer & new_pointer) const;

private:
    void validateLocation(const S3Location & location) const;

    const KeeperIcebergRESTCatalogStorePtr store;
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
