#pragma once

#include <base/types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogCredentialVendor.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogKeeperConfig.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseDefinition.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#include <memory>

namespace DB
{

/// One warehouse of the Iceberg REST catalog (RFC: issue #114697).
class IcebergRESTCatalogWarehouse
{
public:
    using S3Location = IcebergRESTCatalogS3Location;

    IcebergRESTCatalogWarehouse(
        String name_,
        String base_location_,
        KeeperIcebergRESTCatalogStorePtr store_,
        ObjectStoragePtr object_storage_,
        IcebergRESTCatalog::CredentialVendorPtr credential_vendor_);

    /// Also the REST `prefix`.
    const String name;
    /// Default storage prefix for tables.
    const String base_location;
    const ObjectStoragePtr object_storage;

    void validateLocation(const String & location) const;
    [[nodiscard]] String objectKey(const String & location) const;

    /// Catalog state. See `KeeperIcebergRESTCatalogStore` for the contracts.
    using CreateNamespaceResult = KeeperIcebergRESTCatalogStore::CreateNamespaceResult;
    [[nodiscard]] CreateNamespaceResult createNamespace(const IcebergNamespaceName & ns, const std::map<String, String> & properties) const;
    [[nodiscard]] bool namespaceExists(const IcebergNamespaceName & ns) const;
    [[nodiscard]] std::optional<std::map<String, String>> getNamespaceProperties(const IcebergNamespaceName & ns) const;
    [[nodiscard]] std::optional<UpdateNamespacePropertiesResult> updateNamespaceProperties(
        const IcebergNamespaceName & ns, const std::map<String, String> & updates, const std::set<String> & removals) const;
    [[nodiscard]] std::vector<IcebergNamespaceName> listNamespaces(const IcebergNamespaceName & parent) const;

    using CreateTableResult = KeeperIcebergRESTCatalogStore::CreateTableResult;
    [[nodiscard]] CreateTableResult createTable(const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & pointer) const;
    [[nodiscard]] std::optional<IcebergTablePointer> getTable(const IcebergNamespaceName & ns, const String & table) const;
    [[nodiscard]] bool tableExists(const IcebergNamespaceName & ns, const String & table) const;
    [[nodiscard]] std::optional<Strings> listTables(const IcebergNamespaceName & ns) const;
    [[nodiscard]] bool dropTable(const IcebergNamespaceName & ns, const String & table) const;
    using RenameTableResult = KeeperIcebergRESTCatalogStore::RenameTableResult;
    [[nodiscard]] RenameTableResult renameTable(
        const IcebergNamespaceName & source_ns, const String & source, const IcebergNamespaceName & dest_ns, const String & dest) const;
    using UpdateTableResult = KeeperIcebergRESTCatalogStore::UpdateTableResult;
    using TableUpdate = KeeperIcebergRESTCatalogStore::TableUpdate;
    using UpdateTablesResult = KeeperIcebergRESTCatalogStore::UpdateTablesResult;
    [[nodiscard]] UpdateTablesResult updateTables(const std::vector<TableUpdate> & updates) const;

    /// Temporary S3 credentials for the table at `location`.
    [[nodiscard]] IcebergRESTCatalog::VendedCredentials vendCredentials(const String & location, IcebergRESTCatalog::VendedAccessLevel access) const;

    /// Checks that the role was assumed, then lists one key under `base_location`. This proves that the role can read the location.
    void checkStorageAccess() const;

private:
    void validateLocation(const S3Location & location) const;

    const KeeperIcebergRESTCatalogStorePtr store;
    const IcebergRESTCatalog::CredentialVendorPtr credential_vendor;
};

using IcebergRESTCatalogWarehousePtr = std::shared_ptr<const IcebergRESTCatalogWarehouse>;

String stripTrailingSlashes(String location);

/// Builds the Keeper store and the S3 client of the warehouse.
IcebergRESTCatalogWarehousePtr createIcebergRESTCatalogWarehouse(
    ContextPtr context, const String & name, const IcebergRESTCatalogWarehouseDefinition & definition, const IcebergRESTCatalogKeeperConfig & keeper_config);

}
