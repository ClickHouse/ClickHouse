#pragma once

#include <base/types.h>
#include <Common/CacheBase.h>
#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogKeeperConfig.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>

#include <map>
#include <memory>
#include <optional>

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

    std::optional<IcebergRESTCatalogWarehouseDefinition> tryGetDefinition(const String & name) const;
    std::map<String, IcebergRESTCatalogWarehouseDefinition> list() const;

    /// Assigns a new uuid to the warehouse. Returns false if the warehouse already exists.
    bool create(const String & name, IcebergRESTCatalogWarehouseDefinition definition);

    enum class ReplaceCredentialResult
    {
        Replaced,
        NotFound,
        /// The definition changed between the read and the write.
        Conflict,
    };
    /// Checks storage access with the new credential before storing it.
    ReplaceCredentialResult replaceCredential(const String & name, const IcebergRESTCatalogWarehouseCredential & credential);

    enum class DropResult
    {
        Dropped,
        NotFound,
        NotEmpty,
    };
    /// Removes the warehouse node and its uuid subtree. Nothing on S3 is touched.
    DropResult drop(const String & name);

private:
    struct Entry
    {
        IcebergRESTCatalogWarehousePtr warehouse;
        /// Last modification of the root node the object was built from.
        /// Not `version`, because `version` restarts when the warehouse is dropped and created again.
        Int64 mzxid = 0;
    };

    /// Builds the S3 client of the definition and reads the base location with it. An S3 error is reported as `BAD_ARGUMENTS`.
    void checkStorageAccess(const String & name, const IcebergRESTCatalogWarehouseDefinition & definition) const;

    const ContextPtr context;
    const IcebergRESTCatalogKeeperConfig keeper_config;
    const LoggerPtr log;

    /// Bounded, so dropped or idle warehouses do not keep their S3 clients forever.
    static constexpr size_t MAX_CACHED_WAREHOUSES = 50;
    mutable CacheBase<String, Entry> warehouses;
};

using IcebergRESTCatalogWarehouseRegistryPtr = std::shared_ptr<IcebergRESTCatalogWarehouseRegistry>;

}
