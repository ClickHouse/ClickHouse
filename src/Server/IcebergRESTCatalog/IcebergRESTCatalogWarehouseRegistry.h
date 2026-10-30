#pragma once

#include <base/types.h>
#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogKeeperConfig.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>

namespace DB
{

/// The warehouses stored in Keeper. Every lookup reads the warehouse root node, so a warehouse
/// created, changed or dropped on any server is visible on the next request.
/// The write operations serve the management API (issue #127488).
class IcebergRESTCatalogWarehouseRegistry
{
public:
    explicit IcebergRESTCatalogWarehouseRegistry(ContextPtr context_);

    /// Returns nullptr if the warehouse does not exist.
    IcebergRESTCatalogWarehousePtr tryGet(const String & name) const;

    std::optional<IcebergRESTCatalogWarehouseDefinition> tryGetDefinition(const String & name) const;
    std::map<String, IcebergRESTCatalogWarehouseDefinition> list() const;

    /// Builds the S3 client of the definition and reads the base location with it.
    void checkStorageAccess(const String & name, const IcebergRESTCatalogWarehouseDefinition & definition) const;

    /// Returns false if the warehouse already exists.
    bool create(const String & name, const IcebergRESTCatalogWarehouseDefinition & definition);

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
    /// Removes the root node. Nothing on S3 is touched.
    DropResult drop(const String & name);

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

    mutable std::mutex mutex;
    mutable std::map<String, Entry> warehouses TSA_GUARDED_BY(mutex);
};

using IcebergRESTCatalogWarehouseRegistryPtr = std::shared_ptr<IcebergRESTCatalogWarehouseRegistry>;

}
