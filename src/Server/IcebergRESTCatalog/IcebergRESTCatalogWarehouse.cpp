#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>

#include <Common/Exception.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{

constexpr std::string_view S3_SCHEME = "s3://";

/// `s3://<bucket>/` of the object storage.
String bucketPrefix(const IObjectStorage & object_storage)
{
    return fmt::format("{}{}/", S3_SCHEME, object_storage.getObjectsNamespace());
}

}

IcebergRESTCatalogWarehouse::IcebergRESTCatalogWarehouse(
    String name_, String base_location_, KeeperIcebergRESTCatalogStorePtr store_, ObjectStoragePtr object_storage_)
    : name(std::move(name_)), base_location(std::move(base_location_)), store(std::move(store_)), object_storage(std::move(object_storage_))
{
    /// The server has credentials for one bucket only, so the default table location must be in it.
    if (!ownsLocation(base_location))
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "base_location {} of warehouse {} is not inside bucket '{}' of its object storage",
            base_location,
            name,
            object_storage->getObjectsNamespace());
}

/// TODO: support s3a:// and s3n://
bool IcebergRESTCatalogWarehouse::ownsLocation(const String & location) const
{
    const auto prefix = bucketPrefix(*object_storage);
    return location.starts_with(prefix) && location.size() > prefix.size();
}

String IcebergRESTCatalogWarehouse::objectKey(const String & location) const
{
    if (!ownsLocation(location))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Location {} is outside the bucket of warehouse {}", location, name);
    return location.substr(bucketPrefix(*object_storage).size());
}

String stripTrailingSlashes(String location)
{
    while (location.ends_with('/'))
        location.pop_back();
    return location;
}

}
