#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>

#include <Common/Exception.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <fmt/format.h>

#include <optional>

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

/// Splits `s3://<bucket>/<key>` into the bucket and the key. The key may be empty.
struct S3Location
{
    String bucket;
    String key;
};

std::optional<S3Location> parseS3Location(const String & location)
{
    /// TODO: support s3a:// and s3n://
    if (!location.starts_with(S3_SCHEME))
        return std::nullopt;
    const auto slash = location.find('/', S3_SCHEME.size());
    if (slash == String::npos)
        return S3Location{.bucket = location.substr(S3_SCHEME.size()), .key = ""};
    return S3Location{.bucket = location.substr(S3_SCHEME.size(), slash - S3_SCHEME.size()), .key = location.substr(slash + 1)};
}

}

IcebergRESTCatalogWarehouse::IcebergRESTCatalogWarehouse(
    String name_, String base_location_, KeeperIcebergRESTCatalogStorePtr store_, ObjectStoragePtr object_storage_)
    : name(std::move(name_)), base_location(std::move(base_location_)), store(std::move(store_)), object_storage(std::move(object_storage_))
{
    /// The server has credentials for one bucket only, so the default table location must be in it.
    if (!isInBucket(base_location))
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "base_location {} of warehouse {} is not inside bucket '{}' of its object storage",
            base_location,
            name,
            object_storage->getObjectsNamespace());
}

bool IcebergRESTCatalogWarehouse::isInBucket(const String & location) const
{
    const auto parsed = parseS3Location(location);
    return parsed && parsed->bucket == object_storage->getObjectsNamespace();
}

String IcebergRESTCatalogWarehouse::objectKey(const String & location) const
{
    const auto parsed = parseS3Location(location);
    if (!parsed || parsed->bucket != object_storage->getObjectsNamespace())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Location {} is outside the bucket of warehouse {}", location, name);
    return parsed->key;
}

String stripTrailingSlashes(String location)
{
    while (location.ends_with('/'))
        location.pop_back();
    return location;
}

bool hasS3Scheme(const String & location)
{
    return location.starts_with(S3_SCHEME);
}

}
