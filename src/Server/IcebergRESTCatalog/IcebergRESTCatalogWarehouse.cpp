#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>

#include <Common/Exception.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr std::string_view S3_SCHEME = "s3://";

using S3Location = IcebergRESTCatalogWarehouse::S3Location;

/// Splits `s3://<bucket>/<key>` into the bucket and the key. The key may be empty.
S3Location parseS3Location(const String & location)
{
    /// TODO: support s3a:// and s3n://
    if (!location.starts_with(S3_SCHEME))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Location {} is not an s3:// URI", location);
    const auto slash = location.find('/', S3_SCHEME.size());
    if (slash == String::npos)
        return S3Location{.bucket = location.substr(S3_SCHEME.size()), .key = ""};
    return S3Location{.bucket = location.substr(S3_SCHEME.size(), slash - S3_SCHEME.size()), .key = location.substr(slash + 1)};
}

}

IcebergRESTCatalogWarehouse::IcebergRESTCatalogWarehouse(
    String name_, String base_location_, KeeperIcebergRESTCatalogStorePtr store_, ObjectStoragePtr object_storage_)
    : name(std::move(name_)), base_location(std::move(base_location_)), object_storage(std::move(object_storage_)), store(std::move(store_))
{
    /// The server has credentials for one bucket only, so the default table location must be in it.
    validateLocation(base_location);
}

void IcebergRESTCatalogWarehouse::validateLocation(const String & location) const
{
    validateLocation(parseS3Location(location));
}

void IcebergRESTCatalogWarehouse::validateLocation(const S3Location & location) const
{
    if (location.bucket != object_storage->getObjectsNamespace())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Location s3://{}/{} is outside bucket '{}' of warehouse {}",
            location.bucket,
            location.key,
            object_storage->getObjectsNamespace(),
            name);
    /// `S3ObjectStorage` re-parses a key of the form `<scheme>://<bucket>/...` and writes to the embedded bucket.
    if (location.key.contains("://"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Location s3://{}/{} contains an embedded URI", location.bucket, location.key);
}

String IcebergRESTCatalogWarehouse::objectKey(const String & location) const
{
    auto parsed = parseS3Location(location);
    validateLocation(parsed);
    return std::move(parsed.key);
}

bool IcebergRESTCatalogWarehouse::createNamespace(const IcebergNamespaceName & ns, const std::map<String, String> & properties) const
{
    return store->createNamespace(ns, properties);
}

bool IcebergRESTCatalogWarehouse::namespaceExists(const IcebergNamespaceName & ns) const
{
    return store->namespaceExists(ns);
}

std::optional<std::map<String, String>> IcebergRESTCatalogWarehouse::getNamespaceProperties(const IcebergNamespaceName & ns) const
{
    return store->getNamespaceProperties(ns);
}

std::optional<UpdateNamespacePropertiesResult> IcebergRESTCatalogWarehouse::updateNamespaceProperties(
    const IcebergNamespaceName & ns, const std::map<String, String> & updates, const std::set<String> & removals) const
{
    return store->updateNamespaceProperties(ns, updates, removals);
}

std::vector<IcebergNamespaceName> IcebergRESTCatalogWarehouse::listNamespaces(const IcebergNamespaceName & parent) const
{
    return store->listNamespaces(parent);
}

IcebergRESTCatalogWarehouse::CreateTableResult
IcebergRESTCatalogWarehouse::createTable(const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & pointer) const
{
    return store->createTable(ns, table, pointer);
}

std::optional<IcebergTablePointer> IcebergRESTCatalogWarehouse::getTable(const IcebergNamespaceName & ns, const String & table) const
{
    return store->getTable(ns, table);
}

bool IcebergRESTCatalogWarehouse::tableExists(const IcebergNamespaceName & ns, const String & table) const
{
    return store->tableExists(ns, table);
}

std::optional<Strings> IcebergRESTCatalogWarehouse::listTables(const IcebergNamespaceName & ns) const
{
    return store->listTables(ns);
}

bool IcebergRESTCatalogWarehouse::dropTable(const IcebergNamespaceName & ns, const String & table) const
{
    return store->dropTable(ns, table);
}

String stripTrailingSlashes(String location)
{
    while (location.ends_with('/'))
        location.pop_back();
    return location;
}

}
