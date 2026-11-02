#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>

#include "config.h"

#include <Common/Exception.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#if USE_AWS_S3
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/diskSettings.h>
#include <IO/S3/URI.h>
#endif

#include <fmt/format.h>

namespace DB
{

#if USE_AWS_S3
namespace S3AuthSetting
{
    extern const S3AuthSettingsString external_id;
    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString role_session_name;
}
#endif

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
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

/// S3 client of the warehouse. It accesses the bucket with the warehouse credential.
/// The endpoint comes from `url_scheme_mappers` in the server config, AWS by default.
ObjectStoragePtr createObjectStorage([[maybe_unused]] ContextPtr context, [[maybe_unused]] const IcebergRESTCatalogWarehouseDefinition & definition)
{
#if USE_AWS_S3
    const auto & credential = definition.credential;
    S3::URI url(definition.base_location);

    auto s3_settings = std::make_unique<S3Settings>();
    s3_settings->request_settings.updateFromSettings(context->getSettingsRef(), /*if_changed*/ false);
    /// The server signs the STS `AssumeRole` call with its own identity, then uses the assumed role for S3.
    s3_settings->auth_settings[S3AuthSetting::role_arn] = credential.role_arn;
    s3_settings->auth_settings[S3AuthSetting::external_id] = credential.external_id;
    s3_settings->auth_settings[S3AuthSetting::role_session_name] = credential.role_session_name;

    auto client = getClient(url, *s3_settings, context, /*for_disk_s3*/ false);
    return std::make_shared<S3ObjectStorage>(
        std::move(client),
        std::move(s3_settings),
        url,
        S3Capabilities{},
        /*key_generator*/ nullptr,
        /*disk_name*/ "IcebergRESTCatalog",
        /*for_disk_s3*/ false,
        /*credentials_refresh_callback*/ S3ObjectStorage::S3CredentialsRefreshCallback{},
        /*client_restricts_server_credentials*/ context->shouldRestrictUserQueryS3Credentials());
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The Iceberg REST catalog needs S3 support, but this build has none");
#endif
}

}

IcebergRESTCatalogWarehouse::IcebergRESTCatalogWarehouse(
    String name_, String base_location_, KeeperIcebergRESTCatalogStorePtr store_, ObjectStoragePtr object_storage_)
    : name(std::move(name_)), base_location(std::move(base_location_)), object_storage(std::move(object_storage_)), store(std::move(store_))
{
    /// The warehouse has one S3 client, bound to one bucket. Every location must be in that bucket.
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

IcebergRESTCatalogWarehouse::CreateNamespaceResult IcebergRESTCatalogWarehouse::createNamespace(const IcebergNamespaceName & ns, const std::map<String, String> & properties) const
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

IcebergRESTCatalogWarehouse::UpdateTableResult IcebergRESTCatalogWarehouse::updateTable(
    const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & expected, const IcebergTablePointer & new_pointer) const
{
    return store->updateTable(ns, table, expected, new_pointer);
}

String stripTrailingSlashes(String location)
{
    while (location.ends_with('/'))
        location.pop_back();
    return location;
}

IcebergRESTCatalogWarehousePtr createIcebergRESTCatalogWarehouse(
    ContextPtr context, const String & name, const IcebergRESTCatalogWarehouseDefinition & definition, const IcebergRESTCatalogKeeperConfig & keeper_config)
{
    auto store = std::make_shared<KeeperIcebergRESTCatalogStore>(
        [context, keeper_config] { return keeper_config.getZooKeeper(*context); },
        keeper_config.warehousePath(name) + "/" + toString(definition.uuid));
    auto object_storage = createObjectStorage(context, definition);
    return std::make_shared<const IcebergRESTCatalogWarehouse>(name, definition.base_location, std::move(store), std::move(object_storage));
}

}
