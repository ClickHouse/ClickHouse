#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouse.h>

#include "config.h"

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Server/IcebergRESTCatalog/KeeperIcebergRESTCatalogStore.h>

#if USE_AWS_S3
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/diskSettings.h>
#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/S3/URI.h>
#endif

#include <fmt/format.h>

#include <ranges>

namespace DB
{

#if USE_AWS_S3
namespace Setting
{
    extern const SettingsBool s3_validate_request_settings;
}

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsUInt64 expiration_window_seconds;
    extern const S3AuthSettingsString external_id;
    extern const S3AuthSettingsString role_arn;
    extern const S3AuthSettingsString role_session_name;
    extern const S3AuthSettingsSecretString secret_access_key;
    extern const S3AuthSettingsSecretString session_token;
    extern const S3AuthSettingsBool use_environment_credentials;
    extern const S3AuthSettingsBool use_insecure_imds_request;
}
#endif

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

struct WarehouseStorage
{
    ObjectStoragePtr object_storage;
    IcebergRESTCatalog::CredentialVendorPtr credential_vendor;
};

#if USE_AWS_S3
/// STS client for credential vending. It signs requests as server, with keys from `<s3>` server config or with environment credentials.
/// Vendor uses it to assume warehouse role.
std::shared_ptr<S3::AWSAssumeRoleClient> createVendingSTSClient(const S3::Client & warehouse_s3_client, const S3::S3AuthSettings & auth_settings)
{
    Aws::Auth::AWSCredentials server_keys(
        Aws::String(auth_settings[S3AuthSetting::access_key_id].value),
        Aws::String(auth_settings[S3AuthSetting::secret_access_key].value),
        Aws::String(auth_settings[S3AuthSetting::session_token].value));

    S3::CredentialsConfiguration server_identity;
    /// Environment credentials are the only source without keys in config.
    server_identity.use_environment_credentials = auth_settings[S3AuthSetting::use_environment_credentials] || server_keys.IsEmpty();
    server_identity.use_insecure_imds_request = auth_settings[S3AuthSetting::use_insecure_imds_request];
    server_identity.expiration_window_seconds = auth_settings[S3AuthSetting::expiration_window_seconds];
    /// Allows environment credentials. They sign only STS call and never reach a client.
    server_identity.forbid_implicit_credentials = false;

    /// Region, proxy and timeouts of warehouse S3 client.
    auto client_configuration = warehouse_s3_client.getClientConfiguration();
    /// STS requests are helper traffic. They are not connection of a logged request.
    client_configuration.publish_connection_info = false;

    auto server_credentials_provider = S3::getCredentialsProvider(client_configuration, server_keys, server_identity);
    return S3::AWSAssumeRoleClient::create(server_credentials_provider, client_configuration);
}

/// S3 object storage of warehouse and its credential vendor. Object storage accesses bucket with warehouse credential.
/// The endpoint comes from `url_scheme_mappers` in the server config, AWS by default.
WarehouseStorage createStorageImpl(ContextPtr context, const IcebergRESTCatalogWarehouseDefinition & definition)
{
    const auto & credential = definition.credential;
    S3::URI url(definition.base_location);

    /// Same initialization as `StorageS3Configuration`: the `<s3>` server config, the proxy resolver,
    /// the request throttlers, then the per-endpoint `<s3><endpoint>` overrides.
    auto s3_settings = std::make_unique<S3Settings>();
    s3_settings->loadFromConfigForObjectStorage(
        context->getConfigRef(), "s3", context->getSettingsRef(), url.uri.getScheme(), context->getSettingsRef()[Setting::s3_validate_request_settings]);
    if (auto endpoint_settings = context->getStorageS3Settings().getSettings(url.uri.toString(), context->getUserName()))
    {
        s3_settings->auth_settings.updateIfChanged(endpoint_settings->auth_settings);
        s3_settings->request_settings.updateIfChanged(endpoint_settings->request_settings);
    }
    /// The server signs the STS `AssumeRole` call with its own identity, then uses the assumed role for S3.
    s3_settings->auth_settings[S3AuthSetting::role_arn] = credential.role_arn;
    s3_settings->auth_settings[S3AuthSetting::external_id] = credential.external_id;
    s3_settings->auth_settings[S3AuthSetting::role_session_name] = credential.role_session_name;

    auto client = getClient(url, *s3_settings, context, /*for_disk_s3*/ false);
    /// Uses `client` and `s3_settings` before object storage takes them.
    auto sts_client = createVendingSTSClient(*client, s3_settings->auth_settings);

    auto object_storage = std::make_shared<S3ObjectStorage>(
        std::move(client),
        std::move(s3_settings),
        url,
        S3Capabilities{},
        /*key_generator*/ nullptr,
        /*disk_name*/ "IcebergRESTCatalog",
        /*for_disk_s3*/ false,
        /*credentials_refresh_callback*/ S3ObjectStorage::S3CredentialsRefreshCallback{},
        /*client_restricts_server_credentials*/ context->shouldRestrictUserQueryS3Credentials());

    auto credential_vendor = std::make_shared<const IcebergRESTCatalog::CredentialVendor>(
        sts_client, credential, object_storage->getS3StorageClient());

    return WarehouseStorage{.object_storage = std::move(object_storage), .credential_vendor = std::move(credential_vendor)};
}
#endif

WarehouseStorage createStorage(ContextPtr context, const IcebergRESTCatalogWarehouseDefinition & definition)
{
#if USE_AWS_S3
    return createStorageImpl(context, definition);
#else
    UNUSED(context, definition);
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The Iceberg REST catalog needs S3 support, but this build has none");
#endif
}

}

IcebergRESTCatalogWarehouse::IcebergRESTCatalogWarehouse(
    String name_,
    String base_location_,
    KeeperIcebergRESTCatalogStorePtr store_,
    ObjectStoragePtr object_storage_,
    IcebergRESTCatalog::CredentialVendorPtr credential_vendor_)
    : name(std::move(name_))
    , base_location(std::move(base_location_))
    , object_storage(std::move(object_storage_))
    , store(std::move(store_))
    , credential_vendor(std::move(credential_vendor_))
{
    /// The warehouse has one S3 client, bound to one bucket. Every location must be in that bucket.
    validateLocation(S3Location::parse(base_location));
}

void IcebergRESTCatalogWarehouse::validateLocation(const String & location) const
{
    validateLocation(S3Location::parse(location));

    /// Base locations of warehouses are disjoint, so tables of two warehouses never overlap.
    if (!stripTrailingSlashes(location).starts_with(base_location + "/"))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Location {} is outside base location {} of warehouse {}", location, base_location, name);
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

    /// Some clients normalize paths, so `a//b`, `a/./b` and `a/x/../b` would all name `a/b` for them.
    /// The overlap check compares strings, so only one spelling of a location is allowed.
    const String key = stripTrailingSlashes(location.key);
    for (const auto part : std::views::split(key, '/'))
    {
        const std::string_view segment(part.begin(), part.end());
        if (segment.empty() || segment == "." || segment == "..")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Location s3://{}/{} contains an empty, '.' or '..' path segment",
                location.bucket,
                location.key);
    }
}

String IcebergRESTCatalogWarehouse::objectKey(const String & location) const
{
    auto parsed = S3Location::parse(location);
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

IcebergRESTCatalogWarehouse::CreateTableOutcome IcebergRESTCatalogWarehouse::createTable(
    const IcebergNamespaceName & ns, const String & table, const IcebergTablePointer & pointer, const String & table_root) const
{
    validateLocation(table_root);
    return store->createTable(ns, table, pointer, table_root);
}

std::optional<IcebergRESTCatalogWarehouse::CreateTableOutcome> IcebergRESTCatalogWarehouse::checkForLocationConflict(
    const IcebergNamespaceName & ns, const String & table, const String & table_root) const
{
    validateLocation(table_root);
    return store->checkForLocationConflict(ns, table, table_root);
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

IcebergRESTCatalogWarehouse::RenameTableResult IcebergRESTCatalogWarehouse::renameTable(
    const IcebergNamespaceName & source_ns, const String & source, const IcebergNamespaceName & dest_ns, const String & dest) const
{
    return store->renameTable(source_ns, source, dest_ns, dest);
}

IcebergRESTCatalogWarehouse::UpdateTablesResult IcebergRESTCatalogWarehouse::updateTables(const std::vector<TableUpdate> & updates) const
{
    return store->updateTables(updates);
}

IcebergRESTCatalog::VendedCredentials IcebergRESTCatalogWarehouse::vendCredentials(const String & location, IcebergRESTCatalog::VendedAccessLevel access) const
{
    validateLocation(location);
    return credential_vendor->vend(location, access);
}

void IcebergRESTCatalogWarehouse::checkStorageAccess() const
{
#if USE_AWS_S3
    /// The credentials stay empty when STS refuses the role. S3 requests are then sent anonymously, and a public bucket accepts them.
    const auto credentials = object_storage->getS3StorageClient()->getCredentials();
    if (credentials.GetAWSAccessKeyId().empty() || credentials.GetAWSSecretKey().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot assume the role of warehouse {}. See the server log for the STS error", name);
#endif

    /// The trailing slash keeps the listing inside the warehouse. A role can be restricted to this prefix.
    auto prefix = objectKey(base_location);
    if (!prefix.empty())
        prefix += '/';

    RelativePathsWithMetadata children;
    object_storage->listObjects(prefix, children, /*max_keys*/ 1);
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
    auto storage = createStorage(context, definition);
    return std::make_shared<const IcebergRESTCatalogWarehouse>(
        name, definition.base_location, std::move(store), std::move(storage.object_storage), std::move(storage.credential_vendor));
}

}
