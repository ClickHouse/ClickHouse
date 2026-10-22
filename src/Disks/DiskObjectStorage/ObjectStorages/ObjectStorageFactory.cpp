#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>

#if USE_AWS_S3
#include <Disks/DiskObjectStorage/ObjectStorages/S3/DiskS3Utils.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/diskSettings.h>
#endif

#if USE_HDFS
#include <Disks/DiskObjectStorage/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>
#endif

#if USE_AZURE_BLOB_STORAGE
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/getDiskConfigurationFromAST.h>
#endif

#include <Disks/DiskObjectStorage/ObjectStorages/Web/WebObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/loadLocalDiskConfig.h>

#include <Interpreters/Context.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/RemoteHostFilter.h>
#include <Common/logger_useful.h>

#include <Common/Macros.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>

#include <filesystem>
#include <set>
#include <utility>

#include <fmt/ranges.h>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 hdfs_replication;
}

namespace ServerSetting
{
    extern const ServerSettingsBool s3_load_table_anonymously_if_credentials_restricted;
}

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

#if USE_AWS_S3

namespace S3AuthSetting
{
    extern const S3AuthSettingsS3UriStyle uri_style;
}

#endif

ObjectStorageFactory & ObjectStorageFactory::instance()
{
    static ObjectStorageFactory factory;
    return factory;
}

void ObjectStorageFactory::registerObjectStorageType(const std::string & type, Creator creator)
{
    if (!registry.emplace(type, creator).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ObjectStorageFactory: the metadata type '{}' is not unique", type);
}

ObjectStoragePtr ObjectStorageFactory::create(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const ContextPtr & context,
    bool run_access_check,
    bool run_local_paths_check,
    RemoteHostFilterCheck remote_host_filter_check) const
{
    std::string type;
    if (config.has(config_prefix + ".object_storage_type"))
        type = config.getString(config_prefix + ".object_storage_type");
    else if (config.has(config_prefix + ".type")) /// .type -- for compatibility.
        type = config.getString(config_prefix + ".type");
    else
    {
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Expected `object_storage_type` in config");
    }

    const auto it = registry.find(type);

    if (it == registry.end())
    {
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                        "ObjectStorageFactory: unknown object storage type: {}", type);
    }

    return it->second(name, config, config_prefix, context, run_access_check, run_local_paths_check, remote_host_filter_check);
}

#if USE_AWS_S3
namespace
{

S3::URI getS3URI(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const ContextPtr & context, S3UriStyle uri_style)
{
    String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
    String endpoint_subpath;
    if (config.has(config_prefix + ".endpoint_subpath"))
        endpoint_subpath = context->getMacros()->expand(config.getString(config_prefix + ".endpoint_subpath"));

    S3::URI uri(fs::path(endpoint) / endpoint_subpath, false, true, uri_style);

    /// An empty key remains empty.
    if (!uri.key.empty() && !uri.key.ends_with('/'))
        uri.key.push_back('/');

    return uri;
}

}

static std::string getEndpoint(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context)
{
    return context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
}

/// A custom disk with an endpoint that remote_url_allow_hosts does not allow: a log line per disk
/// and one system.warnings entry that lists all such disks.
static void warnAboutDiskNotAllowedByRemoteHostFilter(const std::string & disk_name, const S3::URI & uri, const ContextPtr & global_context)
{
    Poco::URI endpoint_uri(uri.endpoint);
    /// host[:port], as in an <s3_bucket> entry.
    std::string host_and_port = endpoint_uri.getAuthority();
    if (auto at = host_and_port.rfind('@'); at != std::string::npos)
        host_and_port.erase(0, at + 1);
    const std::string s3_bucket_entry = host_and_port + "/" + uri.bucket;

    LOG_WARNING(
        getLogger("ObjectStorageFactory"),
        "Custom disk `{}` uses endpoint `{}` that is not allowed by `remote_url_allow_hosts`. The disk was created before "
        "this check and is kept while loading existing metadata (server start or UNDROP). Add `<s3_bucket>{}</s3_bucket>` "
        "to the list or recreate the tables that use it.",
        disk_name, s3_bucket_entry, s3_bucket_entry);

    /// system.warnings keeps one message per type, so the message lists all disks seen so far.
    /// No lock: disk creation runs under storage_policies_mutex in Context::getOrCreateDisk.
    static std::set<std::string> disks;
    disks.insert(fmt::format("`{}` -> `{}`", disk_name, s3_bucket_entry));
    global_context->addOrUpdateWarningMessage(
        Context::WarningType::CUSTOM_DISK_NOT_ALLOWED_BY_REMOTE_URL_ALLOW_HOSTS,
        PreformattedMessage::create(
            "Custom disks with endpoints that are not allowed by `remote_url_allow_hosts` were kept while loading existing "
            "metadata (server start or UNDROP): {}. Add `<s3_bucket>host/bucket</s3_bucket>` entries for them or recreate "
            "the tables that use them.",
            fmt::join(disks, ", ")));
}

static void registerS3ObjectStorage(ObjectStorageFactory & factory)
{
     auto creator = [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* run_access_check */,
        bool /* run_local_paths_check */,
        RemoteHostFilterCheck remote_host_filter_check) -> ObjectStoragePtr
    {
        auto s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);
        auto endpoint = getEndpoint(config, config_prefix, context);
        auto settings = std::make_unique<S3Settings>();
        settings->loadFromConfigForObjectStorage(config, config_prefix, context->getSettingsRef(), Poco::URI(endpoint).getScheme(), true);
        auto uri = getS3URI(config, config_prefix, context, settings->auth_settings[S3AuthSetting::uri_style]);
        /// The endpoint of a disk created in SQL is user input, so it is subject to `remote_url_allow_hosts`
        /// like the URL of an `s3` table; checked before the client is built, which already talks to the endpoint.
        /// For a disk loaded from existing metadata a rejection is only reported: throwing here would fail
        /// the table load, or the server start for a database metadata disk.
        if (remote_host_filter_check != RemoteHostFilterCheck::None
            && !uri.checkRemoteHostFilter(
                context->getGlobalContext()->getRemoteHostFilter(),
                /* throw_on_error */ remote_host_filter_check == RemoteHostFilterCheck::Enforce))
            warnAboutDiskNotAllowedByRemoteHostFilter(name, uri, context->getGlobalContext());
        auto client = getClient(endpoint, *settings, context, /* for_disk_s3 */ true, name);
        auto key_generator = getKeyGenerator(uri, config, config_prefix);

        return std::make_shared<S3ObjectStorage>(std::move(client), std::move(settings), uri, s3_capabilities, key_generator, name);
    };

    factory.registerObjectStorageType("s3", creator);
    factory.registerObjectStorageType("s3_with_keeper", creator);
    factory.registerObjectStorageType("s3_plain", creator);
    factory.registerObjectStorageType("s3_plain_rewritable", creator);
}

#endif

#if USE_HDFS
static void registerHDFSObjectStorage(ObjectStorageFactory & factory)
{
    factory.registerObjectStorageType(
        "hdfs",
        [](const std::string & name,
           const Poco::Util::AbstractConfiguration & config,
           const std::string & config_prefix,
           const ContextPtr & context,
           bool /* run_access_check */,
           bool /* run_local_paths_check */,
        RemoteHostFilterCheck /* remote_host_filter_check */) -> ObjectStoragePtr
        {
            auto uri = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
            checkHDFSURL(uri);
            if (uri.back() != '/')
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDFS path must ends with '/', but '{}' doesn't.", uri);

            std::unique_ptr<HDFSObjectStorageSettings> settings = std::make_unique<HDFSObjectStorageSettings>(
                config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024), context->getSettingsRef()[Setting::hdfs_replication]);

            return std::make_shared<HDFSObjectStorage>(uri, std::move(settings), config, /* lazy_initialize */false, name);
        });
}
#endif

#if USE_AZURE_BLOB_STORAGE
static void registerAzureObjectStorage(ObjectStorageFactory & factory)
{
    auto creator = [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* run_access_check */,
        bool /* run_local_paths_check */,
        RemoteHostFilterCheck remote_host_filter_check) -> ObjectStoragePtr
    {
        auto azure_settings = AzureBlobStorage::getRequestSettings(config, config_prefix, context->getSettingsRef());

        /// AzureObjectStorage::getCommonKeyPrefix() was not implemented previousely by mistake and was always returning an empty string.
        /// However, we use this string as ZooKeeper path for disk with metadata in Keeper.
        /// So, all instances of azure-with-keeper were using the same (empty, or root) path.
        /// We keep using empty prefix by default for compatibility, but allow to configure another one
        const String & common_key_prefix = config.getString(config_prefix + ".common_key_prefix_for_azure", "");

        /// A custom disk is configured at the root, where `validateResolvedAzureDiskCredentials` has written the decision.
        const bool server_credentials_allowed = hasAzureServerCredentialsMarker(config);

        AzureBlobStorage::ConnectionParams params
        {
            .endpoint = AzureBlobStorage::processEndpoint(config, config_prefix),
            .auth_method = AzureBlobStorage::getAuthMethod(config, config_prefix),
            .client_options = AzureBlobStorage::getClientOptions(context, context->getSettingsRef(), *azure_settings, /*for_disk=*/ true),
            /// Only a custom disk, `disk(...)` in user SQL, is checked; a disk from the server config may use the server's identity.
            .forbid_implicit_credentials = remote_host_filter_check != RemoteHostFilterCheck::None
                && !server_credentials_allowed && context->shouldRestrictUserQueryS3Credentials(),
            /// A custom disk loaded from existing metadata gets no credentials instead of failing the table load.
            .anonymous_fallback_for_server_credentials = remote_host_filter_check == RemoteHostFilterCheck::Warn
                && context->getGlobalContext()->getServerSettings()[ServerSetting::s3_load_table_anonymously_if_credentials_restricted],
            .follows_session_restriction = false,
        };

        const bool drop_credentials = params.checkOrDropServerManagedCredentials();
        if (drop_credentials)
            LOG_WARNING(
                getLogger("ObjectStorageFactory"),
                "Loading the dynamic Azure disk {} with a client that has no credentials: it uses the server's own "
                "identity, which is restricted for user queries (s3_allow_server_credentials_in_user_queries = 0). "
                "The disk stays inaccessible; set s3_load_table_anonymously_if_credentials_restricted = 0 to fail "
                "loading instead.",
                name);

        return std::make_shared<AzureObjectStorage>(
            name,
            /// A client without credentials must not probe or create the container: the denied request would fail the load.
            AzureBlobStorage::getContainerClient(params, /*readonly=*/ drop_credentials), std::move(azure_settings),
            params, params.endpoint.prefix.empty() ? params.endpoint.container_name : params.endpoint.container_name + "/" + params.endpoint.prefix,
            params.endpoint.getServiceEndpoint(), common_key_prefix);
    };

    factory.registerObjectStorageType("azure_blob_storage", creator);
    factory.registerObjectStorageType("azure", creator);
    factory.registerObjectStorageType("azure_plain", creator);
    factory.registerObjectStorageType("azure_plain_rewritable", creator);
}
#endif

static void registerWebObjectStorage(ObjectStorageFactory & factory)
{
    factory.registerObjectStorageType("web", [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* run_access_check */,
        bool /* run_local_paths_check */,
        RemoteHostFilterCheck /* remote_host_filter_check */) -> ObjectStoragePtr
    {
        auto uri = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
        if (!uri.ends_with('/'))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "URI must end with '/', but '{}' doesn't.", uri);
        try
        {
            Poco::URI poco_uri(uri);
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Bad URI: `{}`. Error: {}", uri, e.what());
        }

        return std::make_shared<WebObjectStorage>(uri, "", context);
    });
}

static void registerLocalObjectStorage(ObjectStorageFactory & factory)
{
    auto creator = [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* run_access_check */,
        bool run_local_paths_check,
        RemoteHostFilterCheck /* remote_host_filter_check */) -> ObjectStoragePtr
    {
        String object_key_prefix;
        UInt64 keep_free_space_bytes = 0;
        loadDiskLocalConfig(name, config, config_prefix, context, object_key_prefix, keep_free_space_bytes);

        if (run_local_paths_check)
            checkCustomLocalDiskPath(object_key_prefix, context);

        /// keys are mapped to the fs, object_key_prefix is a directory also
        fs::create_directories(object_key_prefix);

        bool read_only = config.getBool(config_prefix + ".readonly", false);
        LocalObjectStorageSettings settings(name, object_key_prefix, read_only);

        return std::make_shared<LocalObjectStorage>(settings);
    };

    factory.registerObjectStorageType("local_blob_storage", creator);
    factory.registerObjectStorageType("local", creator);
    factory.registerObjectStorageType("local_plain", creator);
    factory.registerObjectStorageType("local_plain_rewritable", creator);
}

void registerObjectStorages();

void registerObjectStorages()
{
    auto & factory = ObjectStorageFactory::instance();

#if USE_AWS_S3
    registerS3ObjectStorage(factory);
#endif

#if USE_HDFS
    registerHDFSObjectStorage(factory);
#endif

#if USE_AZURE_BLOB_STORAGE
    registerAzureObjectStorage(factory);
#endif

    registerWebObjectStorage(factory);
    registerLocalObjectStorage(factory);
}

void ObjectStorageFactory::clearRegistry()
{
    registry.clear();
}
}
