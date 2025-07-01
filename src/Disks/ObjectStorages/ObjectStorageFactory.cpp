#include <utility>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include <Disks/DiskType.h>
#include "config.h"
#if USE_AWS_S3
#include <Disks/ObjectStorages/S3/DiskS3Utils.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>
#endif
#if USE_HDFS
#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>
#endif
#if USE_AZURE_BLOB_STORAGE
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#endif
#include <Disks/ObjectStorages/Web/WebObjectStorage.h>
#include <Disks/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/loadLocalDiskConfig.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/PlainObjectStorage.h>
#include <Disks/ObjectStorages/PlainRewritableObjectStorage.h>
#include <Disks/ObjectStorages/createMetadataStorageMetrics.h>
#include <Interpreters/Context.h>
#include <Common/Macros.h>
#include <Core/Settings.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 hdfs_replication;
}

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

bool isCompatibleWithMetadataStorage(
    ObjectStorageType storage_type,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    MetadataStorageType target_metadata_type)
{
    auto compatibility_hint = MetadataStorageFactory::getCompatibilityMetadataTypeHint(storage_type);
    auto metadata_type = MetadataStorageFactory::getMetadataType(config, config_prefix, compatibility_hint);
    return metadataTypeFromString(metadata_type) == target_metadata_type;
}

bool isPlainStorage(ObjectStorageType type, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    return isCompatibleWithMetadataStorage(type, config, config_prefix, MetadataStorageType::Plain);
}

bool isPlainRewritableStorage(ObjectStorageType type, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    return isCompatibleWithMetadataStorage(type, config, config_prefix, MetadataStorageType::PlainRewritable);
}

template <typename BaseObjectStorage, class... Args>
ObjectStoragePtr createObjectStorage(
    ObjectStorageType type, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, Args &&... args)
{
    if (isPlainStorage(type, config, config_prefix))
        return std::make_shared<PlainObjectStorage<BaseObjectStorage>>(std::forward<Args>(args)...);

    if (isPlainRewritableStorage(type, config, config_prefix))
    {
        /// HDFS object storage currently does not support iteration and does not implement listObjects method.
        /// StaticWeb object storage is read-only and works with its dedicated metadata type.
        constexpr auto supported_object_storage_types
            = std::array{ObjectStorageType::S3, ObjectStorageType::Local, ObjectStorageType::Azure};
        if (std::find(supported_object_storage_types.begin(), supported_object_storage_types.end(), type)
            == supported_object_storage_types.end())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "plain_rewritable metadata storage support is not implemented for '{}' object storage",
                DataSourceDescription{DataSourceType::ObjectStorage, type, MetadataStorageType::PlainRewritable, /*description*/ ""}
                    .toString());

        auto metadata_storage_metrics = DB::MetadataStorageMetrics::create<BaseObjectStorage, MetadataStorageType::PlainRewritable>();
        return std::make_shared<PlainRewritableObjectStorage<BaseObjectStorage>>(
            std::move(metadata_storage_metrics), std::forward<Args>(args)...);
    }

    return std::make_shared<BaseObjectStorage>(std::forward<Args>(args)...);
}
}

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
    bool skip_access_check) const
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

    return it->second(name, config, config_prefix, context, skip_access_check);
}

#if USE_AWS_S3
namespace
{

S3::URI getS3URI(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const ContextPtr & context)
{
    String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
    String endpoint_subpath;
    if (config.has(config_prefix + ".endpoint_subpath"))
        endpoint_subpath = context->getMacros()->expand(config.getString(config_prefix + ".endpoint_subpath"));

    S3::URI uri(fs::path(endpoint) / endpoint_subpath);

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

void registerS3ObjectStorage(ObjectStorageFactory & factory)
{
    static constexpr auto disk_type = "s3";

    factory.registerObjectStorageType(disk_type, [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* skip_access_check */) -> ObjectStoragePtr
    {
        auto uri = getS3URI(config, config_prefix, context);
        auto s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);
        auto endpoint = getEndpoint(config, config_prefix, context);
        auto settings = getSettings(config, config_prefix, context, endpoint, /* validate_settings */true);
        auto client = getClient(endpoint, *settings, context, /* for_disk_s3 */true);
        auto key_generator = getKeyGenerator(uri, config, config_prefix);

        auto object_storage = createObjectStorage<S3ObjectStorage>(
            ObjectStorageType::S3, config, config_prefix, std::move(client), std::move(settings), uri, s3_capabilities, key_generator, name);

        return object_storage;
    });
}

void registerS3PlainObjectStorage(ObjectStorageFactory & factory)
{
    static constexpr auto disk_type = "s3_plain";

    factory.registerObjectStorageType(disk_type, [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* skip_access_check */) -> ObjectStoragePtr
    {
        auto uri = getS3URI(config, config_prefix, context);
        auto s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);
        auto endpoint = getEndpoint(config, config_prefix, context);
        auto settings = getSettings(config, config_prefix, context, endpoint, /* validate_settings */true);
        auto client = getClient(endpoint, *settings, context, /* for_disk_s3 */true);
        auto key_generator = getKeyGenerator(uri, config, config_prefix);

        auto object_storage = std::make_shared<PlainObjectStorage<S3ObjectStorage>>(
            std::move(client), std::move(settings), uri, s3_capabilities, key_generator, name);

        return object_storage;
    });
}

void registerS3PlainRewritableObjectStorage(ObjectStorageFactory & factory)
{
    static constexpr auto disk_type = "s3_plain_rewritable";

    factory.registerObjectStorageType(
        disk_type,
        [](const std::string & name,
           const Poco::Util::AbstractConfiguration & config,
           const std::string & config_prefix,
           const ContextPtr & context,
           bool /* skip_access_check */) -> ObjectStoragePtr
        {
            auto uri = getS3URI(config, config_prefix, context);
            auto s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);
            auto endpoint = getEndpoint(config, config_prefix, context);
            auto settings = getSettings(config, config_prefix, context, endpoint, /* validate_settings */true);
            auto client = getClient(endpoint, *settings, context, /* for_disk_s3 */true);
            auto key_generator = getKeyGenerator(uri, config, config_prefix);

            auto metadata_storage_metrics = DB::MetadataStorageMetrics::create<S3ObjectStorage, MetadataStorageType::PlainRewritable>();
            auto object_storage = std::make_shared<PlainRewritableObjectStorage<S3ObjectStorage>>(
                std::move(metadata_storage_metrics), std::move(client), std::move(settings), uri, s3_capabilities, key_generator, name);

            return object_storage;
        });
}

#endif

#if USE_HDFS
void registerHDFSObjectStorage(ObjectStorageFactory & factory)
{
    factory.registerObjectStorageType(
        "hdfs",
        [](const std::string & /* name */,
           const Poco::Util::AbstractConfiguration & config,
           const std::string & config_prefix,
           const ContextPtr & context,
           bool /* skip_access_check */) -> ObjectStoragePtr
        {
            auto uri = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
            checkHDFSURL(uri);
            if (uri.back() != '/')
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDFS path must ends with '/', but '{}' doesn't.", uri);

            std::unique_ptr<HDFSObjectStorageSettings> settings = std::make_unique<HDFSObjectStorageSettings>(
                config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024), context->getSettingsRef()[Setting::hdfs_replication]);

            return createObjectStorage<HDFSObjectStorage>(ObjectStorageType::HDFS, config, config_prefix, uri, std::move(settings), config, /* lazy_initialize */false);
        });
}
#endif

#if USE_AZURE_BLOB_STORAGE
void registerAzureObjectStorage(ObjectStorageFactory & factory)
{
    auto creator = [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* skip_access_check */) -> ObjectStoragePtr
    {
        auto azure_settings = AzureBlobStorage::getRequestSettings(config, config_prefix, context->getSettingsRef());

        AzureBlobStorage::ConnectionParams params
        {
            .endpoint = AzureBlobStorage::processEndpoint(config, config_prefix),
            .auth_method = AzureBlobStorage::getAuthMethod(config, config_prefix),
            .client_options = AzureBlobStorage::getClientOptions(*azure_settings, /*for_disk=*/ true),
        };

        return createObjectStorage<AzureObjectStorage>(
            ObjectStorageType::Azure, config, config_prefix, name,
            params.auth_method, AzureBlobStorage::getContainerClient(params, /*readonly=*/ false), std::move(azure_settings),
            params.endpoint.prefix.empty() ? params.endpoint.container_name : params.endpoint.container_name + "/" + params.endpoint.prefix,
            params.endpoint.getServiceEndpoint());
    };

    factory.registerObjectStorageType("azure_blob_storage", creator);
    factory.registerObjectStorageType("azure", creator);
}
#endif

void registerWebObjectStorage(ObjectStorageFactory & factory)
{
    factory.registerObjectStorageType("web", [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* skip_access_check */) -> ObjectStoragePtr
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

        return createObjectStorage<WebObjectStorage>(ObjectStorageType::Web, config, config_prefix, uri, context);
    });
}

void registerLocalObjectStorage(ObjectStorageFactory & factory)
{
    auto creator = [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* skip_access_check */) -> ObjectStoragePtr
    {
        String object_key_prefix;
        UInt64 keep_free_space_bytes;
        loadDiskLocalConfig(name, config, config_prefix, context, object_key_prefix, keep_free_space_bytes);

        /// keys are mapped to the fs, object_key_prefix is a directory also
        fs::create_directories(object_key_prefix);

        bool read_only = config.getBool(config_prefix + ".readonly", false);
        LocalObjectStorageSettings settings(object_key_prefix, read_only);

        return createObjectStorage<LocalObjectStorage>(ObjectStorageType::Local, config, config_prefix, settings);
    };

    factory.registerObjectStorageType("local_blob_storage", creator);
    factory.registerObjectStorageType("local", creator);
}

void registerObjectStorages()
{
    auto & factory = ObjectStorageFactory::instance();

#if USE_AWS_S3
    registerS3ObjectStorage(factory);
    registerS3PlainObjectStorage(factory);
    registerS3PlainRewritableObjectStorage(factory);
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

}
