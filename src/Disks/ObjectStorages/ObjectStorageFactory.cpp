#include "config.h"
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#if USE_AWS_S3
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <Disks/ObjectStorages/S3/DiskS3Utils.h>
#endif
#if USE_HDFS && !defined(CLICKHOUSE_KEEPER_STANDALONE_BUILD)
#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Storages/HDFS/HDFSCommon.h>
#endif
#if USE_AZURE_BLOB_STORAGE && !defined(CLICKHOUSE_KEEPER_STANDALONE_BUILD)
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>
#endif
#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD
#include <Disks/ObjectStorages/Web/WebObjectStorage.h>
#include <Disks/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/loadLocalDiskConfig.h>
#endif
#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/PlainObjectStorage.h>
#include <Interpreters/Context.h>
#include <Common/Macros.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
    bool isPlainStorage(
        ObjectStorageType type,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix)
    {
        auto compatibility_hint = MetadataStorageFactory::getCompatibilityMetadataTypeHint(type);
        auto metadata_type = MetadataStorageFactory::getMetadataType(config, config_prefix, compatibility_hint);
        return metadataTypeFromString(metadata_type) == MetadataStorageType::Plain;
    }

    template <typename BaseObjectStorage, class ...Args>
    ObjectStoragePtr createObjectStorage(
        ObjectStorageType type,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        Args && ...args)
    {
        if (isPlainStorage(type, config, config_prefix))
        {
            return std::make_shared<PlainObjectStorage<BaseObjectStorage>>(std::forward<Args>(args)...);
        }
        else
        {
            return std::make_shared<BaseObjectStorage>(std::forward<Args>(args)...);
        }
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
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "ObjectStorageFactory: the metadata type '{}' is not unique", type);
    }
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
    {
        type = config.getString(config_prefix + ".object_storage_type");
    }
    else if (config.has(config_prefix + ".type")) /// .type -- for compatibility.
    {
        type = config.getString(config_prefix + ".type");
    }
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
    S3::URI uri(endpoint);

    /// An empty key remains empty.
    if (!uri.key.empty() && !uri.key.ends_with('/'))
        uri.key.push_back('/');

    return uri;
}

void checkS3Capabilities(
    S3ObjectStorage & storage, const S3Capabilities s3_capabilities, const String & name)
{
    /// If `support_batch_delete` is turned on (default), check and possibly switch it off.
    if (s3_capabilities.support_batch_delete && !checkBatchRemove(storage))
    {
        LOG_WARNING(
            getLogger("S3ObjectStorage"),
            "Storage for disk {} does not support batch delete operations, "
            "so `s3_capabilities.support_batch_delete` was automatically turned off during the access check. "
            "To remove this message set `s3_capabilities.support_batch_delete` for the disk to `false`.",
            name);
        storage.setCapabilitiesSupportBatchDelete(false);
    }
}
}

void registerS3ObjectStorage(ObjectStorageFactory & factory)
{
    static constexpr auto disk_type = "s3";

    factory.registerObjectStorageType(disk_type, [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool skip_access_check) -> ObjectStoragePtr
    {
        auto uri = getS3URI(config, config_prefix, context);
        auto s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);
        auto settings = getSettings(config, config_prefix, context);
        auto client = getClient(config, config_prefix, context, *settings);
        auto key_generator = getKeyGenerator(uri, config, config_prefix);

        auto object_storage = createObjectStorage<S3ObjectStorage>(
            ObjectStorageType::S3, config, config_prefix, std::move(client), std::move(settings), uri, s3_capabilities, key_generator, name);

        /// NOTE: should we still perform this check for clickhouse-disks?
        if (!skip_access_check)
            checkS3Capabilities(*dynamic_cast<S3ObjectStorage *>(object_storage.get()), s3_capabilities, name);

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
        bool skip_access_check) -> ObjectStoragePtr
    {
        /// send_metadata changes the filenames (includes revision), while
        /// s3_plain do not care about this, and expect that the file name
        /// will not be changed.
        ///
        /// And besides, send_metadata does not make sense for s3_plain.
        if (config.getBool(config_prefix + ".send_metadata", false))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "s3_plain does not supports send_metadata");

        auto uri = getS3URI(config, config_prefix, context);
        auto s3_capabilities = getCapabilitiesFromConfig(config, config_prefix);
        auto settings = getSettings(config, config_prefix, context);
        auto client = getClient(config, config_prefix, context, *settings);
        auto key_generator = getKeyGenerator(uri, config, config_prefix);

        auto object_storage = std::make_shared<PlainObjectStorage<S3ObjectStorage>>(
            std::move(client), std::move(settings), uri, s3_capabilities, key_generator, name);

        /// NOTE: should we still perform this check for clickhouse-disks?
        if (!skip_access_check)
            checkS3Capabilities(*dynamic_cast<S3ObjectStorage *>(object_storage.get()), s3_capabilities, name);

        return object_storage;
    });
}
#endif

#if USE_HDFS && !defined(CLICKHOUSE_KEEPER_STANDALONE_BUILD)
void registerHDFSObjectStorage(ObjectStorageFactory & factory)
{
    factory.registerObjectStorageType("hdfs", [](
        const std::string & /* name */,
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
            config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
            config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000),
            context->getSettingsRef().hdfs_replication
        );

        return createObjectStorage<HDFSObjectStorage>(ObjectStorageType::HDFS, config, config_prefix, uri, std::move(settings), config);
    });
}
#endif

#if USE_AZURE_BLOB_STORAGE && !defined(CLICKHOUSE_KEEPER_STANDALONE_BUILD)
void registerAzureObjectStorage(ObjectStorageFactory & factory)
{
    auto creator = [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* skip_access_check */) -> ObjectStoragePtr
    {
        AzureBlobStorageEndpoint endpoint = processAzureBlobStorageEndpoint(config, config_prefix);
        return createObjectStorage<AzureObjectStorage>(
            ObjectStorageType::Azure, config, config_prefix, name,
            getAzureBlobContainerClient(config, config_prefix),
            getAzureBlobStorageSettings(config, config_prefix, context),
            endpoint.prefix.empty() ? endpoint.container_name : endpoint.container_name + "/" + endpoint.prefix);
    };
    factory.registerObjectStorageType("azure_blob_storage", creator);
    factory.registerObjectStorageType("azure", creator);
}
#endif

#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD
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
        return createObjectStorage<LocalObjectStorage>(ObjectStorageType::Local, config, config_prefix, object_key_prefix);
    };

    factory.registerObjectStorageType("local_blob_storage", creator);
    factory.registerObjectStorageType("local", creator);
}
#endif

void registerObjectStorages()
{
    auto & factory = ObjectStorageFactory::instance();

#if USE_AWS_S3
    registerS3ObjectStorage(factory);
    registerS3PlainObjectStorage(factory);
#endif

#if USE_HDFS && !defined(CLICKHOUSE_KEEPER_STANDALONE_BUILD)
    registerHDFSObjectStorage(factory);
#endif

#if USE_AZURE_BLOB_STORAGE && !defined(CLICKHOUSE_KEEPER_STANDALONE_BUILD)
    registerAzureObjectStorage(factory);
#endif

#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD
    registerWebObjectStorage(factory);
    registerLocalObjectStorage(factory);
#endif
}

}
