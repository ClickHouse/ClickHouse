#include "config.h"
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <Disks/ObjectStorages/S3/checkBatchRemove.h>
#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageAuth.h>
#include <Disks/ObjectStorages/Web/WebObjectStorage.h>
#include <Disks/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/loadLocalDiskConfig.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Interpreters/Context.h>
#include <Common/Macros.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

ObjectStorageFactory & ObjectStorageFactory::instance()
{
    static ObjectStorageFactory factory;
    return factory;
}

void ObjectStorageFactory::registerObjectStorageType(const std::string & metadata_type, Creator creator)
{
    if (!registry.emplace(metadata_type, creator).second)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "ObjectStorageFactory: the metadata type '{}' is not unique",
                        metadata_type);
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
    if (config.has(config_prefix + ".obejct_storage_type"))
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

static S3::URI getS3URI(const Poco::Util::AbstractConfiguration & config,
                        const std::string & config_prefix,
                        const ContextPtr & context)
{
    String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
    S3::URI uri(endpoint);
    if (!uri.key.ends_with('/'))
        uri.key.push_back('/');
    return uri;
}

#if USE_AWS_S3
void registerS3ObjectStorage(ObjectStorageFactory & factory)
{
    auto creator = [](
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

        auto object_storage = std::make_shared<S3ObjectStorage>(
            std::move(client), std::move(settings), uri.version_id,
            s3_capabilities, uri.bucket, uri.endpoint, uri.key, name);

        /// NOTE: should we still perform this check for clickhouse-disks?
        if (!skip_access_check)
        {
            /// If `support_batch_delete` is turned on (default), check and possibly switch it off.
            if (s3_capabilities.support_batch_delete && !checkBatchRemove(*object_storage, uri.key))
            {
                LOG_WARNING(
                    &Poco::Logger::get("S3ObjectStorage"),
                    "Storage for disk {} does not support batch delete operations, "
                    "so `s3_capabilities.support_batch_delete` was automatically turned off during the access check. "
                    "To remove this message set `s3_capabilities.support_batch_delete` for the disk to `false`.",
                    name
                );
                object_storage->setCapabilitiesSupportBatchDelete(false);
            }
        }
        return object_storage;
    };
    factory.registerObjectStorageType("s3", creator);
}

void registerS3PlainObjectStorage(ObjectStorageFactory & factory)
{
    auto creator = [](
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool /* skip_access_check */) -> ObjectStoragePtr
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

        return std::make_shared<S3PlainObjectStorage>(
            std::move(client), std::move(settings), uri.version_id,
            s3_capabilities, uri.bucket, uri.endpoint, uri.key, name);
    };
    factory.registerObjectStorageType("s3_plain", creator);
}
#endif

#if USE_HDFS
void registerHDFSObjectStorage(ObjectStorageFactory & factory)
{
    auto creator = [](
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

        return std::make_unique<HDFSObjectStorage>(uri, std::move(settings), config);
    };
    factory.registerObjectStorageType("hdfs", creator);
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
        return std::make_unique<AzureObjectStorage>(
            name,
            getAzureBlobContainerClient(config, config_prefix),
            getAzureBlobStorageSettings(config, config_prefix, context));

    };
    factory.registerObjectStorageType("azure_blob_storage", creator);
}
#endif

void registerWebObjectStorage(ObjectStorageFactory & factory)
{
    auto creator = [](
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

        return std::make_shared<WebObjectStorage>(uri, context);
    };
    factory.registerObjectStorageType("web", creator);
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
        return std::make_shared<LocalObjectStorage>(object_key_prefix);
    };
    factory.registerObjectStorageType("local", creator);
}

}
