#include "config.h"

#include <memory>
#include <optional>
#include <Core/Settings.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/ConstChunkGenerator.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/ExtractColumnsTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/HivePartitioningUtils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/VirtualColumnUtils.h>
#if ENABLE_DISTRIBUTED_CACHE
#    include <Disks/IO/ReadBufferFromDistributedCache.h>
#    include <DistributedCache/DistributedCacheRegistry.h>
#    include <IO/DistributedCacheSettings.h>
#endif


namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_download_buffer_size;
extern const SettingsBool use_cache_for_count_from_files;
extern const SettingsString filesystem_cache_name;
extern const SettingsUInt64 filesystem_cache_boundary_alignment;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

std::optional<String> checkAndGetNewFileOnInsertIfNeeded(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const StorageObjectStorageQuerySettings & settings,
    const String & key,
    size_t sequence_number)
{
    if (settings.truncate_on_insert
        || !object_storage.exists(StoredObject(key)))
        return std::nullopt;

    if (settings.create_new_file_on_insert)
    {
        auto pos = key.find_first_of('.');
        String new_key;
        do
        {
            new_key = key.substr(0, pos) + "." + std::to_string(sequence_number) + (pos == std::string::npos ? "" : key.substr(pos));
            ++sequence_number;
        }
        while (object_storage.exists(StoredObject(new_key)));

        return new_key;
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Object in bucket {} with key {} already exists. "
        "If you want to overwrite it, enable setting {}_truncate_on_insert, if you "
        "want to create a new file on each insert, enable setting {}_create_new_file_on_insert",
        configuration.getNamespace(), key, configuration.getTypeName(), configuration.getTypeName());
}

void resolveSchemaAndFormat(
    ColumnsDescription & columns,
    std::string & format,
    ObjectStoragePtr object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    std::optional<FormatSettings> format_settings,
    std::string & sample_path,
    const ContextPtr & context)
{
    if (format == "auto")
    {
        if (configuration->isDataLakeConfiguration())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Format must be already specified for {} storage.",
                configuration->getTypeName());
        }
    }

    if (columns.empty())
    {
        if (configuration->isDataLakeConfiguration())
        {
            auto table_structure = configuration->tryGetTableStructureFromMetadata();
            if (table_structure)
                columns = table_structure.value();
        }

        if (columns.empty())
        {
            if (format == "auto")
            {
                std::tie(columns, format) = StorageObjectStorage::resolveSchemaAndFormatFromData(
                    object_storage, configuration, format_settings, sample_path, context);
            }
            else
            {
                chassert(!format.empty());
                columns = StorageObjectStorage::resolveSchemaFromData(object_storage, configuration, format_settings, sample_path, context);
            }
        }
    }
    else if (format == "auto")
    {
        format = StorageObjectStorage::resolveFormatFromData(object_storage, configuration, format_settings, sample_path, context);
    }

    validateSupportedColumns(columns, *configuration);
}

void validateSupportedColumns(
    ColumnsDescription & columns,
    const StorageObjectStorageConfiguration & configuration)
{
    if (!columns.hasOnlyOrdinary())
    {
        /// We don't allow special columns.
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Special columns like MATERIALIZED, ALIAS or EPHEMERAL are not supported for {} storage.",
            configuration.getTypeName());
    }
}


std::unique_ptr<ReadBufferFromFileBase> createReadBuffer(
    RelativePathWithMetadata & object_info,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context_,
    const LoggerPtr & log,
    const std::optional<ReadSettings> & read_settings)
{
    const auto & settings = context_->getSettingsRef();
    const auto & effective_read_settings = read_settings.has_value() ? read_settings.value() : context_->getReadSettings();

    bool use_distributed_cache = false;
#if ENABLE_DISTRIBUTED_CACHE
    ObjectStorageConnectionInfoPtr connection_info;
    if (settings[Setting::table_engine_read_through_distributed_cache]
        && DistributedCache::Registry::instance().isReady(effective_read_settings.distributed_cache_settings.read_only_from_current_az))
    {
        connection_info = object_storage->getConnectionInfo();
        if (connection_info)
            use_distributed_cache = true;
    }
#endif

    bool use_filesystem_cache = false;
    std::string filesystem_cache_name;
    if (!use_distributed_cache)
    {
        filesystem_cache_name = settings[Setting::filesystem_cache_name].value;
        use_filesystem_cache = effective_read_settings.enable_filesystem_cache && !filesystem_cache_name.empty()
            && (object_storage->getType() == ObjectStorageType::Azure || object_storage->getType() == ObjectStorageType::S3);
    }

    /// We need object metadata for two cases:
    /// 1. object size suggests whether we need to use prefetch
    /// 2. object etag suggests a cache key in case we use filesystem cache
    if (!object_info.metadata)
        object_info.metadata = object_storage->getObjectMetadata(object_info.relative_path);

    const auto & object_size = object_info.metadata->size_bytes;

    auto modified_read_settings = effective_read_settings.adjustBufferSize(object_size);
    /// FIXME: Changing this setting to default value breaks something around parquet reading
    modified_read_settings.remote_read_min_bytes_for_seek = modified_read_settings.remote_fs_buffer_size;
    /// User's object may change, don't cache it.
    modified_read_settings.use_page_cache_for_disks_without_file_cache = false;
    modified_read_settings.filesystem_cache_boundary_alignment = settings[Setting::filesystem_cache_boundary_alignment];

    // Create a read buffer that will prefetch the first ~1 MB of the file.
    // When reading lots of tiny files, this prefetching almost doubles the throughput.
    // For bigger files, parallel reading is more useful.
    const bool object_too_small = object_size <= 2 * context_->getSettingsRef()[Setting::max_download_buffer_size];
    const bool use_prefetch = object_too_small && modified_read_settings.remote_fs_method == RemoteFSReadMethod::threadpool
        && modified_read_settings.remote_fs_prefetch;

    bool use_async_buffer = false;
    ReadSettings nested_buffer_read_settings = modified_read_settings;
    if (use_prefetch || use_filesystem_cache || use_distributed_cache)
    {
        nested_buffer_read_settings.remote_read_buffer_use_external_buffer = true;

        /// FIXME: Use async buffer if use_cache,
        /// because CachedOnDiskReadBufferFromFile does not work as an independent buffer currently.
        use_async_buffer = true;
    }

    std::unique_ptr<ReadBufferFromFileBase> impl;
#if ENABLE_DISTRIBUTED_CACHE
    if (use_distributed_cache)
    {
        const std::string path = object_info.getPath();
        StoredObject object(path, "", object_size);
        auto read_buffer_creator = [object, nested_buffer_read_settings, object_storage]()
        { return object_storage->readObject(object, nested_buffer_read_settings); };

        impl = std::make_unique<ReadBufferFromDistributedCache>(
            path,
            StoredObjects({object}),
            effective_read_settings,
            connection_info,
            ConnectionTimeouts::getTCPTimeoutsWithoutFailover(context_->getSettingsRef()),
            read_buffer_creator,
            /*use_external_buffer*/ use_async_buffer,
            context_->getDistributedCacheLog(),
            /* include_credentials_in_cache_key */ true);
    }
    else if (use_filesystem_cache)
#else
    if (use_filesystem_cache)
#endif
    {
        chassert(object_info.metadata.has_value());
        if (object_info.metadata->etag.empty())
        {
            LOG_WARNING(log, "Cannot use filesystem cache, no etag specified");
        }
        else
        {
            SipHash hash;
            hash.update(object_info.relative_path);
            hash.update(object_info.metadata->etag);

            const auto cache_key = FileCacheKey::fromKey(hash.get128());
            auto cache = FileCacheFactory::instance().get(filesystem_cache_name);

            auto read_buffer_creator = [path = object_info.relative_path, object_size, nested_buffer_read_settings, object_storage]()
            { return object_storage->readObject(StoredObject(path, "", object_size), nested_buffer_read_settings); };

            impl = std::make_unique<CachedOnDiskReadBufferFromFile>(
                object_info.relative_path,
                cache_key,
                cache,
                FileCache::getCommonUser(),
                read_buffer_creator,
                use_async_buffer ? nested_buffer_read_settings : modified_read_settings,
                std::string(CurrentThread::getQueryId()),
                object_size,
                /* allow_seeks */ true,
                /* use_external_buffer */ use_async_buffer,
                /* read_until_position */ std::nullopt,
                context_->getFilesystemCacheLog());

            LOG_TEST(
                log,
                "Using filesystem cache `{}` (path: {}, etag: {}, hash: {})",
                filesystem_cache_name,
                object_info.relative_path,
                object_info.metadata->etag,
                toString(hash.get128()));
        }
    }

    if (!impl)
        impl = object_storage->readObject(StoredObject(object_info.relative_path, "", object_size), nested_buffer_read_settings);

    if (!use_async_buffer)
        return impl;

    LOG_TRACE(log, "Downloading object of size {} with initial prefetch", object_size);

    bool prefer_bigger_buffer_size = effective_read_settings.filesystem_cache_prefer_bigger_buffer_size && impl->isCached();

    size_t buffer_size = prefer_bigger_buffer_size
        ? std::max<size_t>(effective_read_settings.remote_fs_buffer_size, effective_read_settings.prefetch_buffer_size)
        : effective_read_settings.remote_fs_buffer_size;

    if (object_size)
        buffer_size = std::min<size_t>(object_size, buffer_size);

    auto & reader = context_->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
    impl = std::make_unique<AsynchronousBoundedReadBuffer>(
        std::move(impl),
        reader,
        modified_read_settings,
        buffer_size,
        modified_read_settings.remote_read_min_bytes_for_seek,
        context_->getAsyncReadCounters(),
        context_->getFilesystemReadPrefetchesLog());

    if (use_prefetch)
    {
        impl->setReadUntilEnd();
        impl->prefetch(DEFAULT_PREFETCH_PRIORITY);
    }

    return impl;
}

std::string getUniqueStoragePathIdentifier(
    const StorageObjectStorageConfiguration & configuration, const ObjectInfoBase & object_info, bool include_connection_info)
{
    auto path = object_info.getPath();
    if (path.starts_with("/"))
        path = path.substr(1);

    if (include_connection_info)
        return fs::path(configuration.getDataSourceDescription()) / path;
    return fs::path(configuration.getNamespace()) / path;
}

ObjectInfos convertRelativePathsToPlainObjectInfos(RelativePathsWithMetadata && relative_paths)
{
    ObjectInfos object_infos;
    object_infos.reserve(relative_paths.size());

    for (const auto & relative_path : relative_paths)
    {
        object_infos.emplace_back(std::make_shared<ObjectInfoPlain>(std::move(*relative_path)));
    }

    return object_infos;
}
}
