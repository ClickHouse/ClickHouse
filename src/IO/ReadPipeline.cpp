#include <IO/ReadPipeline.h>

#include <Backups/IBackup.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/CachedInMemoryReadBufferFromFile.h>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/ReadBufferFromString.h>
#include <IO/FileEncryptionCommon.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheKey.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>

/// Helper for std::visit with multiple lambdas.
template <class... Ts>
struct overloaded : Ts...
{
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

void ReadPipeline::setSource(ObjectStoragePtr object_storage, StoredObjects objects, std::optional<size_t> read_hint)
{
    source = SourceStage{
        .objects = std::move(objects),
        .source = ObjectStorageSource{.storage = std::move(object_storage), .read_hint = read_hint}};
}

void ReadPipeline::setLocalFileSource(String path, StoredObjects objects, std::optional<size_t> read_hint)
{
    source = SourceStage{
        .objects = std::move(objects),
        .source = LocalFileSource{.path = std::move(path), .read_hint = read_hint}};
}

void ReadPipeline::setBackupSource(std::shared_ptr<IBackup> backup, String path, StoredObjects objects)
{
    source = SourceStage{
        .objects = std::move(objects),
        .source = BackupSource{.backup = std::move(backup), .path = std::move(path)}};
}

void ReadPipeline::setSource(StoredObjects objects, BufferCreator creator)
{
    source = SourceStage{
        .objects = std::move(objects),
        .source = CustomSource{.creator = std::move(creator)}};
}

void ReadPipeline::needGather()
{
    gather = true;
}

void ReadPipeline::needDiskCache(FileCachePtr cache, std::shared_ptr<FilesystemCacheLog> cache_log)
{
    disk_cache = DiskCacheStage{.cache = std::move(cache), .cache_log = std::move(cache_log), .cache_settings = std::nullopt, .custom_cache_key = std::nullopt, .custom_origin = std::nullopt};
}

void ReadPipeline::needDiskCache(FileCachePtr cache, FilesystemCacheSettings cache_settings, std::shared_ptr<FilesystemCacheLog> cache_log)
{
    disk_cache = DiskCacheStage{.cache = std::move(cache), .cache_log = std::move(cache_log), .cache_settings = std::move(cache_settings), .custom_cache_key = std::nullopt, .custom_origin = std::nullopt};
}

void ReadPipeline::needDiskCache(
    FileCachePtr cache,
    FileCacheKey cache_key,
    FileCacheOriginInfo origin,
    FilesystemCacheSettings cache_settings,
    std::shared_ptr<FilesystemCacheLog> cache_log)
{
    disk_cache = DiskCacheStage{
        .cache = std::move(cache),
        .cache_log = std::move(cache_log),
        .cache_settings = std::move(cache_settings),
        .custom_cache_key = std::move(cache_key),
        .custom_origin = std::move(origin)};
}

void ReadPipeline::needMemoryCache(std::shared_ptr<PageCache> cache, String cache_path_prefix)
{
    memory_cache = MemoryCacheStage{.cache = std::move(cache), .cache_path_prefix = std::move(cache_path_prefix), .page_cache_settings = std::nullopt, .custom_cache_path = {}, .custom_file_version = {}};
}

void ReadPipeline::needMemoryCache(std::shared_ptr<PageCache> cache, String cache_path_prefix, PageCacheSettings page_cache_settings)
{
    memory_cache = MemoryCacheStage{.cache = std::move(cache), .cache_path_prefix = std::move(cache_path_prefix), .page_cache_settings = std::move(page_cache_settings), .custom_cache_path = {}, .custom_file_version = {}};
}

void ReadPipeline::needMemoryCache(
    std::shared_ptr<PageCache> cache,
    String custom_cache_path,
    String custom_file_version,
    PageCacheSettings page_cache_settings)
{
    memory_cache = MemoryCacheStage{
        .cache = std::move(cache),
        .cache_path_prefix = {},
        .page_cache_settings = std::move(page_cache_settings),
        .custom_cache_path = std::move(custom_cache_path),
        .custom_file_version = std::move(custom_file_version)};
}

void ReadPipeline::needDistributedCache()
{
    distributed_cache = true;
}

void ReadPipeline::needAsyncPrefetch(
    IAsynchronousReader & reader,
    AsyncReadCountersPtr async_read_counters,
    FilesystemReadPrefetchesLogPtr prefetches_log)
{
    async_prefetch = AsyncPrefetchStage{
        .reader = &reader,
        .async_read_counters = std::move(async_read_counters),
        .prefetches_log = std::move(prefetches_log)};
}

void ReadPipeline::needDecryption(String path, size_t buffer_size, KeyFinderFunc key_finder)
{
    decryption_stages.push_back(DecryptionStage{.path = std::move(path), .buffer_size = buffer_size, .key_finder = std::move(key_finder)});
}

void ReadPipeline::needDecompression(bool allow_different_codecs)
{
    decompression = DecompressionStage{.allow_different_codecs = allow_different_codecs};
}

std::unique_ptr<ReadBufferFromFileBase> ReadPipeline::build() const
{
    if (!source)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ReadPipeline: source stage is not set, call setSource first");

    if (!read_settings)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ReadPipeline: read settings are not set, call setReadSettings first");

    const auto & settings = *read_settings;

    if (source->objects.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ReadPipeline: source has no stored objects");

    if (decompression)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ReadPipeline: decompression stage is not yet implemented");

    std::unique_ptr<ReadBufferFromFileBase> impl;

    if (gather)
    {
        /// -- Stages 1+2+3: Source + DiskCache + Gather --
        /// Object storage path: wrap per-object buffers with optional disk cache,
        /// then join all objects via ReadBufferFromRemoteFSGather.

        auto * obj_source = std::get_if<ObjectStorageSource>(&source->source);
        auto * custom_source = std::get_if<CustomSource>(&source->source);
        if (!obj_source && !custom_source)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ReadPipeline: gather requires ObjectStorageSource or CustomSource");

        ReadBufferFromRemoteFSGather::ReadBufferCreator gather_creator;

        if (disk_cache && disk_cache->cache)
        {
            auto fs_cache_settings = disk_cache->cache_settings.value_or(settings.getFilesystemCacheSettings());

            if (obj_source)
            {
                gather_creator =
                    [storage = obj_source->storage,
                     read_hint = obj_source->read_hint,
                     captured_settings = settings,
                     fs_cache_settings,
                     cache = disk_cache->cache,
                     cache_log = disk_cache->cache_log,
                     custom_key = disk_cache->custom_cache_key,
                     custom_origin = disk_cache->custom_origin](
                        bool restricted_seek, const StoredObject & object) mutable
                        -> std::unique_ptr<ReadBufferFromFileBase>
                {
                    auto cache_key = custom_key.value_or(FileCacheKey::fromPath(object.remote_path));
                    auto origin = custom_origin.value_or(cache->getCommonOriginWithSegmentKeyType(object.local_path));

                    auto impl_creator = [storage, read_hint, captured_settings, restricted_seek, object]() mutable
                        -> std::unique_ptr<ReadBufferFromFileBase>
                    {
                        return storage->readObject(object, captured_settings, read_hint, /* use_external_buffer */ true, restricted_seek);
                    };

                    return std::make_unique<CachedOnDiskReadBufferFromFile>(
                        object.remote_path,
                        cache_key,
                        cache,
                        origin,
                        std::move(impl_creator),
                        fs_cache_settings,
                        captured_settings.remote_fs_buffer_size,
                        captured_settings.local_fs_buffer_size,
                        std::string(CurrentThread::getQueryId()),
                        object.bytes_size,
                        /* allow_seeks_after_first_read */ !restricted_seek,
                        /* use_external_buffer */ true,
                        /* read_until_position */ std::nullopt,
                        cache_log);
                };
            }
            else
            {
                gather_creator =
                    [pipeline_creator = custom_source->creator,
                     captured_settings = settings,
                     fs_cache_settings,
                     cache = disk_cache->cache,
                     cache_log = disk_cache->cache_log,
                     custom_key = disk_cache->custom_cache_key,
                     custom_origin = disk_cache->custom_origin](
                        bool restricted_seek, const StoredObject & object) mutable
                        -> std::unique_ptr<ReadBufferFromFileBase>
                {
                    auto cache_key = custom_key.value_or(FileCacheKey::fromPath(object.remote_path));
                    auto origin = custom_origin.value_or(cache->getCommonOriginWithSegmentKeyType(object.local_path));

                    auto impl_creator = [pipeline_creator, captured_settings, restricted_seek, object]() mutable
                        -> std::unique_ptr<ReadBufferFromFileBase>
                    {
                        return pipeline_creator(object, captured_settings, /* use_external_buffer */ true, restricted_seek);
                    };

                    return std::make_unique<CachedOnDiskReadBufferFromFile>(
                        object.remote_path,
                        cache_key,
                        cache,
                        origin,
                        std::move(impl_creator),
                        fs_cache_settings,
                        captured_settings.remote_fs_buffer_size,
                        captured_settings.local_fs_buffer_size,
                        std::string(CurrentThread::getQueryId()),
                        object.bytes_size,
                        /* allow_seeks_after_first_read */ !restricted_seek,
                        /* use_external_buffer */ true,
                        /* read_until_position */ std::nullopt,
                        cache_log);
                };
            }
        }
        else
        {
            if (obj_source)
            {
                gather_creator =
                    [storage = obj_source->storage, read_hint = obj_source->read_hint,
                     captured_settings = settings](
                        bool restricted_seek, const StoredObject & object) mutable
                        -> std::unique_ptr<ReadBufferFromFileBase>
                {
                    return storage->readObject(object, captured_settings, read_hint, /* use_external_buffer */ true, restricted_seek);
                };
            }
            else
            {
                gather_creator =
                    [pipeline_creator = custom_source->creator, captured_settings = settings](
                        bool restricted_seek, const StoredObject & object) mutable
                        -> std::unique_ptr<ReadBufferFromFileBase>
                {
                    return pipeline_creator(object, captured_settings, /* use_external_buffer */ true, restricted_seek);
                };
            }
        }

        bool use_external_buffer = memory_cache.has_value() || async_prefetch.has_value() || distributed_cache;

        size_t total_objects_size = getTotalSize(source->objects);
        size_t effective_buffer_size = settings.remote_fs_buffer_size;
        size_t buffer_size = use_external_buffer ? 0 : effective_buffer_size;
        if (!use_external_buffer && total_objects_size > 0)
            buffer_size = std::min(buffer_size, total_objects_size);

        impl = std::make_unique<ReadBufferFromRemoteFSGather>(
            std::move(gather_creator),
            source->objects,
            settings,
            use_external_buffer,
            buffer_size);

        /// -- Stage 3.5: Distributed cache --
        /// TODO: when ENABLE_DISTRIBUTED_CACHE, wrap impl with ReadBufferFromDistributedCache here.
        /// Also adjust use_page_cache condition (use_page_cache_with_distributed_cache)
        /// and min_bytes_for_seek in AsyncPrefetch (distributed_cache_settings.min_bytes_for_seek).
    }
    else
    {
        /// -- Stage 1 only: Source (no gather) --
        /// Local disk path: create the buffer directly, no gather wrapping.

        if (source->objects.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ReadPipeline without gather requires exactly 1 stored object, got {}",
                source->objects.size());

        impl = std::visit(overloaded{
            [&](const ObjectStorageSource & s) -> std::unique_ptr<ReadBufferFromFileBase>
            {
                return s.storage->readObject(source->objects[0], settings, s.read_hint, /* use_external_buffer */ false, /* restrict_seek */ false);
            },
            [&](const LocalFileSource & s) -> std::unique_ptr<ReadBufferFromFileBase>
            {
                return createReadBufferFromFileBase(
                    s.path, settings, s.read_hint);
            },
            [&](const BackupSource & s) -> std::unique_ptr<ReadBufferFromFileBase>
            {
                return s.backup->readFile(s.path);
            },
            [&](const CustomSource & s) -> std::unique_ptr<ReadBufferFromFileBase>
            {
                return s.creator(source->objects[0], settings, /* use_external_buffer */ false, /* restrict_seek */ false);
            }
        }, source->source);
    }

    /// -- Stage 4: Memory cache --

    if (memory_cache && memory_cache->cache)
    {
        PageCacheKey cache_key;
        if (memory_cache->custom_cache_path)
        {
            cache_key.path = *memory_cache->custom_cache_path;
            if (memory_cache->custom_file_version)
                cache_key.file_version = *memory_cache->custom_file_version;
        }
        else
        {
            const auto & first_object = source->objects.at(0);
            cache_key.path = memory_cache->cache_path_prefix + first_object.remote_path;
        }

        auto page_cache_settings = memory_cache->page_cache_settings.value_or(settings.getPageCacheSettings());
        impl = std::make_unique<CachedInMemoryReadBufferFromFile>(
            cache_key, memory_cache->cache, std::move(impl), page_cache_settings);
    }

    /// -- Stage 5: Async prefetch --

    if (async_prefetch && async_prefetch->reader)
    {
        size_t total_size = getTotalSize(source->objects);
        size_t async_buffer_size = settings.remote_fs_buffer_size;
        if (total_size > 0)
            async_buffer_size = std::min(async_buffer_size, total_size);

        impl = std::make_unique<AsynchronousBoundedReadBuffer>(
            std::move(impl),
            *async_prefetch->reader,
            settings,
            async_buffer_size,
            settings.remote_read_min_bytes_for_seek,
            async_prefetch->async_read_counters,
            async_prefetch->prefetches_log);
    }

    /// -- Stage 6: Decryption (may have multiple layers for double encryption) --

#if USE_SSL
    for (const auto & dec : decryption_stages)
    {
        if (!dec.key_finder)
            continue;

        if (impl->eof())
        {
            /// Empty encrypted file — no header, return empty buffer.
            return std::make_unique<ReadBufferFromFileDecorator>(
                std::make_unique<ReadBufferFromString>(std::string_view{}), dec.path);
        }

        FileEncryption::Header header;
        header.read(*impl);
        String key = dec.key_finder(header.key_fingerprint, dec.path);

        impl = std::make_unique<ReadBufferFromEncryptedFile>(
            dec.path,
            dec.buffer_size,
            std::move(impl),
            key,
            header);
    }
#endif

    /// -- Stage 7: Decompression (not yet implemented) --

    return impl;
}

String ReadPipeline::describe() const
{
    String result;
    auto append = [&](const char * name)
    {
        if (!result.empty())
            result += " -> ";
        result += name;
    };

    if (source)
    {
        std::visit(overloaded{
            [&](const ObjectStorageSource &) { append("Source(ObjectStorage)"); },
            [&](const LocalFileSource &) { append("Source(LocalFile)"); },
            [&](const BackupSource &) { append("Source(Backup)"); },
            [&](const CustomSource &) { append("Source(Custom)"); }
        }, source->source);
    }
    if (disk_cache)
        append("DiskCache");
    if (gather)
        append("Gather");
    /// DistributedCache stage is not yet implemented in build().
    /// Only show it in describe() when the implementation is wired up.
    if (memory_cache)
        append("MemoryCache");
    if (async_prefetch)
        append("AsyncPrefetch");
    if (!decryption_stages.empty())
        append("Decrypt");
    if (decompression)
        append("Decompress");

    return result.empty() ? "(empty)" : result;
}

ReadPipeline ReadPipeline::clone() const
{
    return *this;
}

const StoredObjects & ReadPipeline::getStoredObjects() const
{
    if (!source)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ReadPipeline: source stage is not set");
    return source->objects;
}

}
