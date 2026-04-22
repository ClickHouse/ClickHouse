#include <IO/ReadPipeline.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
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


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

void ReadPipeline::setSource(ObjectStoragePtr object_storage, StoredObjects objects, std::optional<size_t> read_hint)
{
    auto creator = [storage = std::move(object_storage), read_hint](
        const StoredObject & object, const ReadSettings & settings) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return storage->readObject(object, settings, read_hint);
    };
    source = SourceStage{.objects = std::move(objects), .creator = std::move(creator)};
}

void ReadPipeline::setSource(StoredObjects objects, BufferCreator creator)
{
    source = SourceStage{.objects = std::move(objects), .creator = std::move(creator)};
}

void ReadPipeline::needGather()
{
    gather = true;
}

void ReadPipeline::needDiskCache(FileCachePtr cache, std::shared_ptr<FilesystemCacheLog> cache_log)
{
    disk_cache = DiskCacheStage{.cache = std::move(cache), .cache_log = std::move(cache_log)};
}

void ReadPipeline::needMemoryCache(std::shared_ptr<PageCache> cache, String cache_path_prefix)
{
    memory_cache = MemoryCacheStage{.cache = std::move(cache), .cache_path_prefix = std::move(cache_path_prefix)};
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

std::unique_ptr<ReadBufferFromFileBase> ReadPipeline::build(const ReadSettings & settings) const
{
    if (!source)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ReadPipeline: source stage is not set, call setSource first");

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

        auto nested_settings = settings.withNestedBuffer();
        ReadBufferFromRemoteFSGather::ReadBufferCreator gather_creator;

        if (disk_cache && disk_cache->cache)
        {
            auto cache_settings = nested_settings.withNestedBuffer();

            gather_creator =
                [pipeline_creator = source->creator,
                 cache_settings,
                 cache = disk_cache->cache,
                 cache_log = disk_cache->cache_log](
                    bool restricted_seek, const StoredObject & object) mutable
                    -> std::unique_ptr<ReadBufferFromFileBase>
            {
                auto cache_key = FileCacheKey::fromPath(object.remote_path);
                auto origin = cache->getCommonOriginWithSegmentKeyType(object.local_path);

                auto source_settings = cache_settings;
                source_settings.remote_read_buffer_restrict_seek = restricted_seek;

                auto impl_creator = [pipeline_creator, source_settings, object]() mutable
                    -> std::unique_ptr<ReadBufferFromFileBase>
                {
                    return pipeline_creator(object, source_settings);
                };

                return std::make_unique<CachedOnDiskReadBufferFromFile>(
                    object.remote_path,
                    cache_key,
                    cache,
                    origin,
                    std::move(impl_creator),
                    cache_settings,
                    std::string(CurrentThread::getQueryId()),
                    object.bytes_size,
                    /* allow_seeks_after_first_read */ !restricted_seek,
                    /* use_external_buffer */ cache_settings.remote_read_buffer_use_external_buffer,
                    /* read_until_position */ std::nullopt,
                    cache_log);
            };
        }
        else
        {
            gather_creator =
                [pipeline_creator = source->creator, nested_settings](
                    bool restricted_seek, const StoredObject & object) mutable
                    -> std::unique_ptr<ReadBufferFromFileBase>
            {
                nested_settings.remote_read_buffer_restrict_seek = restricted_seek;
                return pipeline_creator(object, nested_settings);
            };
        }

        bool use_external_buffer = memory_cache.has_value() || async_prefetch.has_value() || distributed_cache;

        size_t total_objects_size = getTotalSize(source->objects);
        size_t buffer_size = use_external_buffer ? 0 : settings.remote_fs_buffer_size;
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

        impl = source->creator(source->objects[0], settings);
    }

    /// -- Stage 4: Memory cache --

    if (memory_cache && memory_cache->cache)
    {
        const auto & first_object = source->objects.at(0);
        PageCacheKey cache_key{.path = memory_cache->cache_path_prefix + first_object.remote_path};

        impl = std::make_unique<CachedInMemoryReadBufferFromFile>(
            cache_key, memory_cache->cache, std::move(impl), settings);
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
        append("Source");
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
