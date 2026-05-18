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

#if ENABLE_DISTRIBUTED_CACHE
#include <DistributedCache/Utils.h>
#endif

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
}

void ReadPipeline::setSource(ObjectStoragePtr object_storage, StoredObjects objects, const ReadSettings & read_settings, std::optional<size_t> read_hint)
{
    source = SourceStage{
        .objects = std::move(objects),
        .source = ObjectStorageSource{.storage = std::move(object_storage), .read_hint = read_hint},
        .read_settings = read_settings};
}

void ReadPipeline::setLocalFileSource(String path, StoredObjects objects, const ReadSettings & read_settings, std::optional<size_t> read_hint)
{
    source = SourceStage{
        .objects = std::move(objects),
        .source = LocalFileSource{.path = std::move(path), .read_hint = read_hint},
        .read_settings = read_settings};
}

void ReadPipeline::setBackupSource(std::shared_ptr<IBackup> backup, String path, StoredObjects objects, const ReadSettings & read_settings)
{
    source = SourceStage{
        .objects = std::move(objects),
        .source = BackupSource{.backup = std::move(backup), .path = std::move(path)},
        .read_settings = read_settings};
}

void ReadPipeline::setSource(BufferCreator creator, StoredObjects objects, const ReadSettings & read_settings)
{
    source = SourceStage{
        .objects = std::move(objects),
        .source = CustomSource{.creator = std::move(creator)},
        .read_settings = read_settings};
}

void ReadPipeline::needGather()
{
    gather = true;
}

void ReadPipeline::needDiskCache(FileCachePtr cache, FilesystemCacheSettings cache_settings, std::shared_ptr<FilesystemCacheLog> cache_log)
{
    disk_caches.push_back(DiskCacheStage{
        .cache = std::move(cache),
        .cache_log = std::move(cache_log),
        .cache_settings = std::move(cache_settings),
        .custom_cache_key = std::nullopt,
        .custom_origin = std::nullopt});
}

void ReadPipeline::needDiskCache(
    FileCachePtr cache,
    FileCacheKey cache_key,
    FileCacheOriginInfo origin,
    FilesystemCacheSettings cache_settings,
    std::shared_ptr<FilesystemCacheLog> cache_log)
{
    disk_caches.push_back(DiskCacheStage{
        .cache = std::move(cache),
        .cache_log = std::move(cache_log),
        .cache_settings = std::move(cache_settings),
        .custom_cache_key = std::move(cache_key),
        .custom_origin = std::move(origin)});
}

void ReadPipeline::needMemoryCache(std::shared_ptr<PageCache> cache, String cache_path_prefix, PageCacheSettings page_cache_settings)
{
    memory_cache = MemoryCacheStage{
        .cache = std::move(cache),
        .cache_path_prefix = std::move(cache_path_prefix),
        .page_cache_settings = std::move(page_cache_settings),
        .custom_cache_path = {},
        .custom_file_version = {}};
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

void ReadPipeline::needDistributedCache(bool include_credentials_in_cache_key)
{
    distributed_cache = DistributedCacheStage{.include_credentials_in_cache_key = include_credentials_in_cache_key};
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
    decryption_stages.push_back(DecryptionStage{
        .path = std::move(path),
        .buffer_size = buffer_size,
        .key_finder = std::move(key_finder)});
}

std::unique_ptr<ReadBufferFromFileBase> ReadPipeline::build() const
{
    if (!source)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ReadPipeline: source stage is not set, call setSource first");

    const auto & settings = source->read_settings;

    if (source->objects.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ReadPipeline: source has no stored objects");

    std::unique_ptr<ReadBufferFromFileBase> impl;

    if (gather)
    {
        /// -- Stages 1+2+3: Source + DiskCache + Gather --
        /// Object storage path: wrap per-object buffers with optional disk cache,
        /// then join all objects via ReadBufferFromRemoteFSGather.

        const auto * obj_source = std::get_if<ObjectStorageSource>(&source->source);
        const auto * custom_source = std::get_if<CustomSource>(&source->source);
        if (!obj_source && !custom_source)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ReadPipeline: gather requires ObjectStorageSource or CustomSource");

        /// Step 1: Build base gather_creator that reads from the source.
        ReadBufferFromRemoteFSGather::ReadBufferCreator gather_creator;

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

        /// Step 2: Wrap each disk cache layer around the source (innermost first).
        /// With stacked CachedObjectStorage (cache-on-cache), each layer calls needDiskCache
        /// in inner-to-outer order. The resulting per-object chain is:
        ///   OuterCache(InnerCache(Source))
        for (const auto & dc : disk_caches)
        {
            auto fs_cache_settings = dc.cache_settings.value_or(settings.getFilesystemCacheSettings());
            gather_creator =
                [prev_creator = std::move(gather_creator),
                 captured_settings = settings,
                 fs_cache_settings,
                 cache = dc.cache,
                 cache_log = dc.cache_log,
                 custom_key = dc.custom_cache_key,
                 custom_origin = dc.custom_origin](
                    bool restricted_seek, const StoredObject & object) mutable
                    -> std::unique_ptr<ReadBufferFromFileBase>
            {
                auto cache_key = custom_key.value_or(FileCacheKey::fromPath(object.remote_path));
                auto origin = custom_origin.value_or(cache->getCommonOriginWithSegmentKeyType(object.local_path));

                /// Copy, not move: gather_creator may be called multiple times (once per object).
                auto impl_creator = [prev_copy = prev_creator, restricted_seek, object]() mutable
                    -> std::unique_ptr<ReadBufferFromFileBase>
                {
                    return prev_copy(restricted_seek, object);
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
                    cache_log,
                    captured_settings.local_throttler);
            };
        }

        /// use_external_buffer is true only when a downstream stage (memory cache, async prefetch)
        /// manages the working buffer. Distributed cache does NOT require it — on master, DC
        /// always implied async prefetch (use_async_buffer = use_prefetch || use_distributed_cache),
        /// but in ReadPipeline these are independent stages. DC reads from TCP and manages its own buffer.
        bool use_external_buffer = memory_cache.has_value() || async_prefetch.has_value();

        size_t total_objects_size = getTotalSize(source->objects);
        size_t effective_buffer_size = settings.remote_fs_buffer_size;
        size_t buffer_size = use_external_buffer ? 0 : effective_buffer_size;
        if (!use_external_buffer && total_objects_size > 0)
            buffer_size = std::min(buffer_size, total_objects_size);

        /// -- Stage 3.5: Distributed cache --
        /// When enabled, reads go through distributed cache servers with fallback to
        /// direct object storage reads via a Gather reader.
#if ENABLE_DISTRIBUTED_CACHE
        if (distributed_cache)
        {
            const auto * dc_obj_source = std::get_if<ObjectStorageSource>(&source->source);
            if (!dc_obj_source)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ReadPipeline: distributed cache requires ObjectStorageSource");

            /// The fallback must be a full Gather reader — same chain as the non-DC path.
            /// `ReadBufferFromDistributedCache::readFromFallbackBuffer` calls
            /// `buffer->set(internal_buffer.begin(), internal_buffer.size())` on the fallback,
            /// so the fallback must accept an external buffer (use_external_buffer=true).
            /// Copy, not move: fallback may be called multiple times (e.g. after
            /// connection pool exhaustion on different read ranges).
            auto fallback_creator = [gather_creator, objects = source->objects,
                                     captured_settings = settings]() mutable
                -> std::unique_ptr<ReadBufferFromFileBase>
            {
                auto creator_copy = gather_creator;
                return std::make_unique<ReadBufferFromRemoteFSGather>(
                    std::move(creator_copy),
                    objects,
                    captured_settings.remote_read_min_bytes_for_seek,
                    /* use_external_buffer */ true,
                    /* buffer_size */ 0);
            };

            impl = DistributedCache::readWithDistributedCache(
                source->objects.at(0).remote_path,
                source->objects,
                settings,
                *dc_obj_source->storage,
                use_external_buffer,
                std::move(fallback_creator),
                distributed_cache->include_credentials_in_cache_key);
        }
        else
#endif
        {
            impl = std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(gather_creator),
                source->objects,
                settings.remote_read_min_bytes_for_seek,
                use_external_buffer,
                buffer_size);
        }
    }
    else
    {
        /// -- Stages 1+2 without gather --
        /// Single-object path (e.g. StorageObjectStorageSource, local disk).
        /// No gather wrapping — preserves readBigAt support for the parquet prefetcher.

        if (source->objects.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ReadPipeline without gather requires exactly 1 stored object, got {}",
                source->objects.size());

        const auto & object = source->objects[0];

        {
            /// -- Stages 1+2: Source + DiskCache(s) (single-object, no gather) --
            /// use_external_buffer for the outermost buffer: true when a downstream
            /// stage (memory cache, async prefetch) manages the working buffer.
            /// Inner cache layers always use external buffer (the outer cache calls set()).
            bool use_ext_buf = memory_cache.has_value() || async_prefetch.has_value();

            if (!disk_caches.empty())
            {
                /// The impl buffer (source reader inside the cache) must always use external buffer mode.
                /// CachedOnDiskReadBufferFromFile couples with its impl via set() — passing the working
                /// buffer for each read and for predownload.
                static constexpr bool impl_use_external_buffer = true;

                /// Build innermost impl_creator (source reader).
                CachedOnDiskReadBufferFromFile::ImplementationBufferCreator impl_creator;

                if (const auto * obj_src = std::get_if<ObjectStorageSource>(&source->source))
                {
                    impl_creator = [storage = obj_src->storage, read_hint = obj_src->read_hint,
                                    captured_object = object, captured_settings = settings]()
                        -> std::unique_ptr<ReadBufferFromFileBase>
                    {
                        return storage->readObject(captured_object, captured_settings, read_hint,
                            impl_use_external_buffer, /* restrict_seek */ false);
                    };
                }
                else if (const auto * cust_src = std::get_if<CustomSource>(&source->source))
                {
                    impl_creator = [creator = cust_src->creator, captured_object = object, captured_settings = settings]()
                        -> std::unique_ptr<ReadBufferFromFileBase>
                    {
                        return creator(captured_object, captured_settings, impl_use_external_buffer, /* restrict_seek */ false);
                    };
                }
                else
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "ReadPipeline: disk cache without gather requires ObjectStorageSource or CustomSource");
                }

                /// Wrap inner cache layers (all except the outermost). Each layer's
                /// impl_creator produces the previous layer's CachedOnDiskReadBufferFromFile.
                /// Inner layers always use use_external_buffer=true.
                for (size_t i = 0; i + 1 < disk_caches.size(); ++i)
                {
                    const auto & dc = disk_caches[i];
                    auto fs_cache_settings = dc.cache_settings.value_or(settings.getFilesystemCacheSettings());
                    auto cache_key = dc.custom_cache_key.value_or(FileCacheKey::fromPath(object.remote_path));
                    auto origin = dc.custom_origin.value_or(dc.cache->getCommonOriginWithSegmentKeyType(object.local_path));

                    impl_creator = [
                        prev_creator = std::move(impl_creator),
                        path = object.remote_path,
                        cache_key, cache = dc.cache, origin,
                        fs_cache_settings,
                        remote_buf_size = settings.remote_fs_buffer_size,
                        local_buf_size = settings.local_fs_buffer_size,
                        object_size = object.bytes_size,
                        cache_log = dc.cache_log,
                        throttler = settings.local_throttler
                    ]() mutable -> std::unique_ptr<ReadBufferFromFileBase>
                    {
                        /// Copy, not move: impl_creator may be called multiple times
                        /// (tryGetFileSize, read, re-read after seek/reset).
                        auto prev_copy = prev_creator;
                        return std::make_unique<CachedOnDiskReadBufferFromFile>(
                            path, cache_key, cache, origin,
                            std::move(prev_copy),
                            fs_cache_settings,
                            remote_buf_size, local_buf_size,
                            std::string(CurrentThread::getQueryId()),
                            object_size,
                            /* allow_seeks_after_first_read */ true,
                            /* use_external_buffer */ true,
                            /* read_until_position */ std::nullopt,
                            cache_log, throttler);
                    };
                }

                /// Build the outermost CachedOnDiskReadBufferFromFile.
                /// The impl_creator now produces the full inner cache chain.
                const auto & outermost = disk_caches.back();
                auto fs_cache_settings = outermost.cache_settings.value_or(settings.getFilesystemCacheSettings());
                auto cache_key = outermost.custom_cache_key.value_or(FileCacheKey::fromPath(object.remote_path));
                auto origin = outermost.custom_origin.value_or(outermost.cache->getCommonOriginWithSegmentKeyType(object.local_path));

                impl = std::make_unique<CachedOnDiskReadBufferFromFile>(
                    object.remote_path,
                    cache_key,
                    outermost.cache,
                    origin,
                    std::move(impl_creator),
                    fs_cache_settings,
                    settings.remote_fs_buffer_size,
                    settings.local_fs_buffer_size,
                    std::string(CurrentThread::getQueryId()),
                    object.bytes_size,
                    /* allow_seeks_after_first_read */ true,
                    use_ext_buf,
                    /* read_until_position */ std::nullopt,
                    outermost.cache_log,
                    settings.local_throttler);
            }
            else
            {
                /// -- Stage 1 only: Source (no cache, no gather) --
                impl = std::visit(overloaded{
                    [&](const ObjectStorageSource & s) -> std::unique_ptr<ReadBufferFromFileBase>
                    {
                        return s.storage->readObject(object, settings, s.read_hint, use_ext_buf, /* restrict_seek */ false);
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
                        return s.creator(object, settings, use_ext_buf, /* restrict_seek */ false);
                    }
                }, source->source);
            }
        }

        /// -- Stage 2.5 (non-gather): Distributed cache --
#if ENABLE_DISTRIBUTED_CACHE
        if (distributed_cache)
        {
            const auto * dc_obj_source = std::get_if<ObjectStorageSource>(&source->source);
            if (!dc_obj_source)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ReadPipeline: distributed cache requires ObjectStorageSource");

            if (!disk_caches.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ReadPipeline: disk cache + distributed cache without gather is not supported. "
                    "Use needGather() to enable the gather path which handles both stages");

            /// Fallback: read directly from object storage (same as non-DC path).
            /// `ReadBufferFromDistributedCache::readFromFallbackBuffer` calls
            /// `buffer->set(internal_buffer.begin(), internal_buffer.size())` on the fallback,
            /// so the fallback must accept an external buffer (use_external_buffer=true).
            auto fallback_creator = [storage = dc_obj_source->storage,
                                     read_hint = dc_obj_source->read_hint,
                                     captured_object = source->objects.at(0),
                                     captured_settings = settings]()
                -> std::unique_ptr<ReadBufferFromFileBase>
            {
                return storage->readObject(captured_object, captured_settings, read_hint,
                    /* use_external_buffer */ true, /* restrict_seek */ false);
            };

            /// DC manages its own buffer (reads from TCP). Same as the gather path.
            bool use_ext_buf_for_dc = false;
            impl = DistributedCache::readWithDistributedCache(
                source->objects.at(0).remote_path,
                source->objects,
                settings,
                *dc_obj_source->storage,
                use_ext_buf_for_dc,
                std::move(fallback_creator),
                distributed_cache->include_credentials_in_cache_key);
        }
#endif
    }

    /// -- Stage 4: Memory cache --

    if (memory_cache && memory_cache->cache)
    {
        PageCacheFile cache_file;
        if (memory_cache->custom_cache_path)
        {
            cache_file.path = *memory_cache->custom_cache_path;
            if (memory_cache->custom_file_version)
                cache_file.file_version = *memory_cache->custom_file_version;
        }
        else
        {
            const auto & first_object = source->objects.at(0);
            cache_file.path = memory_cache->cache_path_prefix + first_object.remote_path;
        }

        /// Apply stage-level page cache settings when provided, falling back to source settings.
        auto page_cache_read_settings = settings;
        if (memory_cache->page_cache_settings)
        {
            const auto & pcs = *memory_cache->page_cache_settings;
            page_cache_read_settings.read_from_page_cache_if_exists_otherwise_bypass_cache = pcs.read_from_page_cache_if_exists_otherwise_bypass_cache;
            page_cache_read_settings.page_cache_inject_eviction = pcs.page_cache_inject_eviction;
            page_cache_read_settings.page_cache_block_size = pcs.page_cache_block_size;
            page_cache_read_settings.page_cache_lookahead_blocks = pcs.page_cache_lookahead_blocks;
            page_cache_read_settings.page_cache_max_coalesced_bytes = pcs.page_cache_max_coalesced_bytes;
        }

        impl = std::make_unique<CachedInMemoryReadBufferFromFile>(
            cache_file, memory_cache->cache, std::move(impl), page_cache_read_settings);
    }

    /// -- Stage 5: Async prefetch --

    if (async_prefetch && async_prefetch->reader)
    {
        size_t total_size = getTotalSize(source->objects);
        size_t async_buffer_size = settings.remote_fs_buffer_size;
        if (total_size > 0)
            async_buffer_size = std::min(async_buffer_size, total_size);

        /// When distributed cache is active, use its min_bytes_for_seek
        /// (typically larger, since seeks within the cache are cheaper).
        size_t min_bytes_for_seek = distributed_cache
            ? settings.distributed_cache_settings.min_bytes_for_seek
            : settings.remote_read_min_bytes_for_seek;

        impl = std::make_unique<AsynchronousBoundedReadBuffer>(
            std::move(impl),
            *async_prefetch->reader,
            async_buffer_size,
            min_bytes_for_seek,
            settings.priority,
            settings.page_cache_block_size,
            settings.enable_filesystem_read_prefetches_log,
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
    for (size_t i = 0; i < disk_caches.size(); ++i)
        append("DiskCache");
    if (gather)
        append("Gather");
    if (distributed_cache)
        append("DistributedCache");
    if (memory_cache)
        append("MemoryCache");
    if (async_prefetch)
        append("AsyncPrefetch");
    if (!decryption_stages.empty())
        append("Decrypt");

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
