#include <IO/ReadPipeline.h>

#include <IO/BufferSourceReader.h>
#include <IO/LocalSourceReader.h>
#include <IO/ObjectStorageSourceReader.h>
#include <IO/PageCacheProvider.h>
#include <IO/DiskCacheProvider.h>
#include <IO/PipelineReadBuffer.h>
#include <IO/ReaderExecutor.h>
#include <Interpreters/Context.h>
#include <Interpreters/ReaderExecutorLog.h>

#include <Backups/IBackup.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/CachedInMemoryReadBufferFromFile.h>
#include <IO/ReadBufferFromEmptyFile.h>
#include <IO/ReadBufferFromEncryptedFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/ReadBufferFromString.h>
#include <IO/FileEncryptionCommon.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheKey.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>

#if ENABLE_DISTRIBUTED_CACHE
#include <DistributedCache/Utils.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /// Helper for std::visit with multiple lambdas.
    template <class... Ts>
    struct Overloaded : Ts...
    {
        using Ts::operator()...;
    };
    template <class... Ts>
    Overloaded(Ts...) -> Overloaded<Ts...>;
}

void ReadPipeline::setSource(ObjectStoragePtr object_storage, StoredObjects objects, const ReadSettings & read_settings, std::optional<size_t> read_hint)
{
    chassert(!source.has_value(), "ReadPipeline: source is already set");
    source = SourceStage{
        .objects = std::move(objects),
        .source = ObjectStorageSource{.storage = std::move(object_storage), .read_hint = read_hint},
        .read_settings = read_settings};
}

void ReadPipeline::setLocalFileSource(String path, StoredObjects objects, const ReadSettings & read_settings, std::optional<size_t> read_hint)
{
    chassert(!source.has_value(), "ReadPipeline: source is already set");
    source = SourceStage{
        .objects = std::move(objects),
        .source = LocalFileSource{.path = std::move(path), .read_hint = read_hint},
        .read_settings = read_settings};
}

void ReadPipeline::setBackupSource(std::shared_ptr<IBackup> backup, String path, StoredObjects objects, const ReadSettings & read_settings)
{
    chassert(!source.has_value(), "ReadPipeline: source is already set");
    source = SourceStage{
        .objects = std::move(objects),
        .source = BackupSource{.backup = std::move(backup), .path = std::move(path)},
        .read_settings = read_settings};
}

void ReadPipeline::setSource(BufferCreator creator, StoredObjects objects, const ReadSettings & read_settings)
{
    chassert(!source.has_value(), "ReadPipeline: source is already set");
    source = SourceStage{
        .objects = std::move(objects),
        .source = CustomSource{.creator = std::move(creator)},
        .read_settings = read_settings};
}

void ReadPipeline::setAlreadyCompleteSource(BufferCreator creator, StoredObjects objects, const ReadSettings & read_settings)
{
    chassert(!source.has_value(), "ReadPipeline: source is already set");
    source = SourceStage{
        .objects = std::move(objects),
        .source = CustomSource{.creator = std::move(creator)},
        .read_settings = read_settings,
        .already_complete = true};
}

void ReadPipeline::needGather()
{
    gather = true;
}

void ReadPipeline::needFilesystemCache(FileCachePtr cache, FilesystemCacheSettings cache_settings, std::shared_ptr<FilesystemCacheLog> cache_log)
{
    filesystem_caches.push_back(FilesystemCacheStage{
        .cache = std::move(cache),
        .cache_log = std::move(cache_log),
        .cache_settings = std::move(cache_settings),
        .custom_cache_key = std::nullopt,
        .custom_origin = std::nullopt});
}

void ReadPipeline::needFilesystemCache(
    FileCachePtr cache,
    FileCacheKey cache_key,
    FileCacheOriginInfo origin,
    FilesystemCacheSettings cache_settings,
    std::shared_ptr<FilesystemCacheLog> cache_log)
{
    filesystem_caches.push_back(FilesystemCacheStage{
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

void ReadPipeline::needPrefetchPool(std::shared_ptr<PrefetchThreadPool> pool)
{
    prefetch_pool = std::move(pool);
}

void ReadPipeline::needBufferLimit(std::shared_ptr<SourceBufferLimit> limit)
{
    buffer_limit = std::move(limit);
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

    /// Empty objects = zero-blob file.
    if (source->objects.empty())
        return std::make_unique<ReadBufferFromEmptyFile>();

    /// `setAlreadyCompleteSource` registers a creator whose returned buffer
    /// is itself a complete reader — typically a packed-archive file view that
    /// internally wraps its own `disk->readFile` (with caches, decryption,
    /// prefetch already applied to the underlying I/O). Wrapping again with the
    /// executor or any stage would be both pointless overhead and incorrect
    /// (e.g. trips `ReadBufferFromFileView`'s swap-state pattern when the
    /// executor uses external-buffer mode). Stages must NOT be configured on
    /// such a pipeline; the assertion below catches accidental misuse.
    if (source->already_complete)
    {
        chassert(!gather && !memory_cache && filesystem_caches.empty()
                 && !async_prefetch && decryption_stages.empty() && !distributed_cache
                 && !prefetch_pool && !buffer_limit,
                 "ReadPipeline: setAlreadyCompleteSource is incompatible with any stage");
        const auto & custom = std::get<CustomSource>(source->source);
        return custom.creator(
            source->objects.front(),
            source->read_settings,
            /*use_external_buffer=*/false,
            /*restrict_seek=*/false);
    }

    /// Capture the query id once here (on the calling thread, which has the
    /// query context). Subsequent cached-buffer creations happen lazily inside
    /// gather/impl creators that may run on threadpool workers without query
    /// context, so calling `CurrentThread::getQueryId()` there would return "".
    const std::string query_id(CurrentThread::getQueryId());

    /// Experimental ReaderExecutor path owns prefetch / memory-cache / decryption
    /// internally, so it must NOT be wrapped by `wrapAsyncPrefetch` /
    /// `wrapMemoryCache` / `wrapDecryption`. In particular, when an
    /// `AsynchronousBoundedReadBuffer` wraps us, `ThreadPoolRemoteFSReader::execute`
    /// asserts `reader.buffer().begin() == request.buf` after `set()+next()` — but
    /// `PipelineReadBuffer::nextImpl` exposes refcounted rope-node memory, not the
    /// caller's external buffer, so that invariant cannot hold. Returning early
    /// here bypasses the legacy wraps entirely.
    if (auto pipeline_buf = tryBuildReaderExecutor(query_id))
        return pipeline_buf;

    auto impl = gather
        ? buildGatherStage(query_id)        // Stages 1+2+3 (+3.5 DC)
        : buildSingleObjectStage(query_id); // Stages 1+2 (+2.5 DC)

    impl = wrapMemoryCache(std::move(impl));   // Stage 4
    impl = wrapAsyncPrefetch(std::move(impl)); // Stage 5
    impl = wrapDecryption(std::move(impl));    // Stage 6 (encryption)

    return impl;
}

std::unique_ptr<ReadBufferFromFileBase> ReadPipeline::tryBuildReaderExecutor(const std::string & query_id) const
{
    const auto & settings = source->read_settings;
    if (!settings.use_reader_executor)
        return nullptr;

    /// Build a source reader appropriate for the source variant. Falls back to
    /// nullptr (and the caller picks the legacy path) for source types we do not
    /// support yet.
    std::shared_ptr<ISourceReader> source_reader;
    size_t min_bytes_for_seek = ReaderExecutor::DEFAULT_MIN_BYTES_FOR_SEEK;

    if (const auto * local_src = std::get_if<LocalFileSource>(&source->source))
    {
        LOG_DEBUG(getLogger("ReadPipeline"), "build: using ReaderExecutor for local file, {} objects, path={}",
            source->objects.size(), local_src->path);
        source_reader = std::make_shared<LocalSourceReader>(settings);
        min_bytes_for_seek = 0; /// Local seeks are free.
    }
    else if (const auto * obj_src = std::get_if<ObjectStorageSource>(&source->source))
    {
        LOG_DEBUG(getLogger("ReadPipeline"), "build: using ReaderExecutor for object storage, {} objects, gather={}",
            source->objects.size(), gather);
        source_reader = std::make_shared<ObjectStorageSourceReader>(obj_src->storage, settings);
    }
    else if (const auto * backup_src = std::get_if<BackupSource>(&source->source))
    {
        LOG_DEBUG(getLogger("ReadPipeline"), "build: using ReaderExecutor for backup, path={}", backup_src->path);
        auto backup = backup_src->backup;
        auto backup_path = backup_src->path;
        source_reader = std::make_shared<BufferSourceReader>(
            [backup, backup_path](const StoredObject &) { return backup->readFile(backup_path); },
            "BackupSource");
    }
    else if (const auto * custom_src = std::get_if<CustomSource>(&source->source))
    {
        LOG_DEBUG(getLogger("ReadPipeline"), "build: using ReaderExecutor for custom source");
        auto creator = custom_src->creator;
        auto captured_settings = settings;
        source_reader = std::make_shared<BufferSourceReader>(
            [creator, captured_settings](const StoredObject & object)
            {
                /// External-buffer mode: ReaderExecutor drives reads via set()+next().
                return creator(object, captured_settings, /*use_external_buffer=*/true, /*restrict_seek=*/false);
            },
            "CustomSource");
    }

    if (!source_reader)
        return nullptr;

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> executor_caches;

    /// PageCache (memory) — goes first in chain (fastest). It's a
    /// file-level cache: derive a single `PageCacheFile` from the front
    /// object (or custom path/version) and let the provider use it for
    /// every lookup, regardless of which `StoredObject` is being accessed.
    ///
    /// Skipped when any object has unknown size. PageCache cells are sized
    /// to the file's actual byte length so the tail block has no past-EOF
    /// region; that calibration needs the total size up front. Master's
    /// `CachedInMemoryReadBufferFromFile` makes the same call by requiring
    /// `file_size.value()` everywhere.
    bool any_unknown_size = false;
    size_t total_file_size = 0;
    for (const auto & obj : source->objects)
    {
        if (obj.bytes_size == StoredObject::UnknownSize)
        {
            any_unknown_size = true;
            break;
        }
        total_file_size += obj.bytes_size;
    }

    if (memory_cache && memory_cache->cache && !any_unknown_size)
    {
        const auto & pcs = memory_cache->page_cache_settings;
        PageCacheFile cache_file;
        cache_file.path = memory_cache->custom_cache_path.value_or(
            memory_cache->cache_path_prefix + source->objects.front().remote_path);
        cache_file.file_version = memory_cache->custom_file_version.value_or("");
        executor_caches.push_back(std::make_shared<PageCacheProvider>(
            memory_cache->cache,
            std::move(cache_file),
            pcs.page_cache_block_size,
            pcs.page_cache_inject_eviction,
            pcs.read_from_page_cache_if_exists_otherwise_bypass_cache,
            total_file_size));
    }

    /// FileCache (disk) — goes second in chain. Pass the stage's
    /// `custom_cache_key` / `custom_origin` through so the provider can
    /// honour caller-specified cache identity (etag-keyed flow,
    /// `Data` / `System` origin classification) per-object.
    ///
    /// Iterate `filesystem_caches` in reverse: `CachedObjectStorage::prepareRead`
    /// pushes the wrapped storage's caches first and its own cache last, so
    /// the vector is stored inner-to-outer. The legacy builder
    /// (`buildSingleObjectStage`) wraps the source from inside out, which
    /// makes the outermost cache the FIRST one a read traverses — outer
    /// hit serves directly, miss falls through to inner, then source. The
    /// executor queries `caches[0]` first, so reversing here matches that
    /// outer-first query order. Single-cache pipelines (the common case)
    /// are unaffected — reversing a one-element range is a no-op.
    for (auto it = filesystem_caches.rbegin(); it != filesystem_caches.rend(); ++it)
    {
        const auto & dc = *it;
        if (dc.cache)
        {
            executor_caches.push_back(std::make_shared<DiskCacheProvider>(
                dc.cache, dc.cache_settings, query_id, dc.cache_log,
                dc.custom_cache_key, dc.custom_origin));
        }
    }

    String log_file_path = source->objects.empty() ? "" : source->objects.front().remote_path;

    auto executor = std::make_unique<ReaderExecutor>(
        source_reader,
        source->objects,
        std::move(executor_caches),
        ReaderExecutor::DEFAULT_WINDOW_SIZE,
        min_bytes_for_seek,
        std::move(log_file_path));

    if (prefetch_pool)
        executor->setPrefetchPool(prefetch_pool);

    if (buffer_limit)
        executor->setBufferLimit(buffer_limit);

    if (settings.enable_reader_executor_log)
    {
        if (auto global = Context::getGlobalContextInstance())
            executor->setReaderExecutorLog(global->getReaderExecutorLog());
    }

    for (const auto & dec : decryption_stages)
        executor->addDecryptionLayer(dec.path, dec.buffer_size, dec.key_finder);

    executor->initDecryption();

    return std::make_unique<PipelineReadBuffer>(std::move(executor));
}

std::unique_ptr<ReadBufferFromFileBase> ReadPipeline::buildGatherStage(const std::string & query_id) const
{
    /// -- Stages 1+2+3: Source + FilesystemCache + Gather --
    /// Object storage path: wrap per-object buffers with optional filesystem cache,
    /// then join all objects via ReadBufferFromRemoteFSGather.

    const auto & settings = source->read_settings;

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

    /// Step 2: Wrap each filesystem cache layer around the source (innermost first).
    /// With stacked CachedObjectStorage (cache-on-cache), each layer calls needFilesystemCache
    /// in inner-to-outer order. The resulting per-object chain is:
    ///   OuterCache(InnerCache(Source))
    for (const auto & dc : filesystem_caches)
    {
        auto fs_cache_settings = dc.cache_settings;
        gather_creator =
            [prev_creator = std::move(gather_creator),
             captured_settings = settings,
             fs_cache_settings,
             cache = dc.cache,
             cache_log = dc.cache_log,
             custom_key = dc.custom_cache_key,
             custom_origin = dc.custom_origin,
             query_id](
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
                query_id,
                object.bytes_size,
                /* allow_seeks_after_first_read */ !restricted_seek,
                /* use_external_buffer */ true,
                /* read_until_position */ std::nullopt,
                cache_log,
                captured_settings.local_throttler);
        };
    }

    /// use_external_buffer is true only when a downstream stage (memory cache, async prefetch)
    /// manages the working buffer. Distributed cache does NOT require it — it reads from TCP
    /// and manages its own buffer, so memory_cache/async_prefetch are the only stages that
    /// hand external memory to the inner reader via `set()`.
    bool use_external_buffer = memory_cache.has_value() || async_prefetch.has_value();

    size_t total_objects_size = getTotalSize(source->objects);
    size_t effective_buffer_size = settings.remote_fs_buffer_size;
    size_t buffer_size = use_external_buffer ? 0 : effective_buffer_size;
    if (!use_external_buffer && total_objects_size > 0 && total_objects_size != StoredObject::UnknownSize)
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

        auto impl = DistributedCache::readWithDistributedCache(
            source->objects.at(0).local_path,
            source->objects,
            settings,
            *dc_obj_source->storage,
            use_external_buffer,
            std::move(fallback_creator),
            distributed_cache->include_credentials_in_cache_key);
        chassert(impl, "readWithDistributedCache must return a valid buffer or throw");
        return impl;
    }
#endif

    return std::make_unique<ReadBufferFromRemoteFSGather>(
        std::move(gather_creator),
        source->objects,
        settings.remote_read_min_bytes_for_seek,
        use_external_buffer,
        buffer_size);
}

std::unique_ptr<ReadBufferFromFileBase> ReadPipeline::buildSingleObjectStage(const std::string & query_id) const
{
    /// -- Stages 1+2 (+2.5 DC) without gather --
    /// Single-object path (e.g. StorageObjectStorageSource, local disk).
    /// No gather wrapping — preserves readBigAt support for the parquet prefetcher.
    ///
    /// Three mutually exclusive sub-paths, each returns its own final impl:
    ///   1. distributed_cache  → DC owns the chain, source impl is never built.
    ///   2. filesystem_caches  → stacked CachedOnDiskReadBufferFromFile wrapping source.
    ///   3. neither            → source impl directly.

    if (source->objects.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ReadPipeline without gather requires exactly 1 stored object, got {}",
            source->objects.size());

    const auto & settings = source->read_settings;
    const auto & object = source->objects[0];

    /// use_external_buffer for the outermost buffer: true when a downstream
    /// stage (memory cache, async prefetch) manages the working buffer.
    /// Inner cache layers always use external buffer (the outer cache calls set()).
    bool use_ext_buf = memory_cache.has_value() || async_prefetch.has_value();

    /// -- Stage 2.5 (non-gather): Distributed cache --
    /// When DC is active it owns the whole chain and assigns `impl` outright.
    /// Filesystem cache + DC without gather is rejected: a filesystem-cache layer
    /// built above would be unreachable (DC would replace it). The gather path
    /// composes both naturally; callers that need both must call needGather().
#if ENABLE_DISTRIBUTED_CACHE
    if (distributed_cache)
    {
        const auto * dc_obj_source = std::get_if<ObjectStorageSource>(&source->source);
        if (!dc_obj_source)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ReadPipeline: distributed cache requires ObjectStorageSource");

        /// FIXME: lift this restriction. Filesystem cache should be reachable on the
        /// DC fallback path (DC miss → fallback → FS cache → source). Today the
        /// fallback creator below goes directly to `storage->readObject`, skipping
        /// any filesystem-cache layer; we'd need to feed the FS-cache-wrapped source
        /// into the fallback creator instead, mirroring how the gather path composes
        /// the two stages.
        if (!filesystem_caches.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ReadPipeline: filesystem cache + distributed cache without gather is not supported. "
                "Use needGather() to enable the gather path which handles both stages");

        /// Fallback: read directly from object storage (same as non-DC path).
        ///
        /// use_external_buffer=true: `ReadBufferFromDistributedCache::readFromFallbackBuffer`
        /// calls `buffer->set(internal_buffer.begin(), internal_buffer.size())` on the
        /// fallback to hand over DC's working buffer. The fallback must accept external
        /// memory.
        ///
        /// restrict_seek=false: DC calls `buffer->seek(file_offset_of_buffer_end, SEEK_SET)`
        /// on the fallback before reading. In the gather path the enclosing Gather handles
        /// seeks internally (per-object readers can have restricted seek). Here there is
        /// no Gather, so the underlying object reader must support seeks directly.
        auto fallback_creator = [storage = dc_obj_source->storage,
                                 read_hint = dc_obj_source->read_hint,
                                 captured_object = object,
                                 captured_settings = settings]()
            -> std::unique_ptr<ReadBufferFromFileBase>
        {
            return storage->readObject(captured_object, captured_settings, read_hint,
                /* use_external_buffer */ true, /* restrict_seek */ false);
        };

        /// DC uses external buffer mode when a downstream stage (memory cache or
        /// async prefetch) wraps it — consistent with the gather path above.
        auto impl = DistributedCache::readWithDistributedCache(
            object.local_path,
            source->objects,
            settings,
            *dc_obj_source->storage,
            use_ext_buf,
            std::move(fallback_creator),
            distributed_cache->include_credentials_in_cache_key);
        chassert(impl, "readWithDistributedCache must return a valid buffer or throw");
        return impl;
    }
#endif

    /// -- Stages 1+2: Source + FilesystemCache(s) (single-object, no DC, no gather) --
    if (!filesystem_caches.empty())
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
                "ReadPipeline: filesystem cache without gather requires ObjectStorageSource or CustomSource");
        }

        /// Wrap inner cache layers (all except the outermost). Each layer's
        /// impl_creator produces the previous layer's CachedOnDiskReadBufferFromFile.
        /// Inner layers always use use_external_buffer=true.
        for (size_t i = 0; i + 1 < filesystem_caches.size(); ++i)
        {
            const auto & dc = filesystem_caches[i];
            auto fs_cache_settings = dc.cache_settings;
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
                throttler = settings.local_throttler,
                query_id
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
                    query_id,
                    object_size,
                    /* allow_seeks_after_first_read */ true,
                    /* use_external_buffer */ true,
                    /* read_until_position */ std::nullopt,
                    cache_log, throttler);
            };
        }

        /// Build the outermost CachedOnDiskReadBufferFromFile.
        /// The impl_creator now produces the full inner cache chain.
        const auto & outermost = filesystem_caches.back();
        auto fs_cache_settings = outermost.cache_settings;
        auto cache_key = outermost.custom_cache_key.value_or(FileCacheKey::fromPath(object.remote_path));
        auto origin = outermost.custom_origin.value_or(outermost.cache->getCommonOriginWithSegmentKeyType(object.local_path));

        return std::make_unique<CachedOnDiskReadBufferFromFile>(
            object.remote_path,
            cache_key,
            outermost.cache,
            origin,
            std::move(impl_creator),
            fs_cache_settings,
            settings.remote_fs_buffer_size,
            settings.local_fs_buffer_size,
            query_id,
            object.bytes_size,
            /* allow_seeks_after_first_read */ true,
            use_ext_buf,
            /* read_until_position */ std::nullopt,
            outermost.cache_log,
            settings.local_throttler);
    }

    /// -- Stage 1 only: Source (no cache, no DC, no gather) --
    return std::visit(Overloaded{
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

std::unique_ptr<ReadBufferFromFileBase> ReadPipeline::wrapMemoryCache(std::unique_ptr<ReadBufferFromFileBase> impl) const
{
    /// -- Stage 4: Memory cache --
    if (!memory_cache || !memory_cache->cache)
        return impl;

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

    return std::make_unique<CachedInMemoryReadBufferFromFile>(
        cache_file, memory_cache->cache, std::move(impl), memory_cache->page_cache_settings);
}

std::unique_ptr<ReadBufferFromFileBase> ReadPipeline::wrapAsyncPrefetch(std::unique_ptr<ReadBufferFromFileBase> impl) const
{
    /// -- Stage 5: Async prefetch --
    /// Only applied when a caller explicitly requests it via `needAsyncPrefetch`.
    /// Today that's `DiskObjectStorage::prepareRead` (for remote reads with
    /// `remote_fs_method=threadpool` or distributed cache) and
    /// `StorageObjectStorageSource`. Local reads do not use this stage — they
    /// rely on `createReadBufferFromFileBase` (which can itself be async at the
    /// file-descriptor level via `pread_threadpool`/`io_uring`).
    if (!async_prefetch || !async_prefetch->reader)
        return impl;

    const auto & settings = source->read_settings;

    size_t total_size = getTotalSize(source->objects);
    size_t async_buffer_size = settings.remote_fs_buffer_size;
    if (total_size > 0 && total_size != StoredObject::UnknownSize)
        async_buffer_size = std::min(async_buffer_size, total_size);

    /// When distributed cache is active, use its min_bytes_for_seek
    /// (typically larger, since seeks within the cache are cheaper).
    size_t min_bytes_for_seek = distributed_cache
        ? settings.distributed_cache_settings.min_bytes_for_seek
        : settings.remote_read_min_bytes_for_seek;

    /// When the memory-cache stage is enabled, `AsynchronousBoundedReadBuffer`
    /// detects its `CachedInMemoryReadBufferFromFile` inner buffer and uses
    /// `page_cache_block_size` as the prefetch alignment. That alignment MUST
    /// match the block size the memory-cache stage was configured with,
    /// otherwise prefetches don't line up with cache blocks.
    size_t async_page_cache_block_size = memory_cache
        ? memory_cache->page_cache_settings.page_cache_block_size
        : settings.page_cache_settings.page_cache_block_size;

    return std::make_unique<AsynchronousBoundedReadBuffer>(
        std::move(impl),
        *async_prefetch->reader,
        async_buffer_size,
        min_bytes_for_seek,
        settings.priority,
        async_page_cache_block_size,
        settings.enable_filesystem_read_prefetches_log,
        async_prefetch->async_read_counters,
        async_prefetch->prefetches_log);
}

std::unique_ptr<ReadBufferFromFileBase> ReadPipeline::wrapDecryption(std::unique_ptr<ReadBufferFromFileBase> impl) const
{
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
        std::visit(Overloaded{
            [&](const ObjectStorageSource &) { append("Source(ObjectStorage)"); },
            [&](const LocalFileSource &) { append("Source(LocalFile)"); },
            [&](const BackupSource &) { append("Source(Backup)"); },
            [&](const CustomSource &) { append("Source(Custom)"); }
        }, source->source);
    }
    for (size_t i = 0; i < filesystem_caches.size(); ++i)
        append("FilesystemCache");
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
