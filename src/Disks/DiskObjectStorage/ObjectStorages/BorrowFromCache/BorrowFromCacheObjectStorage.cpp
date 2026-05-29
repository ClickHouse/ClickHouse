#include <Disks/DiskObjectStorage/ObjectStorages/BorrowFromCache/BorrowFromCacheObjectStorage.h>

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <IO/copyData.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/WriteBufferToFileSegment.h>
#include <Common/BlobStorageLogWriter.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
}

namespace
{
    /// Decorator that keeps a shared `FileSegmentsHolder` alive for as long as the write buffer is alive.
    /// The same holder is also stored in `BorrowFromCacheObjectStorage::entries`. Sharing ownership
    /// ensures the segment outlives both the buffer and concurrent removals from `entries`,
    /// avoiding a use-after-free of the raw `FileSegment *` held inside `WriteBufferToFileSegment`.
    class HoldingWriteBuffer : public WriteBufferFromFileDecorator
    {
    public:
        HoldingWriteBuffer(std::unique_ptr<WriteBuffer> impl_, std::shared_ptr<FileSegmentsHolder> holder_)
            : WriteBufferFromFileDecorator(std::move(impl_))
            , holder(std::move(holder_))
        {
        }

    private:
        std::shared_ptr<FileSegmentsHolder> holder;
    };

    /// Decorator that keeps a shared `FileSegmentsHolder` alive for the lifetime of the read buffer.
    /// `BorrowFromCacheObjectStorage::removeObjectIfExists` can erase the entry concurrently and
    /// destroy the last holder, which for `Ephemeral` segments releases the underlying cache file.
    /// Holding ownership here ensures the file stays alive until the reader is destroyed.
    class HoldingReadBuffer : public ReadBufferFromFileDecorator
    {
    public:
        HoldingReadBuffer(std::unique_ptr<ReadBufferFromFileBase> impl_, std::shared_ptr<FileSegmentsHolder> holder_)
            : ReadBufferFromFileDecorator(std::move(impl_))
            , holder(std::move(holder_))
        {
        }

        /// `ReadBufferFromFileDecorator` does not forward this, so the base `SeekableReadBuffer`
        /// version throws `NOT_IMPLEMENTED`. `ReadBufferFromRemoteFSGather` calls this in a
        /// `chassert`, which trips on debug/sanitizer builds during `IDisk::checkAccess`.
        size_t getFileOffsetOfBufferEnd() const override { return impl->getFileOffsetOfBufferEnd(); }

    private:
        std::shared_ptr<FileSegmentsHolder> holder;
    };
}

BorrowFromCacheObjectStorage::BorrowFromCacheObjectStorage(const std::string & name_, FileCachePtr file_cache_)
    : name(name_)
    , file_cache(std::move(file_cache_))
    , log(getLogger("BorrowFromCacheObjectStorage"))
{
}

BorrowFromCacheObjectStorage::SegmentEntry * BorrowFromCacheObjectStorage::findEntry(const std::string & remote_path) const
{
    auto it = entries.find(remote_path);
    if (it == entries.end())
        return nullptr;
    return &it->second;
}

bool BorrowFromCacheObjectStorage::exists(const StoredObject & object) const
{
    std::lock_guard lock(mutex);
    return findEntry(object.remote_path) != nullptr;
}

ReadSettings BorrowFromCacheObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    auto modified_settings{read_settings};
    modified_settings.local_fs_method = LocalFSReadMethod::pread;
    modified_settings.direct_io_threshold = 0;
    return IObjectStorage::patchSettings(modified_settings);
}

std::unique_ptr<ReadBufferFromFileBase> BorrowFromCacheObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    bool /* use_external_buffer */,
    bool /* restrict_seek */) const
{
    /// Capture both `cache_path` and the `holder` under the lock. The holder is shared
    /// with the returned read buffer via `HoldingReadBuffer` so the underlying cache file
    /// outlives concurrent removals from `entries`.
    std::shared_ptr<FileSegmentsHolder> holder;
    std::string cache_path;
    {
        std::lock_guard lock(mutex);
        auto * entry = findEntry(object.remote_path);
        if (!entry)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Object not found in BorrowFromCacheObjectStorage: {}", object.remote_path);
        holder = entry->holder;
        cache_path = entry->cache_path;
    }

    LOG_TEST(log, "Read object: {} -> {}", object.remote_path, cache_path);
    auto inner = createReadBufferFromFileBase(cache_path, patchSettings(read_settings), read_hint);
    return std::make_unique<HoldingReadBuffer>(std::move(inner), std::move(holder));
}

std::unique_ptr<WriteBufferFromFileBase> BorrowFromCacheObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> /* attributes */,
    size_t buf_size,
    const WriteSettings & /* write_settings */)
{
    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "BorrowFromCacheObjectStorage doesn't support append to files");

    const auto key = FileSegment::Key::random();
    std::shared_ptr<FileSegmentsHolder> segment_holder = file_cache->set(
        key, 0, std::max<size_t>(1, buf_size),
        CreateFileSegmentSettings(FileSegmentKind::Ephemeral), FileCache::getCommonOrigin());

    chassert(segment_holder->size() == 1);
    segment_holder->front().getKeyMetadata()->createBaseDirectory(/* throw_if_failed */ true);

    std::string cache_path = segment_holder->front().getPath();
    LOG_TEST(log, "Write object: {} -> {}", object.remote_path, cache_path);

    auto inner_buffer = std::make_unique<WriteBufferToFileSegment>(&segment_holder->front(), buf_size);
    auto write_buffer = std::make_unique<HoldingWriteBuffer>(std::move(inner_buffer), segment_holder);

    {
        std::lock_guard lock(mutex);
        entries[object.remote_path] = SegmentEntry{segment_holder, cache_path};
    }

    return write_buffer;
}

void BorrowFromCacheObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    auto blob_storage_log = BlobStorageLogWriter::create(name);

    Stopwatch watch;
    {
        std::lock_guard lock(mutex);
        entries.erase(object.remote_path);
    }

    if (blob_storage_log)
    {
        blob_storage_log->addEvent(
            BlobStorageLogElement::EventType::Delete,
            /* bucket */ name,
            /* remote_path */ object.remote_path,
            /* local_path */ object.local_path,
            /* data_size */ object.bytes_size,
            /* elapsed_microseconds */ watch.elapsedMicroseconds(),
            /* error_code */ 0,
            /* error_message */ "");
    }
}

void BorrowFromCacheObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    auto blob_storage_log = BlobStorageLogWriter::create(name);

    Stopwatch watch;
    {
        std::lock_guard lock(mutex);
        for (const auto & object : objects)
            entries.erase(object.remote_path);
    }

    if (blob_storage_log)
    {
        auto elapsed = watch.elapsedMicroseconds();
        auto time_now = std::chrono::system_clock::now();
        for (const auto & object : objects)
        {
            blob_storage_log->addEvent(
                BlobStorageLogElement::EventType::Delete,
                /* bucket */ name,
                /* remote_path */ object.remote_path,
                /* local_path */ object.local_path,
                /* data_size */ object.bytes_size,
                /* elapsed_microseconds */ !objects.empty() ? elapsed / objects.size() : elapsed,
                /* error_code */ 0,
                /* error_message */ "",
                time_now);
        }
    }
}

ObjectMetadata BorrowFromCacheObjectStorage::getObjectMetadata(const std::string & path, bool /* with_tags */) const
{
    /// Hold the `FileSegmentsHolder` for the whole method so the cache file is not removed
    /// by a concurrent `removeObjectIfExists` between releasing the lock and reading metadata.
    std::shared_ptr<FileSegmentsHolder> holder;
    std::string cache_path;
    {
        std::lock_guard lock(mutex);
        auto * entry = findEntry(path);
        if (!entry)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Object not found in BorrowFromCacheObjectStorage: {}", path);
        holder = entry->holder;
        cache_path = entry->cache_path;
    }

    ObjectMetadata metadata;
    auto time = fs::last_write_time(cache_path);
    metadata.size_bytes = fs::file_size(cache_path);
    metadata.etag = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(time.time_since_epoch()).count());
    metadata.last_modified = Poco::Timestamp::fromEpochTime(
        std::chrono::duration_cast<std::chrono::seconds>(time.time_since_epoch()).count());
    return metadata;
}

std::optional<ObjectMetadata> BorrowFromCacheObjectStorage::tryGetObjectMetadata(const std::string & path, bool /* with_tags */) const
{
    /// Hold the `FileSegmentsHolder` for the whole method so the cache file is not removed
    /// by a concurrent `removeObjectIfExists` between releasing the lock and reading metadata.
    std::shared_ptr<FileSegmentsHolder> holder;
    std::string cache_path;
    {
        std::lock_guard lock(mutex);
        auto * entry = findEntry(path);
        if (!entry)
            return std::nullopt;
        holder = entry->holder;
        cache_path = entry->cache_path;
    }

    std::error_code error;
    auto time = fs::last_write_time(cache_path, error);
    if (error)
    {
        if (error == std::errc::no_such_file_or_directory)
            return std::nullopt;
        throw fs::filesystem_error("Cannot get last write time", cache_path, error);
    }

    ObjectMetadata metadata;
    metadata.size_bytes = fs::file_size(cache_path);
    metadata.etag = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(time.time_since_epoch()).count());
    metadata.last_modified = Poco::Timestamp::fromEpochTime(
        std::chrono::duration_cast<std::chrono::seconds>(time.time_since_epoch()).count());
    return metadata;
}

void BorrowFromCacheObjectStorage::copyObject( /// NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> /* object_to_attributes */)
{
    auto in = readObject(object_from, read_settings, /* read_hint= */ {});
    auto out = writeObject(object_to, WriteMode::Rewrite, /* attributes= */ {}, /* buf_size= */ DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    copyData(*in, *out);
    out->finalize();
}

void BorrowFromCacheObjectStorage::shutdown()
{
    std::lock_guard lock(mutex);
    entries.clear();
}

void BorrowFromCacheObjectStorage::startup()
{
}

ObjectStorageKeyGeneratorPtr BorrowFromCacheObjectStorage::createKeyGenerator() const
{
    return createObjectStorageKeyGeneratorByPrefix("");
}

}
