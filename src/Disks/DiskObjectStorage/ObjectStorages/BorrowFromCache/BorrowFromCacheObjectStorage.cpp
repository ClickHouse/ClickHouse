#include <Disks/DiskObjectStorage/ObjectStorages/BorrowFromCache/BorrowFromCacheObjectStorage.h>

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/copyData.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/WriteBufferToFileSegment.h>
#include <Common/ObjectStorageKeyGenerator.h>
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
    std::optional<size_t> read_hint) const
{
    std::string cache_path;
    {
        std::lock_guard lock(mutex);
        auto * entry = findEntry(object.remote_path);
        if (!entry)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Object not found in BorrowFromCacheObjectStorage: {}", object.remote_path);
        cache_path = entry->cache_path;
    }

    LOG_TEST(log, "Read object: {} -> {}", object.remote_path, cache_path);
    return createReadBufferFromFileBase(cache_path, patchSettings(read_settings), read_hint);
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
    auto segment_holder = file_cache->set(
        key, 0, std::max<size_t>(1, buf_size),
        CreateFileSegmentSettings(FileSegmentKind::Ephemeral), FileCache::getCommonOrigin());

    chassert(segment_holder->size() == 1);
    segment_holder->front().getKeyMetadata()->createBaseDirectory(/* throw_if_failed */ true);

    std::string cache_path = segment_holder->front().getPath();
    LOG_TEST(log, "Write object: {} -> {}", object.remote_path, cache_path);

    auto write_buffer = std::make_unique<WriteBufferToFileSegment>(&segment_holder->front(), buf_size);

    {
        std::lock_guard lock(mutex);
        entries[object.remote_path] = SegmentEntry{std::move(segment_holder), cache_path};
    }

    return write_buffer;
}

void BorrowFromCacheObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    std::lock_guard lock(mutex);
    entries.erase(object.remote_path);
}

void BorrowFromCacheObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    std::lock_guard lock(mutex);
    for (const auto & object : objects)
        entries.erase(object.remote_path);
}

ObjectMetadata BorrowFromCacheObjectStorage::getObjectMetadata(const std::string & path, bool /* with_tags */) const
{
    std::string cache_path;
    {
        std::lock_guard lock(mutex);
        auto * entry = findEntry(path);
        if (!entry)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Object not found in BorrowFromCacheObjectStorage: {}", path);
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
    std::string cache_path;
    {
        std::lock_guard lock(mutex);
        auto * entry = findEntry(path);
        if (!entry)
            return std::nullopt;
        cache_path = entry->cache_path;
    }

    std::error_code error;
    auto time = fs::last_write_time(cache_path, error);
    if (error)
        return std::nullopt;

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
