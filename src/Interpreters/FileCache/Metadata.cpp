#include <Interpreters/FileCache/Metadata.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileSegment.h>
#include <Interpreters/Context.h>
#include <Common/ProfileEvents.h>
#include <Common/filesystemHelpers.h>
#include <Interpreters/FileCache/FileCacheUtils.h>
#include <Common/logger_useful.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ErrnoException.h>
#include <Common/FailPoint.h>
#include <Common/ThreadPool.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/threadPoolCallbackRunner.h>
#include <filesystem>
#include <unordered_set>
#include <Interpreters/FileCache/FileSegmentInfo.h>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheDownloadQueueElements;
    extern const Metric FilesystemCacheDelayedCleanupElements;
    extern const Metric FilesystemCacheCleanupHelperThreads;
    extern const Metric FilesystemCacheCleanupHelperThreadsActive;
    extern const Metric FilesystemCacheCleanupHelperThreadsScheduled;
    extern const Metric FilesystemCacheKeys;
}

namespace ProfileEvents
{
    extern const Event FilesystemCacheLockOriginPoolMicroseconds;
    extern const Event FilesystemCacheCreatedKeyDirectories;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int FILECACHE_ACCESS_DENIED;
    extern const int CANNOT_RMDIR;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace FailPoints
{
    extern const char file_cache_empty_key_directory_queue_memory_limit[];
}

namespace
{

/// Returns false when the directory is missing or not empty.
bool removeDirectoryIfEmpty(const fs::path & directory)
{
    if (0 == ::rmdir(directory.c_str()))
        return true;

    /// Saved because building the exception message may change `errno`.
    const int rmdir_errno = errno;
    if (rmdir_errno == ENOENT || rmdir_errno == ENOTEMPTY || rmdir_errno == EEXIST)
        return false;
    ErrnoException::throwFromPathWithErrno(
        ErrorCodes::CANNOT_RMDIR, directory, rmdir_errno, "Cannot remove directory {}", directory.string());
}

}

FileSegmentMetadata::FileSegmentMetadata(FileSegmentPtr && file_segment_)
    : file_segment(std::move(file_segment_))
{
    switch (file_segment->state())
    {
        case FileSegment::State::DOWNLOADED:
        {
            chassert(file_segment->getQueueIterator());
            break;
        }
        case FileSegment::State::EMPTY:
        case FileSegment::State::DOWNLOADING:
        {
            break;
        }
        default:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can create file segment with either EMPTY, DOWNLOADED, DOWNLOADING state, got: {}",
                FileSegment::stateToString(file_segment->state()));
    }
}

size_t FileSegmentMetadata::size() const
{
    auto metadata = file_segment->tryGetKeyMetadata();
    if (metadata && metadata->useRealDiskSize())
        return metadata->alignFileSize(file_segment->getReservedSize());
    return file_segment->getReservedSize();
}


KeyMetadata::KeyMetadata(
    const Key & key_,
    OriginInfoPtr origin_,
    const CacheMetadata * cache_metadata_,
    bool created_base_directory_)
    : key(key_)
    , origin(std::move(origin_))
    , cache_metadata(cache_metadata_)
    , created_base_directory(created_base_directory_)
{
    chassert(origin);

    if (*origin == FileCache::getInternalOrigin())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create key metadata with internal user id");

    if (!origin->weight.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create key metadata without user weight");

    chassert(!created_base_directory || fs::exists(getPath()));
}

bool KeyMetadata::checkAccess(const UserID & user_id_) const
{
    return user_id_ == origin->user_id || user_id_ == FileCache::getInternalOrigin().user_id;
}

void KeyMetadata::assertAccess(const UserID & user_id_) const
{
    if (!checkAccess(user_id_))
    {
        throw Exception(ErrorCodes::FILECACHE_ACCESS_DENIED,
                        "Metadata for key {} belongs to another user",
                        key.toString());
    }
}

CacheMetadata::OriginInfoPtr CacheMetadata::getOrCreateSharedOrigin(const OriginInfo & origin)
{
    OriginPoolKey pool_key{origin.user_id, origin.weight, origin.segment_type};
    return origins.withShard(pool_key, [&](auto & map) -> OriginInfoPtr
    {
        auto it = map.find(pool_key);
        if (it == map.end())
            it = map.emplace(pool_key, std::make_shared<const OriginInfo>(origin)).first;
        return it->second;
    });
}

void CacheMetadata::removeSharedOrigins(const UserID & user_id)
{
    /// A single client can own several pool keys (distinct weight / segment_type), which the
    /// pool spreads across shards by the full key's hash, so scan every shard.
    origins.forEachShard([&](auto & map)
    {
        for (auto it = map.begin(); it != map.end();)
        {
            if (it->first.user_id == user_id)
                it = map.erase(it);
            else
                ++it;
        }
    });
}

LockedKeyPtr KeyMetadata::lock()
{
    auto locked = tryLock();
    if (locked)
        return locked;

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot lock key {} (state: {})", key, magic_enum::enum_name(key_state));
}

LockedKeyPtr KeyMetadata::tryLock()
{
    auto locked = lockNoStateCheck();
    if (key_state == KeyMetadata::KeyState::ACTIVE)
        return locked;

    return nullptr;
}

LockedKeyPtr KeyMetadata::lockNoStateCheck()
{
    return std::make_unique<LockedKey>(shared_from_this());
}

KeyMetadata::KeyState KeyMetadata::getState()
{
    auto locked = lockNoStateCheck();
    return key_state;
}

std::error_code KeyMetadata::createBaseDirectory()
{
    if (created_base_directory.load())
        return {};

    std::shared_lock lock(cache_metadata->key_prefix_directory_mutex);

    if (created_base_directory.load(std::memory_order_relaxed))
        return {};

    std::error_code ec;
    fs::create_directories(getPath(), ec);

    if (!ec)
    {
        created_base_directory.store(true);
        ProfileEvents::increment(ProfileEvents::FilesystemCacheCreatedKeyDirectories);
    }
    else if (ec != std::errc::no_space_on_device && ec != std::errc::too_many_files_open)
        LOG_TRACE(cache_metadata->log, "Failed to create base directory for key {}, {}", key, ec.message());


    return ec;
}

std::string KeyMetadata::getPath() const
{
    return cache_metadata->getKeyPath(key, *origin);
}

std::string KeyMetadata::getFileSegmentPath(const FileSegment & file_segment) const
{
    /// A fully downloaded regular segment carries its size in the file name; until then
    /// (while downloading) the file is named just by its offset. `hasSizeInFileName` tells
    /// which of the two names the file currently has on disk.
    std::optional<size_t> size;
    if (file_segment.hasSizeInFileName())
        size = file_segment.range().size();
    return cache_metadata->getFileSegmentPath(key, file_segment.offset(), file_segment.getKind(), *origin, size);
}

std::string KeyMetadata::getFileSegmentPath(size_t offset, FileSegmentKind segment_kind, std::optional<size_t> size) const
{
    return cache_metadata->getFileSegmentPath(key, offset, segment_kind, *origin, size);
}

LoggerPtr KeyMetadata::logger() const
{
    return cache_metadata->log;
}

size_t KeyMetadata::alignFileSize(size_t file_size) const
{
    return cache_metadata->alignFileSize(file_size);
}

bool KeyMetadata::useRealDiskSize() const
{
    return cache_metadata->useRealDiskSize();
}

CacheMetadata::CacheMetadata(
    const std::string & path_,
    size_t background_download_queue_size_limit_,
    size_t background_download_threads_,
    bool write_cache_per_user_directory_,
    bool use_real_disk_size_,
    size_t background_cleanup_threads_,
    size_t background_cleanup_elements_per_thread_)
    : path(path_)
    , cleanup_queue(std::make_shared<CleanupQueue>())
    , download_queue(std::make_shared<DownloadQueue>(background_download_queue_size_limit_))
    , write_cache_per_user_directory(write_cache_per_user_directory_)
    , use_real_disk_size(use_real_disk_size_)
    , log(getLogger("CacheMetadata"))
    , origins(ProfileEvents::FilesystemCacheLockOriginPoolMicroseconds)
    , download_threads_num(background_download_threads_)
    , max_cleanup_threads(background_cleanup_threads_)
    , cleanup_elements_per_thread(background_cleanup_elements_per_thread_)
{

}

CacheMetadata::~CacheMetadata() = default;

size_t CacheMetadata::alignFileSize(size_t file_size) const
{
    const size_t block_size = fs_block_size.load(std::memory_order_acquire);
    if (block_size == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Filesystem block size is not initialized for cache path {}", path);
    return FileCacheUtils::roundUpToMultiple(file_size, block_size);
}

bool CacheMetadata::useRealDiskSize() const
{
    return use_real_disk_size;
}

void CacheMetadata::fillStatVFS()
{
    const auto stat = getStatVFS(path);
    /// Use `f_frsize` (the fundamental filesystem block/fragment size), not `f_bsize` (the preferred I/O
    /// block size), because on-disk space is accounted in `f_frsize` units (`f_blocks`, `f_bfree`, `f_bavail`
    /// are all counted in `f_frsize`), and `FileCacheSettings` already computes total space via `f_frsize`.
    /// Fall back to `f_bsize` when `f_frsize` is reported as zero.
    const size_t block_size = stat.f_frsize != 0 ? stat.f_frsize : stat.f_bsize;
    fs_block_size.store(block_size, std::memory_order_release);
}

String CacheMetadata::getFileNameForFileSegment(size_t offset, FileSegmentKind segment_kind, std::optional<size_t> size)
{
    switch (segment_kind)
    {
        case FileSegmentKind::Ephemeral:
            /// Temporary (ephemeral) segments are removed on startup, so there is no point
            /// in encoding their size; keep the historic "_temporary" marker instead.
            return std::to_string(offset) + "_temporary";
        case FileSegmentKind::Regular:
            /// `<offset>_<size>` once the segment is fully downloaded, just `<offset>` while
            /// it is still being written (final size not yet known).
            if (size.has_value())
                return std::to_string(offset) + "_" + std::to_string(*size);
            return std::to_string(offset);
    }
}

String CacheMetadata::getFileSegmentPath(
    const Key & key,
    size_t offset,
    FileSegmentKind segment_kind,
    const OriginInfo & origin,
    std::optional<size_t> size) const
{
    return fs::path(getKeyPath(key, origin)) / getFileNameForFileSegment(offset, segment_kind, size);
}

String CacheMetadata::getKeyPath(const Key & key, const OriginInfo & origin) const
{
    const auto key_str = key.toString();
    const auto key_type_prefix = getKeyTypePrefix(origin.segment_type);
    if (write_cache_per_user_directory)
    {
        /// The id is a single path component: every carrier of a non-internal id rejects a NUL and
        /// a '/' up front via `DistributedCache::getClientIDRejectionReason`.
        chassert(!origin.user_id.contains('\0') && !origin.user_id.contains('/'));
        return fs::path(path) / key_type_prefix / fmt::format("{}.{}", origin.user_id, origin.weight.value()) / key_str.substr(0, 3) / key_str;
    }

    return fs::path(path) / key_type_prefix / key_str.substr(0, 3) / key_str;
}

CacheMetadataGuard::Lock CacheMetadata::MetadataBucket::lock() const
{
    return guard.lock();
}

CacheMetadata::MetadataBucket & CacheMetadata::getMetadataBucket(const Key & key)
{
    const auto bucket = static_cast<size_t>(key.key % buckets_num);
    return metadata_buckets[bucket];
}

LockedKeyPtr CacheMetadata::lockKeyMetadata(
    const FileCacheKey & key,
    KeyNotFoundPolicy key_not_found_policy,
    const OriginInfo & origin,
    bool is_initial_load)
{
    auto key_metadata = getKeyMetadata(key, key_not_found_policy, origin, is_initial_load);
    if (!key_metadata)
        return nullptr;

    {
        auto locked_metadata = key_metadata->lockNoStateCheck();
        const auto key_state = locked_metadata->getKeyState();

        if (key_state == KeyMetadata::KeyState::ACTIVE)
            return locked_metadata;

        if (key_not_found_policy == KeyNotFoundPolicy::THROW)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key `{}` in cache", key);
        if (key_not_found_policy == KeyNotFoundPolicy::THROW_LOGICAL)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key `{}` in cache", key);

        if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL)
            return nullptr;

        if (key_state == KeyMetadata::KeyState::QUEUED_FOR_REMOVAL)
        {
            locked_metadata->removeFromCleanupQueue();
            LOG_DEBUG(log, "Removal of key {} is cancelled", key);
            return locked_metadata;
        }

        chassert(key_state == KeyMetadata::KeyState::REMOVED);
        chassert(key_not_found_policy == KeyNotFoundPolicy::CREATE_EMPTY);
    }

    /// Now we are at the case when the key was removed (key_state == KeyMetadata::KeyState::REMOVED)
    /// but we need to return empty key (key_not_found_policy == KeyNotFoundPolicy::CREATE_EMPTY)
    /// Retry
    return lockKeyMetadata(key, key_not_found_policy, origin);
}

KeyMetadataPtr CacheMetadata::getKeyMetadata(
    const Key & key,
    KeyNotFoundPolicy key_not_found_policy,
    const OriginInfo & origin,
    bool is_initial_load)
{
    KeyMetadataPtr result;
    {
        auto & bucket = getMetadataBucket(key);
        auto lock = bucket.lock();

        auto it = bucket.find(key);
        if (it == bucket.end())
        {
            if (key_not_found_policy == KeyNotFoundPolicy::THROW)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key `{}` in cache", key);
            if (key_not_found_policy == KeyNotFoundPolicy::THROW_LOGICAL)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key `{}` in cache", key);
            if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL)
                return nullptr;

            it = bucket.emplace(
                key, std::make_shared<KeyMetadata>(key, getOrCreateSharedOrigin(origin), this, is_initial_load)).first;

            CurrentMetrics::add(CurrentMetrics::FilesystemCacheKeys);
        }

        it->second->assertAccess(origin.user_id);
        result = it->second;
    }

    /// Refresh idle-client TTL after releasing the bucket lock: prevents
    /// lock-order inversion with the eviction task. Skip internal/common ids
    /// and probe-only lookups (result == nullptr). The startup load is not a client
    /// access, so it must not pass for one (the TTL counts from the load regardless).
    if (result && on_client_access && !is_initial_load)
    {
        const auto & user_id = origin.user_id;
        if (!user_id.empty()
            && user_id != FileCache::getInternalOrigin().user_id
            && user_id != FileCache::getCommonOrigin().user_id)
        {
            on_client_access(user_id);
        }
    }

    return result;
}

bool CacheMetadata::isEmpty() const
{
    for (const auto & bucket : metadata_buckets)
        if (!bucket.empty())
            return false;
    return true;
}

void CacheMetadata::iterate(IterateFunc && func, const KeyMetadata::UserID & user_id)
{
    for (auto & bucket : metadata_buckets)
    {
        auto lk = bucket.lock();
        for (auto & [key, key_metadata] : bucket)
        {
            if (!key_metadata->checkAccess(user_id))
                continue;

            auto locked_key = key_metadata->lockNoStateCheck();
            const auto key_state = locked_key->getKeyState();

            if (key_state == KeyMetadata::KeyState::ACTIVE)
            {
                func(*locked_key);
                continue;
            }
            if (key_state == KeyMetadata::KeyState::QUEUED_FOR_REMOVAL)
                continue;

            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Cannot lock key {}: key does not exist", key_metadata->key);
        }
    }
}

class CacheMetadata::IteratorImpl
{
public:
    IteratorImpl(MetadataBuckets & metadata_buckets_, const UserID & user_id_)
        : user_id(user_id_)
        , metadata_buckets(metadata_buckets_)
        , bucket_it(metadata_buckets_.begin())
    {
    }

    bool next(Iterator::OnFileSegmentFunc func)
    {
        while (true)
        {
            if (bucket_it == metadata_buckets.end())
                return false;

            if (!bucket_lock)
                bucket_lock = bucket_it->lock();

            if (!key_it.has_value())
                key_it = bucket_it->begin();

            if (key_it.value() == bucket_it->end())
            {
                ++bucket_it;
                bucket_lock.reset();

                key_it.reset();
                key_lock.reset();

                file_segment_it.reset();
                continue;
            }

            const auto & key = key_it.value()->second;

            if (!key_lock)
            {
                if (!key->checkAccess(user_id))
                {
                    ++key_it.value();
                    continue;
                }

                /// Will lock only if key is in state ACTIVE.
                key_lock = key->tryLock();
                if (!key_lock)
                {
                    ++key_it.value();
                    continue;
                }
            }

            if (!file_segment_it.has_value())
                file_segment_it = key->begin();

            if (file_segment_it.value() == key->end())
            {
                ++key_it.value();
                key_lock.reset();

                file_segment_it.reset();
                continue;
            }

            func(FileSegment::getInfo(file_segment_it.value()->second->file_segment));
            ++(file_segment_it.value());
            return true;
        }
    }

private:
    const UserID user_id;
    MetadataBuckets & metadata_buckets;
    MetadataBuckets::iterator bucket_it;
    std::optional<MetadataBucket::iterator> key_it;
    std::optional<KeyMetadata::iterator> file_segment_it;

    std::optional<CacheMetadataGuard::Lock> bucket_lock;
    LockedKeyPtr key_lock;
};

class CacheMetadata::BatchedIteratorImpl
{
public:
    BatchedIteratorImpl(MetadataBuckets & metadata_buckets_, const UserID & user_id_)
        : user_id(user_id_)
        , metadata_buckets(metadata_buckets_)
        , bucket_it(metadata_buckets_.begin())
    {
    }

    bool next(Iterator::OnFileSegmentFunc func)
    {
        bool result = false;
        while (bucket_it != metadata_buckets.end())
        {
            auto bucket_lock = bucket_it->lock();
            for (const auto & [_, key_metadata] : *bucket_it)
            {
                if (!key_metadata->checkAccess(user_id))
                    continue;

                /// Will lock only if key is in state ACTIVE.
                auto key_lock = key_metadata->tryLock();
                if (!key_lock)
                    continue;

                result |= key_metadata->size();
                for (const auto & [_, file_segment_metadata] : *key_metadata)
                    func(FileSegment::getInfo(file_segment_metadata->file_segment));
            }
            ++bucket_it;
            if (result)
                break;
        }
        return result;
    }

private:
    const UserID user_id;
    MetadataBuckets & metadata_buckets;
    MetadataBuckets::iterator bucket_it;
};

CacheMetadata::Iterator::Iterator(const UserID & user_id_, MetadataBuckets & metadata_buckets_)
    : user_id(user_id_), metadata_buckets(metadata_buckets_)
{
}

bool CacheMetadata::Iterator::next(OnFileSegmentFunc func)
{
    if (!impl.has_value())
        impl = std::make_shared<IteratorImpl>(metadata_buckets, user_id);

    if (auto * iterator = std::get_if<CacheMetadata::IteratorImplPtr>(&impl.value()); iterator)
        return (*iterator)->next(func);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected IteratorImplPtr");
}

bool CacheMetadata::Iterator::nextBatch(OnFileSegmentFunc func)
{
    if (!impl)
        impl = std::make_shared<BatchedIteratorImpl>(metadata_buckets, user_id);

    if (auto * iterator = std::get_if<CacheMetadata::BatchedIteratorImplPtr>(&impl.value()); iterator)
        return (*iterator)->next(func);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected BatchedIteratorImplPtr");
}

CacheMetadata::IteratorPtr CacheMetadata::getIterator(const UserID & user_id)
{
    return std::make_unique<Iterator>(user_id, metadata_buckets);
}

bool CacheMetadata::removeAllKeys(const UserID & user_id, ThreadPool * pool)
{
    std::atomic<bool> fully_removed = true;

    auto process_bucket = [&](MetadataBucket & bucket)
    {
        auto lock = bucket.lock();
        for (auto it = bucket.begin(); it != bucket.end();)
        {
            if (!it->second->checkAccess(user_id))
            {
                ++it;
                continue;
            }

            auto locked_key = it->second->lockNoStateCheck();
            if (locked_key->getKeyState() == KeyMetadata::KeyState::ACTIVE)
            {
                bool removed_all = locked_key->removeAllFileSegments();
                if (removed_all)
                {
                    it = removeEmptyKey(bucket, it, *locked_key, lock);
                    continue;
                }
                /// Non-releasable segments remain (held by some FileSegmentsHolder).
                fully_removed = false;
            }
            ++it;
        }
    };

    if (pool)
    {
        std::atomic<size_t> next_bucket_index = 0;
        auto worker = [&]
        {
            for (size_t i = next_bucket_index.fetch_add(1, std::memory_order_relaxed);
                 i < metadata_buckets.size();
                 i = next_bucket_index.fetch_add(1, std::memory_order_relaxed))
            {
                process_bucket(metadata_buckets[i]);
            }
        };

        ThreadPoolCallbackRunnerLocal<void> runner(*pool, ThreadName::FILESYSTEM_CACHE_DROP);
        /// The calling thread participates, so schedule one fewer worker.
        const size_t extra_workers = std::min(pool->getMaxThreads(), metadata_buckets.size()) - 1;
        for (size_t i = 0; i < extra_workers; ++i)
            runner.enqueueAndKeepTrack(worker);
        worker();
        runner.waitForAllToFinishAndRethrowFirstError();
    }
    else
    {
        for (auto & bucket : metadata_buckets)
            process_bucket(bucket);
    }

    /// With all of this client's keys gone, the origins deduplicated for it in the pool are no
    /// longer referenced by any key, so drop them too; otherwise the pool grows without bound as
    /// clients come and go (e.g. idle-client TTL eviction). Skip when some segment is still held:
    /// its key (and thus its shared origin) survives, and a re-access would re-create the entry.
    if (fully_removed)
        removeSharedOrigins(user_id);

    return fully_removed;
}

void CacheMetadata::removeKey(const Key & key, bool if_exists, const UserID & user_id)
{
    auto & bucket = getMetadataBucket(key);
    auto lock = bucket.lock();
    auto it = bucket.find(key);
    if (it == bucket.end())
    {
        if (if_exists)
            return;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key: {}", key);
    }

    it->second->assertAccess(user_id);
    auto locked_key = it->second->lockNoStateCheck();
    auto state = locked_key->getKeyState();
    if (state != KeyMetadata::KeyState::ACTIVE)
    {
        if (if_exists)
            return;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key: {} (state: {})", key, magic_enum::enum_name(state));
    }

    bool removed_all = locked_key->removeAllFileSegments();
    if (removed_all)
        removeEmptyKey(bucket, it, *locked_key, lock);
}

CacheMetadata::MetadataBucket::iterator
CacheMetadata::removeEmptyKey(
    MetadataBucket & bucket,
    MetadataBucket::iterator it,
    LockedKey & locked_key,
    const CacheMetadataGuard::Lock &)
{
    const auto & key = locked_key.getKey();

    if (!locked_key.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove non-empty key: {}", key);

    locked_key.markAsRemoved();
    auto next_it = bucket.erase(it);

    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheKeys);

    LOG_TEST(log, "Key {} is removed from metadata", key);

    const fs::path key_directory = getKeyPath(key, *locked_key.getKeyMetadata()->origin);
    const fs::path key_prefix_directory = key_directory.parent_path();

    try
    {
        /// Why remove_all: because of migration to "file size in file name" we can have left over files in certain cases.
        /// FIXME: Can we get rid of those leftovers?
        /// Anyway remove_all here is safe since Key level lock is held throughout.
        if (removeDirectoryIfEmpty(key_directory) || fs::remove_all(key_directory))
            LOG_TEST(log, "Directory ({}) for key {} removed", key_directory.string(), key);
    }
    catch (...)
    {
        LOG_ERROR(log, "Error while removing key {}: {}", key, getCurrentExceptionMessage(true));
        chassert(false);
        return next_it;
    }

    try
    {
        std::unique_lock mutex(key_prefix_directory_mutex);
        if (removeDirectoryIfEmpty(key_prefix_directory))
        {
            LOG_TEST(log, "Prefix directory ({}) for key {} removed", key_prefix_directory.string(), key);

            /// Also drop the per-user-id directory once empty. Safe under the
            /// same mutex: createBaseDirectory takes it as a shared_lock, so
            /// no other thread can recreate this directory while we hold the
            /// unique_lock here.
            if (write_cache_per_user_directory)
            {
                const fs::path user_directory = key_prefix_directory.parent_path();
                if (removeDirectoryIfEmpty(user_directory))
                    LOG_TEST(log, "User directory ({}) for key {} removed", user_directory.string(), key);
            }
        }
    }
    catch (...)
    {
        LOG_ERROR(log, "Error while removing key {}: {}", key, getCurrentExceptionMessage(true));
        chassert(false);
    }
    return next_it;
}

void CacheMetadata::removeEmptyKeyDirectoryFromStartup(const std::string & key_prefix_directory, const Key & key)
{
    const auto key_directory = fs::path(key_prefix_directory) / key.toString();
    {
        auto & bucket = getMetadataBucket(key);
        auto lock = bucket.lock();
        if (bucket.contains(key) || !removeDirectoryIfEmpty(key_directory))
            return;
    }
    LOG_DEBUG(log, "Removed empty key directory: {}", key_directory.string());
}

struct CleanupTask
{
    /// A key which has no file segments left.
    explicit CleanupTask(const FileCacheKey & key_)
        : key(key_), startup_leftover(false)
    {
    }

    /// The key directories of one key prefix directory which the initial load found empty.
    CleanupTask(std::string && key_prefix_path_, VectorWithMemoryTracking<FileCacheKey> && startup_leftover_keys_)
        : startup_leftover(true)
        , key_prefix_path(std::move(key_prefix_path_))
        , startup_leftover_keys(std::move(startup_leftover_keys_))
    {
    }

    /// Not set for a startup leftover.
    FileCacheKey key{};
    bool startup_leftover;
    /// Set only for a startup leftover: its keys have no metadata to take the path from.
    std::string key_prefix_path;
    VectorWithMemoryTracking<FileCacheKey> startup_leftover_keys;
    /// Emptied keys and empty key directories which were still waiting when this task was taken.
    size_t backlog = 0;
};

class CleanupQueue
{
public:
    void add(const FileCacheKey & key)
    {
        {
            std::lock_guard lock(mutex);
            if (cancelled || !keys.insert(key).second)
                return;
            ++size;
            CurrentMetrics::add(CurrentMetrics::FilesystemCacheDelayedCleanupElements);
        }
        cv.notify_one();
    }

    void addEmptyKeyDirectoryFromStartup(const std::string & key_prefix_directory, const FileCacheKey & key)
    {
        {
            std::lock_guard lock(mutex);
            if (cancelled)
                return;

            auto it = empty_key_directories_from_startup.try_emplace(key_prefix_directory).first;
            SCOPE_EXIT({ if (it->second.empty()) empty_key_directories_from_startup.erase(it); });
            it->second.push_back(key);
            ++size;
            CurrentMetrics::add(CurrentMetrics::FilesystemCacheDelayedCleanupElements);
        }
        cv.notify_one();
    }

    /// Waits for a task and returns nothing when the queue is cancelled.
    std::optional<CleanupTask> waitAndPop()
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return cancelled || size != 0; });
        return popUnlocked();
    }

    /// Returns nothing when no task is waiting or the queue is cancelled.
    std::optional<CleanupTask> tryPop()
    {
        std::lock_guard lock(mutex);
        return popUnlocked();
    }

    /// For a failed initial load: a retried load would queue the same directories again.
    void dropEmptyKeyDirectoriesFromStartup()
    {
        std::lock_guard lock(mutex);

        size_t dropped_elements = 0;
        for (const auto & [_, keys_from_startup] : empty_key_directories_from_startup)
            dropped_elements += keys_from_startup.size();
        size -= dropped_elements;
        CurrentMetrics::sub(CurrentMetrics::FilesystemCacheDelayedCleanupElements, dropped_elements);

        empty_key_directories_from_startup.clear();
    }

    void cancel()
    {
        {
            std::lock_guard lock(mutex);
            cancelled = true;

            /// No task is taken from the queue after this point, so the ones which are left do not wait for cleanup anymore.
            CurrentMetrics::sub(CurrentMetrics::FilesystemCacheDelayedCleanupElements, size);
            size = 0;
            keys.clear();
            empty_key_directories_from_startup.clear();
        }
        cv.notify_all();
    }

private:
    /// Emptied keys go first. A task from startup is a whole key prefix directory, so that
    /// the cleanup threads remove directories under different parents and take this mutex less often.
    std::optional<CleanupTask> popUnlocked()
    {
        if (cancelled)
            return {};

        std::optional<CleanupTask> task;
        size_t taken_elements = 1;
        if (!keys.empty())
        {
            task.emplace(keys.extract(keys.begin()).value());
        }
        else if (!empty_key_directories_from_startup.empty())
        {
            auto node = empty_key_directories_from_startup.extract(empty_key_directories_from_startup.begin());
            taken_elements = node.mapped().size();
            task.emplace(std::move(node.key()), std::move(node.mapped()));
        }
        else
        {
            return {};
        }

        size -= taken_elements;
        task->backlog = size;
        CurrentMetrics::sub(CurrentMetrics::FilesystemCacheDelayedCleanupElements, taken_elements);
        return task;
    }

    std::unordered_set<FileCacheKey> keys;
    /// Key directories that the initial load found empty, by key prefix directory. There can be millions
    /// of them, so the memory limit applies: the load does not queue a directory which goes over it.
    UnorderedMapWithMemoryTracking<std::string, VectorWithMemoryTracking<FileCacheKey>> empty_key_directories_from_startup;
    /// Emptied keys plus key directories from startup.
    size_t size = 0;
    std::mutex mutex;
    std::condition_variable cv;
    bool cancelled = false;
};

void CacheMetadata::removeKeyIfStillQueuedForRemoval(const Key & key)
{
    auto & bucket = getMetadataBucket(key);
    auto lock = bucket.lock();

    auto it = bucket.find(key);
    if (it == bucket.end())
        return;

    auto locked_key = it->second->lockNoStateCheck();
    if (locked_key->getKeyState() == KeyMetadata::KeyState::QUEUED_FOR_REMOVAL)
    {
        removeEmptyKey(bucket, it, *locked_key, lock);
    }
}

void CacheMetadata::runCleanupTask(const CleanupTask & task)
{
    if (!task.startup_leftover)
    {
        removeKeyIfStillQueuedForRemoval(task.key);
        return;
    }

    for (const auto & key : task.startup_leftover_keys)
    {
        try
        {
            removeEmptyKeyDirectoryFromStartup(task.key_prefix_path, key);
        }
        catch (...)
        {
            /// The other directories of this task must still be removed.
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

void CacheMetadata::runCleanupUntilEmpty()
{
    while (true)
    {
        try
        {
            auto task = cleanup_queue->tryPop();
            /// No task is waiting, so this helper thread is not needed anymore.
            if (!task)
                return;
            runCleanupTask(*task);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

void CacheMetadata::cleanupThreadFunc()
{
    /// Helper threads are started when cleanup tasks pile up; each exits once it finds none left.
    /// The queue is cancelled before this function returns, so joining them waits for one task at most,
    /// which can be all the empty key directories of one key prefix directory.
    /// This thread is one of `max_cleanup_threads`, and the pool needs room for one thread at least.
    const size_t max_helper_threads = std::max<size_t>(1, max_cleanup_threads - 1);
    ThreadPool helper_threads(
        CurrentMetrics::FilesystemCacheCleanupHelperThreads,
        CurrentMetrics::FilesystemCacheCleanupHelperThreadsActive,
        CurrentMetrics::FilesystemCacheCleanupHelperThreadsScheduled,
        /* max_threads */max_helper_threads,
        /* max_free_threads */0,
        /* queue_size */max_helper_threads);

    while (true)
    {
        try
        {
            auto task = cleanup_queue->waitAndPop();
            /// Only at shutdown: with an empty queue `waitAndPop` waits, and it returns nothing once cancelled.
            if (!task)
                return;
            runCleanupTask(*task);

            /// This thread counts as the first one.
            const size_t running_threads = helper_threads.active() + 1;
            if (running_threads < max_cleanup_threads
                && task->backlog >= running_threads * cleanup_elements_per_thread
                && helper_threads.trySchedule([this] { runCleanupUntilEmpty(); }))
            {
                LOG_DEBUG(log, "Started a helper cleanup thread: {} keys were waiting for cleanup", task->backlog);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    }
}

class DownloadQueue
{
friend class CacheMetadata;
public:
    explicit DownloadQueue(size_t queue_size_limit_) : queue_size_limit(queue_size_limit_) {}

    bool add(FileSegmentPtr file_segment)
    {
        {
            std::lock_guard lock(mutex);
            if (cancelled || (queue_size_limit && queue.size() >= queue_size_limit))
                return false;
            queue.push(DownloadInfo{file_segment->key(), file_segment->offset(), file_segment});
        }

        CurrentMetrics::add(CurrentMetrics::FilesystemCacheDownloadQueueElements);
        cv.notify_one();
        return true;
    }

    bool setQueueLimit(size_t size) { return queue_size_limit.exchange(size) != size; }

private:
    void cancel()
    {
        {
            std::lock_guard lock(mutex);
            cancelled = true;
        }
        cv.notify_all();
    }

    std::atomic<size_t> queue_size_limit;
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool cancelled = false;

    struct DownloadInfo
    {
        DownloadInfo(
            const CacheMetadata::Key & key_,
            const size_t & offset_,
            const std::weak_ptr<FileSegment> & file_segment_)
            : key(key_), offset(offset_), file_segment(file_segment_) {}

        CacheMetadata::Key key;
        size_t offset;
        /// We keep weak pointer to file segment
        /// instead of just getting it from file_segment_metadata,
        /// because file segment at key:offset count be removed and added back to metadata
        /// before we actually started background download.
        std::weak_ptr<FileSegment> file_segment;
    };

    std::queue<DownloadInfo> queue;
};

void CacheMetadata::downloadThreadFunc(const bool & stop_flag)
{
    std::optional<Memory<>> memory;
    while (true)
    {
        Key key;
        size_t offset = 0;
        std::weak_ptr<FileSegment> file_segment_weak;

        {
            std::unique_lock lock(download_queue->mutex);
            if (download_queue->cancelled || stop_flag)
                return;

            if (download_queue->queue.empty())
            {
                download_queue->cv.wait(lock, [&](){ return download_queue->cancelled || !download_queue->queue.empty() || stop_flag; });
                if (download_queue->cancelled || stop_flag)
                    return;
            }

            auto entry = download_queue->queue.front();
            key = entry.key;
            offset = entry.offset;
            file_segment_weak = entry.file_segment;

            download_queue->queue.pop();
        }

        CurrentMetrics::sub(CurrentMetrics::FilesystemCacheDownloadQueueElements);

        try
        {
            FileSegmentsHolderPtr holder;
            try
            {
                {
                    auto locked_key = lockKeyMetadata(key, KeyNotFoundPolicy::RETURN_NULL, FileCache::getInternalOrigin());
                    if (!locked_key)
                        continue;

                    auto file_segment_metadata = locked_key->tryGetByOffset(offset);
                    if (!file_segment_metadata || file_segment_metadata->isEvictingOrRemoved(*locked_key))
                        continue;

                    auto file_segment = file_segment_weak.lock();

                    if (!file_segment
                        || file_segment != file_segment_metadata->file_segment
                        || file_segment->state() != FileSegment::State::PARTIALLY_DOWNLOADED)
                        continue;

                    holder = std::make_unique<FileSegmentsHolder>(FileSegments{file_segment});
                }

                auto & file_segment = holder->front();

                if (file_segment.getOrSetDownloader() != FileSegment::getCallerId())
                    continue;

                chassert(file_segment.getDownloadedSize() != file_segment.range().size());
                chassert(file_segment.assertCorrectness());

                downloadImpl(file_segment, memory);
                holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
            }
            catch (...)
            {
                if (holder)
                {
                    auto & file_segment = holder->front();
                    file_segment.setDownloadFailed();

                    LOG_ERROR(
                        log, "Error during background download of {}:{} ({}): {}",
                        file_segment.key(), file_segment.offset(),
                        file_segment.getInfoForLog(), getCurrentExceptionMessage(true));
                }
                else
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                    chassert(false);
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
        }
    }
}

bool CacheMetadata::setBackgroundDownloadQueueSizeLimit(size_t size)
{
    return download_queue->setQueueLimit(size);
}

void CacheMetadata::downloadImpl(FileSegment & file_segment, std::optional<Memory<>> & memory) const
{
    LOG_TEST(
        log, "Downloading {} bytes for file segment {}",
        file_segment.range().size() - file_segment.getDownloadedSize(), file_segment.getInfoForLog());

    size_t size_to_download = file_segment.getSizeForBackgroundDownload();
    if (!size_to_download)
        return;

    auto buf = file_segment.getRemoteFileReader();
    if (!buf)
    {
        LOG_TEST(log, "No reader in {}:{} (state: {}, range: {}, downloaded size: {})",
                 file_segment.key(), file_segment.offset(), file_segment.state(),
                 file_segment.range().toString(), file_segment.getDownloadedSize());
        return;
    }

    chassert(buf->internalBuffer().empty(),
             fmt::format("Memory buffer for buffer must have been reset before "
             "being put into background download ({})", file_segment.getInfoForLog()));

    if (!memory)
        memory.emplace(std::min(size_t(DBMS_DEFAULT_BUFFER_SIZE), size_to_download));
    buf->set(memory->data(), std::min(size_to_download, memory->size()));

    const auto reserve_space_lock_wait_timeout_milliseconds =
        Context::getGlobalContextInstance()->getReadSettings().filesystem_cache_settings.reserve_space_wait_lock_timeout_milliseconds;

    size_t offset = file_segment.getCurrentWriteOffset();
    if (offset != static_cast<size_t>(buf->getPosition()))
        buf->seek(offset, SEEK_SET);

    FileCacheReserveAhead reserve_ahead;
    while (size_to_download && !buf->eof())
    {
        const auto available = buf->available();
        chassert(available);

        const auto size = std::min(available, size_to_download);
        size_to_download -= size;

        std::string failure_reason;
        /// Don't reserve ahead past this background pass.
        const size_t reserve_hint = size + size_to_download;
        if (!file_segment.reserve(
                size, reserve_space_lock_wait_timeout_milliseconds, failure_reason,
                /* reserve_stat */nullptr, reserve_hint, &reserve_ahead))
        {
            LOG_TEST(
                log, "Failed to reserve space during background download "
                "for {}:{} (downloaded size: {}/{})",
                file_segment.key(), file_segment.offset(),
                file_segment.getDownloadedSize(), file_segment.range().size());
            break;
        }

        try
        {
            file_segment.write(buf->position(), size, offset);
            offset += size;
            buf->position() += size;
        }
        catch (ErrnoException & e)
        {
            int code = e.getErrno();
            if (code == /* No space left on device */28 || code == /* Quota exceeded */122)
            {
                LOG_INFO(log, "Insert into cache is skipped due to insufficient disk space. ({})", e.displayText());
                break;
            }
            throw;
        }
    }

    /// Reset buffer to avoid
    /// Logical error: 'remote_fs_segment_reader->getFileOffsetOfBufferEnd() == file_segment.getCurrentWriteOffset()'
    file_segment.resetRemoteFileReader();
    file_segment.completePartAndResetDownloader();

    LOG_TEST(log, "Downloaded file segment: {}", file_segment.getInfoForLog());
}

void CacheMetadata::startup()
{
    download_threads.reserve(download_threads_num);
    for (size_t i = 0; i < download_threads_num; ++i)
    {
        download_threads.emplace_back(std::make_shared<DownloadThread>());
        download_threads.back()->thread = std::make_unique<ThreadFromGlobalPool>(
            [this, thread = download_threads.back()] { downloadThreadFunc(thread->stop_flag); });
    }
    cleanup_thread = std::make_unique<ThreadFromGlobalPool>([this]{ cleanupThreadFunc(); });
}

void CacheMetadata::shutdown()
{
    download_queue->cancel();
    cleanup_queue->cancel();

    for (auto & download_thread : download_threads)
    {
        if (download_thread->thread && download_thread->thread->joinable())
            download_thread->thread->join();
    }
    if (cleanup_thread && cleanup_thread->joinable())
        cleanup_thread->join();
}

bool CacheMetadata::isBackgroundDownloadEnabled()
{
    return download_threads_num;
}

bool CacheMetadata::setBackgroundDownloadThreads(size_t threads_num)
{
    if (threads_num == download_threads_num)
        return false;

    SCOPE_EXIT({ download_threads_num = download_threads.size(); });

    if (threads_num > download_threads_num)
    {
        size_t add_threads = threads_num - download_threads_num;
        for (size_t i = 0; i < add_threads; ++i)
        {
            download_threads.emplace_back(std::make_shared<DownloadThread>());
            try
            {
                download_threads.back()->thread = std::make_unique<ThreadFromGlobalPool>(
                    [this, thread = download_threads.back()] { downloadThreadFunc(thread->stop_flag); });
            }
            catch (...)
            {
                download_threads.pop_back();
                throw;
            }
        }
    }
    else if (threads_num < download_threads_num)
    {
        size_t remove_threads = download_threads_num - threads_num;

        {
            std::lock_guard lock(download_queue->mutex);
            for (size_t i = 0; i < remove_threads; ++i)
                download_threads[download_threads.size() - 1 - i]->stop_flag = true;
        }

        download_queue->cv.notify_all();

        for (size_t i = 0; i < remove_threads; ++i)
        {
            chassert(download_threads.back()->stop_flag);

            auto & thread = download_threads.back()->thread;
            if (thread && thread->joinable())
                thread->join();

            download_threads.pop_back();
        }
    }
    return true;
}

bool KeyMetadata::addToDownloadQueue(FileSegmentPtr file_segment)
{
    return cache_metadata->download_queue->add(file_segment);
}

void CacheMetadata::addEmptyKeyDirectoryFromStartup(const std::string & key_prefix_directory, const Key & key)
{
    fiu_do_on(FailPoints::file_cache_empty_key_directory_queue_memory_limit,
    {
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Failpoint: the queue of empty key directories is over the memory limit");
    });
    cleanup_queue->addEmptyKeyDirectoryFromStartup(key_prefix_directory, key);
}

void CacheMetadata::dropEmptyKeyDirectoriesFromStartup()
{
    cleanup_queue->dropEmptyKeyDirectoriesFromStartup();
}

void KeyMetadata::addToCleanupQueue()
{
    cache_metadata->cleanup_queue->add(key);
}

LockedKey::LockedKey(std::shared_ptr<KeyMetadata> key_metadata_)
    : key_metadata(key_metadata_)
    , lock(key_metadata->guard.lock())
{
}

LockedKey::~LockedKey()
{
    if (!key_metadata->empty() || getKeyState() != KeyMetadata::KeyState::ACTIVE)
        return;

    /// If state if ACTIVE and key turns out empty - we submit it for delayed removal.
    /// Because we do not want to always lock all cache metadata lock, when we remove files segments.
    /// but sometimes we do - we remove the empty key without delay - then key state
    /// will be REMOVED here and we will return in the check above.
    /// See comment near cleanupThreadFunc() for more details.

    key_metadata->key_state = KeyMetadata::KeyState::QUEUED_FOR_REMOVAL;
    LOG_TEST(key_metadata->logger(), "Submitting key {} for removal", getKey());
    key_metadata->addToCleanupQueue();
}

void LockedKey::removeFromCleanupQueue()
{
    if (key_metadata->key_state != KeyMetadata::KeyState::QUEUED_FOR_REMOVAL)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot cancel the removal of a key which is not queued for removal");

    /// Just mark key_state as "not to be removed", the cleanup thread will check it and skip the key.
    key_metadata->key_state = KeyMetadata::KeyState::ACTIVE;
}

void LockedKey::markAsRemoved()
{
    key_metadata->key_state = KeyMetadata::KeyState::REMOVED;
}

bool LockedKey::isLastOwnerOfFileSegment(size_t offset) const
{
    const auto file_segment_metadata = getByOffset(offset);
    return file_segment_metadata->file_segment.use_count() == 2;
}

bool LockedKey::removeAllFileSegments()
{
    bool removed_all = true;
    for (auto it = key_metadata->begin(); it != key_metadata->end();)
    {
        if (!it->second->releasable())
        {
            ++it;
            removed_all = false;
            continue;
        }
        if (it->second->isEvictingOrRemoved(*this))
        {
            /// File segment is currently a removal candidate,
            /// we do not know if it will be removed or not yet,
            /// but its size is currently accounted as potentially removed,
            /// so if we remove file segment now, we break the freeable_count
            /// calculation in tryReserve.
            ++it;
            removed_all = false;
            continue;
        }

        auto file_segment = it->second->file_segment;
        it = removeFileSegment(file_segment->offset(), file_segment->lock());
    }
    return removed_all;
}

KeyMetadata::iterator LockedKey::removeFileSegmentIfExists(size_t offset, bool can_be_broken, bool invalidate_queue_entry)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        return {};

    /// A segment still in use (e.g. being downloaded or read) must not be detached out from
    /// under its holder; only releasable segments can be dropped unless removal is forced.
    if (!can_be_broken && !it->second->releasable())
        return {};

    auto file_segment = it->second->file_segment;
    return removeFileSegmentImpl(it, file_segment->lock(), can_be_broken, invalidate_queue_entry);
}

KeyMetadata::iterator LockedKey::removeFileSegment(size_t offset, bool can_be_broken, bool invalidate_queue_entry)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no offset {}", offset);

    /// A segment still in use (e.g. being downloaded or read) must not be detached out from
    /// under its holder; only releasable segments can be dropped unless removal is forced.
    if (!can_be_broken && !it->second->releasable())
        return {};

    auto file_segment = it->second->file_segment;
    return removeFileSegmentImpl(it, file_segment->lock(), can_be_broken, invalidate_queue_entry);
}

KeyMetadata::iterator LockedKey::removeFileSegment(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    bool can_be_broken,
    bool invalidate_queue_entry)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no offset {} in key {}", offset, getKey());

    return removeFileSegmentImpl(it, segment_lock, can_be_broken, invalidate_queue_entry);
}

KeyMetadata::iterator LockedKey::removeFileSegmentImpl(
    KeyMetadata::iterator it,
    const FileSegmentGuard::Lock & segment_lock,
    bool can_be_broken,
    bool invalidate_queue_entry)
{
    auto file_segment = it->second->file_segment;

    LOG_TEST(
        key_metadata->logger(), "Remove from cache. Key: {}, offset: {}, size: {}",
        getKey(), file_segment->offset(), file_segment->reserved_size.load());

    chassert(can_be_broken || file_segment->assertCorrectnessUnlocked(segment_lock));

    if (file_segment->queue_iterator && invalidate_queue_entry)
        file_segment->queue_iterator->invalidate();

    try
    {
        file_segment->detach(segment_lock, *this);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        chassert(false);
        /// Do not rethrow, we must delete the file below.
    }

    try
    {
        const auto path = key_metadata->getFileSegmentPath(*file_segment);
        if (file_segment->downloaded_size == 0)
        {
            chassert(!fs::exists(path));
        }
        else if (fs::exists(path))
        {
            fs::remove(path);

            /// Clear OpenedFileCache to avoid reading from incorrect file descriptor.
            int flags = file_segment->getFlagsForLocalRead();
            /// Files are created with flags from file_segment->getFlagsForLocalRead()
            /// plus optionally O_DIRECT is added, depends on query setting, so remove both.
            OpenedFileCache::instance().remove(path, flags);
            OpenedFileCache::instance().remove(path, flags | O_DIRECT);

            LOG_TEST(key_metadata->logger(), "Removed file segment at path: {}", path);
        }
        else if (!can_be_broken)
        {
#ifdef DEBUG_OR_SANITIZER_BUILD
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected path {} to exist", path);
#else
            LOG_WARNING(key_metadata->logger(), "Expected path {} to exist, while removing {}:{}",
                        path, getKey(), file_segment->offset());
#endif
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        chassert(false);
    }

    return key_metadata->erase(it);
}

bool LockedKey::addToDownloadQueue(size_t offset, const FileSegmentGuard::Lock &)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return key_metadata->addToDownloadQueue(it->second->file_segment);
}

std::optional<FileSegment::Range> LockedKey::hasIntersectingRange(const FileSegment::Range & range) const
{
    if (key_metadata->empty())
        return {};

    auto it = key_metadata->lower_bound(range.left);
    if (it != key_metadata->end()) /// has next range
    {
        auto next_range = it->second->file_segment->range();
        if (!(range < next_range))
            return next_range;

        if (it == key_metadata->begin())
            return {};
    }

    auto prev_range = std::prev(it)->second->file_segment->range();
    if (!(prev_range < range))
        return prev_range;

    return {};
}

std::shared_ptr<const FileSegmentMetadata> LockedKey::getByOffset(size_t offset) const
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return it->second;
}

std::shared_ptr<FileSegmentMetadata> LockedKey::getByOffset(size_t offset)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return it->second;
}

std::shared_ptr<const FileSegmentMetadata> LockedKey::tryGetByOffset(size_t offset) const
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        return nullptr;
    return it->second;
}

std::shared_ptr<FileSegmentMetadata> LockedKey::tryGetByOffset(size_t offset)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        return nullptr;
    return it->second;
}

std::string LockedKey::toString() const
{
    std::string result;
    for (auto it = key_metadata->begin(); it != key_metadata->end(); ++it)
    {
        if (it != key_metadata->begin())
            result += ", ";
        result += std::to_string(it->first);
    }
    return result;
}


std::vector<FileSegment::Info> LockedKey::sync()
{
    std::vector<FileSegment::Info> broken;
    for (auto it = key_metadata->begin(); it != key_metadata->end();)
    {
        if (it->second->isEvictingOrRemoved(*this) || !it->second->releasable())
        {
            ++it;
            continue;
        }

        auto file_segment = it->second->file_segment;
        if (file_segment->isDetached())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "File segment has unexpected state: DETACHED ({})", file_segment->getInfoForLog());
        }

        if (file_segment->getDownloadedSize() == 0)
        {
            ++it;
            continue;
        }

        const auto & path = key_metadata->getFileSegmentPath(*file_segment);
        if (!fs::exists(path))
        {
            LOG_WARNING(
                key_metadata->logger(),
                "File segment has DOWNLOADED state, but file does not exist ({})",
                file_segment->getInfoForLog());

            broken.push_back(FileSegment::getInfo(file_segment));
            it = removeFileSegment(file_segment->offset(), file_segment->lock(), /* can_be_broken */true);
            continue;
        }

        const size_t actual_size = fs::file_size(path);
        const size_t expected_size = file_segment->getDownloadedSize();

        if (actual_size == expected_size)
        {
            ++it;
            continue;
        }

        LOG_WARNING(
            key_metadata->logger(),
            "File segment has unexpected size. Having {}, expected {} ({})",
            actual_size, expected_size, file_segment->getInfoForLog());

        broken.push_back(FileSegment::getInfo(file_segment));
        it = removeFileSegment(file_segment->offset(), file_segment->lock(), /* can_be_broken */true);
    }
    return broken;
}

}
