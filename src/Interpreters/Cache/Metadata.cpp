#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheDownloadQueueElements;
    extern const Metric FilesystemCacheDelayedCleanupElements;
}

namespace ProfileEvents
{
    extern const Event FilesystemCacheLockKeyMicroseconds;
    extern const Event FilesystemCacheLockMetadataMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int FILECACHE_ACCESS_DENIED;
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
    return file_segment->getReservedSize();
}

KeyMetadata::KeyMetadata(
    const Key & key_,
    const UserInfo & user_,
    const CacheMetadata * cache_metadata_,
    bool created_base_directory_)
    : key(key_)
    , user(user_)
    , cache_metadata(cache_metadata_)
    , created_base_directory(created_base_directory_)
{
    if (user_ == FileCache::getInternalUser())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create key metadata with internal user id");

    if (!user_.weight.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create key metadata without user weight");

    chassert(!created_base_directory || fs::exists(getPath()));
}

bool KeyMetadata::checkAccess(const UserID & user_id_) const
{
    return user_id_ == user.user_id || user_id_ == FileCache::getInternalUser().user_id;
}

void KeyMetadata::assertAccess(const UserID & user_id_) const
{
    if (!checkAccess(user_id_))
    {
        throw Exception(ErrorCodes::FILECACHE_ACCESS_DENIED,
                        "Metadata for key {} belongs to user {}, but user {} requested it",
                        key.toString(), user.user_id, user_id_);
    }
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
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockKeyMicroseconds);
    return std::make_unique<LockedKey>(shared_from_this());
}

bool KeyMetadata::createBaseDirectory(bool throw_if_failed)
{
    if (!created_base_directory.exchange(true))
    {
        try
        {
            std::shared_lock lock(cache_metadata->key_prefix_directory_mutex);
            fs::create_directories(getPath());
        }
        catch (const fs::filesystem_error & e)
        {
            created_base_directory = false;

            if (!throw_if_failed &&
                (e.code() == std::errc::no_space_on_device
                 || e.code() == std::errc::read_only_file_system
                 || e.code() == std::errc::permission_denied
                 || e.code() == std::errc::too_many_files_open
                 || e.code() == std::errc::operation_not_permitted))
            {
                LOG_TRACE(cache_metadata->log, "Failed to create base directory for key {}, "
                          "because no space left on device", key);

                return false;
            }
            throw;
        }
    }
    return true;
}

std::string KeyMetadata::getPath() const
{
    return cache_metadata->getKeyPath(key, user);
}

std::string KeyMetadata::getFileSegmentPath(const FileSegment & file_segment) const
{
    return cache_metadata->getFileSegmentPath(key, file_segment.offset(), file_segment.getKind(), user);
}

LoggerPtr KeyMetadata::logger() const
{
    return cache_metadata->log;
}

CacheMetadata::CacheMetadata(
    const std::string & path_,
    size_t background_download_queue_size_limit_,
    size_t background_download_threads_,
    bool write_cache_per_user_directory_)
    : path(path_)
    , cleanup_queue(std::make_shared<CleanupQueue>())
    , download_queue(std::make_shared<DownloadQueue>(background_download_queue_size_limit_))
    , write_cache_per_user_directory(write_cache_per_user_directory_)
    , log(getLogger("CacheMetadata"))
    , download_threads_num(background_download_threads_)
{
}

String CacheMetadata::getFileNameForFileSegment(size_t offset, FileSegmentKind segment_kind)
{
    String file_suffix;
    switch (segment_kind)
    {
        case FileSegmentKind::Ephemeral:
            file_suffix = "_temporary";
            break;
        case FileSegmentKind::Regular:
            break;
    }
    return std::to_string(offset) + file_suffix;
}

String CacheMetadata::getFileSegmentPath(
    const Key & key,
    size_t offset,
    FileSegmentKind segment_kind,
    const UserInfo & user) const
{
    return fs::path(getKeyPath(key, user)) / getFileNameForFileSegment(offset, segment_kind);
}

String CacheMetadata::getKeyPath(const Key & key, const UserInfo & user) const
{
    const auto key_str = key.toString();
    if (write_cache_per_user_directory)
        return fs::path(path) / fmt::format("{}.{}", user.user_id, user.weight.value()) / key_str.substr(0, 3) / key_str;

    return fs::path(path) / key_str.substr(0, 3) / key_str;
}

CacheMetadataGuard::Lock CacheMetadata::MetadataBucket::lock() const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockMetadataMicroseconds);
    return guard.lock();
}

CacheMetadata::MetadataBucket & CacheMetadata::getMetadataBucket(const Key & key)
{
    const auto bucket = key.key % buckets_num;
    return metadata_buckets[bucket];
}

LockedKeyPtr CacheMetadata::lockKeyMetadata(
    const FileCacheKey & key,
    KeyNotFoundPolicy key_not_found_policy,
    const UserInfo & user,
    bool is_initial_load)
{
    auto key_metadata = getKeyMetadata(key, key_not_found_policy, user, is_initial_load);
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

        if (key_state == KeyMetadata::KeyState::REMOVING)
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
    return lockKeyMetadata(key, key_not_found_policy, user);
}

KeyMetadataPtr CacheMetadata::getKeyMetadata(
    const Key & key,
    KeyNotFoundPolicy key_not_found_policy,
    const UserInfo & user,
    bool is_initial_load)
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
            key, std::make_shared<KeyMetadata>(key, user, this, is_initial_load)).first;
    }

    it->second->assertAccess(user.user_id);
    return it->second;
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
            if (key_state == KeyMetadata::KeyState::REMOVING)
                continue;

            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Cannot lock key {}: key does not exist", key_metadata->key);
        }
    }
}

void CacheMetadata::removeAllKeys(bool if_releasable, const UserID & user_id)
{
    for (auto & bucket : metadata_buckets)
    {
        auto lock = bucket.lock();
        for (auto it = bucket.begin(); it != bucket.end();)
        {
            if (!it->second->checkAccess(user_id))
                continue;

            auto locked_key = it->second->lockNoStateCheck();
            if (locked_key->getKeyState() == KeyMetadata::KeyState::ACTIVE)
            {
                bool removed_all = locked_key->removeAllFileSegments(if_releasable);
                if (removed_all)
                {
                    it = removeEmptyKey(bucket, it, *locked_key, lock);
                    continue;
                }
            }
            ++it;
        }
    }
}

void CacheMetadata::removeKey(const Key & key, bool if_exists, bool if_releasable, const UserID & user_id)
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

    bool removed_all = locked_key->removeAllFileSegments(if_releasable);
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

    LOG_DEBUG(log, "Key {} is removed from metadata", key);

    const fs::path key_directory = getKeyPath(key, locked_key.getKeyMetadata()->user);
    const fs::path key_prefix_directory = key_directory.parent_path();

    try
    {
        if (fs::exists(key_directory))
        {
            fs::remove_all(key_directory);
            LOG_TEST(log, "Directory ({}) for key {} removed", key_directory.string(), key);
        }
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
        if (fs::exists(key_prefix_directory) && fs::is_empty(key_prefix_directory))
        {
            fs::remove(key_prefix_directory);
            LOG_TEST(log, "Prefix directory ({}) for key {} removed", key_prefix_directory.string(), key);
        }

        /// TODO: Remove empty user directories.
    }
    catch (...)
    {
        LOG_ERROR(log, "Error while removing key {}: {}", key, getCurrentExceptionMessage(true));
        chassert(false);
    }
    return next_it;
}

class CleanupQueue
{
    friend class CacheMetadata;
public:
    void add(const FileCacheKey & key)
    {
        bool inserted;
        {
            std::lock_guard lock(mutex);
            if (cancelled)
                return;
            inserted = keys.insert(key).second;
        }
        if (inserted)
        {
            CurrentMetrics::add(CurrentMetrics::FilesystemCacheDelayedCleanupElements);
            cv.notify_one();
        }
    }

    void cancel()
    {
        {
            std::lock_guard lock(mutex);
            cancelled = true;
        }
        cv.notify_all();
    }

private:
    std::unordered_set<FileCacheKey> keys;
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool cancelled = false;
};

void CacheMetadata::cleanupThreadFunc()
{
    while (true)
    {
        Key key;
        {
            std::unique_lock lock(cleanup_queue->mutex);
            if (cleanup_queue->cancelled)
                return;

            auto & keys = cleanup_queue->keys;
            if (keys.empty())
            {
                cleanup_queue->cv.wait(lock, [&](){ return cleanup_queue->cancelled || !keys.empty(); });
                if (cleanup_queue->cancelled)
                    return;
            }

            auto it = keys.begin();
            key = *it;
            keys.erase(it);
        }

        CurrentMetrics::sub(CurrentMetrics::FilesystemCacheDelayedCleanupElements);

        try
        {

            auto & bucket = getMetadataBucket(key);
            auto lock = bucket.lock();

            auto it = bucket.find(key);
            if (it == bucket.end())
                continue;

            auto locked_key = it->second->lockNoStateCheck();
            if (locked_key->getKeyState() == KeyMetadata::KeyState::REMOVING)
            {
                removeEmptyKey(bucket, it, *locked_key, lock);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
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
        size_t offset;
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
                    auto locked_key = lockKeyMetadata(key, KeyNotFoundPolicy::RETURN_NULL, FileCache::getInternalUser());
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

    auto reader = file_segment.getRemoteFileReader();
    if (!reader)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "No reader. "
            "File segment should not have been submitted for background download ({})",
            file_segment.getInfoForLog());
    }

    /// If remote_fs_read_method == 'threadpool',
    /// reader itself never owns/allocates the buffer.
    if (reader->internalBuffer().empty())
    {
        if (!memory)
            memory.emplace(DBMS_DEFAULT_BUFFER_SIZE);
        reader->set(memory->data(), memory->size());
    }

    const auto reserve_space_lock_wait_timeout_milliseconds =
        Context::getGlobalContextInstance()->getReadSettings().filesystem_cache_reserve_space_wait_lock_timeout_milliseconds;

    size_t offset = file_segment.getCurrentWriteOffset();
    if (offset != static_cast<size_t>(reader->getPosition()))
        reader->seek(offset, SEEK_SET);

    while (!reader->eof())
    {
        auto size = reader->available();

        std::string failure_reason;
        if (!file_segment.reserve(size, reserve_space_lock_wait_timeout_milliseconds, failure_reason))
        {
            LOG_TEST(
                log, "Failed to reserve space during background download "
                "for {}:{} (downloaded size: {}/{})",
                file_segment.key(), file_segment.offset(),
                file_segment.getDownloadedSize(), file_segment.range().size());
            return;
        }

        try
        {
            file_segment.write(reader->position(), size, offset);
            offset += size;
            reader->position() += size;
        }
        catch (ErrnoException & e)
        {
            int code = e.getErrno();
            if (code == /* No space left on device */28 || code == /* Quota exceeded */122)
            {
                LOG_INFO(log, "Insert into cache is skipped due to insufficient disk space. ({})", e.displayText());
                return;
            }
            throw;
        }
    }

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

    key_metadata->key_state = KeyMetadata::KeyState::REMOVING;
    LOG_TEST(key_metadata->logger(), "Submitting key {} for removal", getKey());
    key_metadata->addToCleanupQueue();
}

void LockedKey::removeFromCleanupQueue()
{
    if (key_metadata->key_state != KeyMetadata::KeyState::REMOVING)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove non-removing");

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

bool LockedKey::removeAllFileSegments(bool if_releasable)
{
    bool removed_all = true;
    for (auto it = key_metadata->begin(); it != key_metadata->end();)
    {
        if (if_releasable && !it->second->releasable())
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

KeyMetadata::iterator LockedKey::removeFileSegment(size_t offset, bool can_be_broken, bool invalidate_queue_entry)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no offset {}", offset);

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
        getKey(), file_segment->offset(), file_segment->reserved_size);

    chassert(can_be_broken || file_segment->assertCorrectnessUnlocked(segment_lock));

    if (file_segment->queue_iterator && invalidate_queue_entry)
        file_segment->queue_iterator->invalidate();

    file_segment->detach(segment_lock, *this);

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

void LockedKey::shrinkFileSegmentToDownloadedSize(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock)
{
    /**
     * In case file was partially downloaded and it's download cannot be continued
     * because of no space left in cache, we need to be able to cut file segment's size to downloaded_size.
     */

    auto metadata = getByOffset(offset);
    const auto & file_segment = metadata->file_segment;
    chassert(file_segment->assertCorrectnessUnlocked(segment_lock));

    const size_t downloaded_size = file_segment->getDownloadedSize();
    if (downloaded_size == file_segment->range().size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Nothing to reduce, file segment fully downloaded: {}",
            file_segment->getInfoForLogUnlocked(segment_lock));
    }

    chassert(file_segment->reserved_size >= downloaded_size);
    int64_t diff = file_segment->reserved_size - downloaded_size;

    metadata->file_segment = std::make_shared<FileSegment>(
        getKey(), offset, downloaded_size, FileSegment::State::DOWNLOADED,
        CreateFileSegmentSettings(file_segment->getKind()), false,
        file_segment->cache, key_metadata, file_segment->queue_iterator);

    if (diff)
        metadata->getQueueIterator()->decrementSize(diff);

    chassert(file_segment->assertCorrectnessUnlocked(segment_lock));
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
        it = removeFileSegment(file_segment->offset(), file_segment->lock(), /* can_be_broken */false);
    }
    return broken;
}

}
