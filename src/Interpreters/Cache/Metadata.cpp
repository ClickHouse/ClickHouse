#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
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
    const std::string & key_path_,
    CleanupQueuePtr cleanup_queue_,
    DownloadQueuePtr download_queue_,
    Poco::Logger * log_,
    std::shared_mutex & key_prefix_directory_mutex_,
    bool created_base_directory_)
    : key(key_)
    , key_path(key_path_)
    , cleanup_queue(cleanup_queue_)
    , download_queue(download_queue_)
    , key_prefix_directory_mutex(key_prefix_directory_mutex_)
    , created_base_directory(created_base_directory_)
    , log(log_)
{
    if (created_base_directory)
        chassert(fs::exists(key_path));
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

bool KeyMetadata::createBaseDirectory()
{
    if (!created_base_directory.exchange(true))
    {
        try
        {
            std::shared_lock lock(key_prefix_directory_mutex);
            fs::create_directories(key_path);
        }
        catch (const fs::filesystem_error & e)
        {
            created_base_directory = false;

            if (e.code() == std::errc::no_space_on_device)
            {
                LOG_TRACE(log, "Failed to create base directory for key {}, "
                          "because no space left on device", key);

                return false;
            }
            throw;
        }
    }
    return true;
}

std::string KeyMetadata::getFileSegmentPath(const FileSegment & file_segment) const
{
    return fs::path(key_path)
        / CacheMetadata::getFileNameForFileSegment(file_segment.offset(), file_segment.getKind());
}


CacheMetadata::CacheMetadata(const std::string & path_)
    : path(path_)
    , cleanup_queue(std::make_shared<CleanupQueue>())
    , download_queue(std::make_shared<DownloadQueue>())
    , log(&Poco::Logger::get("CacheMetadata"))
{
}

String CacheMetadata::getFileNameForFileSegment(size_t offset, FileSegmentKind segment_kind)
{
    String file_suffix;
    switch (segment_kind)
    {
        case FileSegmentKind::Temporary:
            file_suffix = "_temporary";
            break;
        case FileSegmentKind::Regular:
            break;
    }
    return std::to_string(offset) + file_suffix;
}

String CacheMetadata::getPathForFileSegment(const Key & key, size_t offset, FileSegmentKind segment_kind) const
{
    return fs::path(getPathForKey(key)) / getFileNameForFileSegment(offset, segment_kind);
}

String CacheMetadata::getPathForKey(const Key & key) const
{
    const auto key_str = key.toString();
    return fs::path(path) / key_str.substr(0, 3) / key_str;
}

CacheMetadataGuard::Lock CacheMetadata::lockMetadata() const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockMetadataMicroseconds);
    return guard.lock();
}

LockedKeyPtr CacheMetadata::lockKeyMetadata(
    const FileCacheKey & key,
    KeyNotFoundPolicy key_not_found_policy,
    bool is_initial_load)
{
    auto key_metadata = getKeyMetadata(key, key_not_found_policy, is_initial_load);
    if (!key_metadata)
        return nullptr;

    {
        auto locked_metadata = key_metadata->lockNoStateCheck();
        const auto key_state = locked_metadata->getKeyState();

        if (key_state == KeyMetadata::KeyState::ACTIVE)
            return locked_metadata;

        if (key_not_found_policy == KeyNotFoundPolicy::THROW)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key `{}` in cache", key);
        else if (key_not_found_policy == KeyNotFoundPolicy::THROW_LOGICAL)
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
    return lockKeyMetadata(key, key_not_found_policy);
}

KeyMetadataPtr CacheMetadata::getKeyMetadata(
    const Key & key,
    KeyNotFoundPolicy key_not_found_policy,
    bool is_initial_load)
{
    auto lock = lockMetadata();

    auto it = find(key);
    if (it == end())
    {
        if (key_not_found_policy == KeyNotFoundPolicy::THROW)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key `{}` in cache", key);
        else if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL)
            return nullptr;

        it = emplace(
            key, std::make_shared<KeyMetadata>(
                key, getPathForKey(key), cleanup_queue, download_queue, log, key_prefix_directory_mutex, is_initial_load)).first;
    }

    return it->second;
}

void CacheMetadata::iterate(IterateFunc && func)
{
    auto lock = lockMetadata();
    for (auto & [key, key_metadata] : *this)
    {
        auto locked_key = key_metadata->lockNoStateCheck();
        const auto key_state = locked_key->getKeyState();

        if (key_state == KeyMetadata::KeyState::ACTIVE)
        {
            func(*locked_key);
            continue;
        }
        else if (key_state == KeyMetadata::KeyState::REMOVING)
            continue;

        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Cannot lock key {}: key does not exist", key_metadata->key);
    }
}

void CacheMetadata::removeAllKeys(bool if_releasable)
{
    auto lock = lockMetadata();
    for (auto it = begin(); it != end();)
    {
        auto locked_key = it->second->lockNoStateCheck();
        if (locked_key->getKeyState() == KeyMetadata::KeyState::ACTIVE)
        {
            bool removed_all = locked_key->removeAllFileSegments(if_releasable);
            if (removed_all)
            {
                it = removeEmptyKey(it, *locked_key, lock);
                continue;
            }
        }
        ++it;
    }
}

void CacheMetadata::removeKey(const Key & key, bool if_exists, bool if_releasable)
{
    auto metadata_lock = lockMetadata();

    auto it = find(key);
    if (it == end())
    {
        if (if_exists)
            return;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key: {}", key);
    }

    auto locked_key = it->second->lockNoStateCheck();
    auto state = locked_key->getKeyState();
    if (state != KeyMetadata::KeyState::ACTIVE)
    {
        if (if_exists)
            return;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such key: {} (state: {})", key, magic_enum::enum_name(state));
    }

    bool removed_all = locked_key->removeAllFileSegments(if_releasable);
    if (removed_all)
        removeEmptyKey(it, *locked_key, metadata_lock);
}

CacheMetadata::iterator CacheMetadata::removeEmptyKey(iterator it, LockedKey & locked_key, const CacheMetadataGuard::Lock &)
{
    const auto & key = locked_key.getKey();

    if (!it->second->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove non-empty key: {}", key);

    locked_key.markAsRemoved();
    auto next_it = erase(it);

    LOG_DEBUG(log, "Key {} is removed from metadata", key);

    const fs::path key_directory = getPathForKey(key);
    const fs::path key_prefix_directory = key_directory.parent_path();

    try
    {
        if (fs::exists(key_directory))
            fs::remove_all(key_directory);
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
            fs::remove(key_prefix_directory);
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
    friend struct CacheMetadata;
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
        /// There is an invariant that key cannot be submitted for removal if it is already in removal queue.
        /// Because
        /// 1) when submit key to removal it acquires state REMOVING and we submit key for removal only if it has ACTIVE state.
        /// 2) if a key is added to cache and it was found in removal queue - it will be removed from the queue and get state ACTIVE.
        /// and both these actions are synchronized by the same KeyGuard.
        chassert(inserted);
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
            auto lock = lockMetadata();

            auto it = find(key);
            if (it == end())
                continue;

            auto locked_key = it->second->lockNoStateCheck();
            if (locked_key->getKeyState() == KeyMetadata::KeyState::REMOVING)
            {
                removeEmptyKey(it, *locked_key, lock);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void CacheMetadata::cancelCleanup()
{
    cleanup_queue->cancel();
}

class DownloadQueue
{
friend struct CacheMetadata;
public:
    void add(FileSegmentPtr file_segment)
    {
        {
            std::lock_guard lock(mutex);
            if (cancelled)
                return;
            queue.push(DownloadInfo{file_segment->key(), file_segment->offset(), file_segment});
        }

        CurrentMetrics::add(CurrentMetrics::FilesystemCacheDownloadQueueElements);
        cv.notify_one();
    }

private:
    void cancel()
    {
        {
            std::lock_guard lock(mutex);
            cancelled = true;
        }
        cv.notify_all();
    }

    std::mutex mutex;
    std::condition_variable cv;
    bool cancelled = false;

    struct DownloadInfo
    {
        DownloadInfo(const CacheMetadata::Key & key_, const size_t & offset_, const std::weak_ptr<FileSegment> & file_segment_)
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

void CacheMetadata::downloadThreadFunc()
{
    std::optional<Memory<>> memory;
    while (true)
    {
        Key key;
        size_t offset;
        std::weak_ptr<FileSegment> file_segment_weak;

        {
            std::unique_lock lock(download_queue->mutex);
            if (download_queue->cancelled)
                return;

            if (download_queue->queue.empty())
            {
                download_queue->cv.wait(lock, [&](){ return download_queue->cancelled || !download_queue->queue.empty(); });
                if (download_queue->cancelled)
                    return;
            }

            auto entry = download_queue->queue.front();
            key = entry.key;
            offset = entry.offset;
            file_segment_weak = entry.file_segment;

            download_queue->queue.pop();
        }

        CurrentMetrics::sub(CurrentMetrics::FilesystemCacheDownloadQueueElements);

        FileSegmentsHolderPtr holder;
        try
        {
            {
                auto locked_key = lockKeyMetadata(key, KeyNotFoundPolicy::RETURN_NULL);
                if (!locked_key)
                    continue;

                auto file_segment_metadata = locked_key->tryGetByOffset(offset);
                if (!file_segment_metadata || file_segment_metadata->evicting())
                    continue;

                auto file_segment = file_segment_weak.lock();

                if (!file_segment
                    || file_segment != file_segment_metadata->file_segment
                    || file_segment->state() != FileSegment::State::PARTIALLY_DOWNLOADED)
                    continue;

                holder = std::make_unique<FileSegmentsHolder>(FileSegments{file_segment});
            }

            downloadImpl(holder->front(), memory);
        }
        catch (...)
        {
            if (holder)
            {
                const auto & file_segment = holder->front();
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
}

void CacheMetadata::downloadImpl(FileSegment & file_segment, std::optional<Memory<>> & memory)
{
    chassert(file_segment.assertCorrectness());

    if (file_segment.getOrSetDownloader() != FileSegment::getCallerId())
        return;

    if (file_segment.getDownloadedSize() == file_segment.range().size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File segment is already fully downloaded");

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

    size_t offset = file_segment.getCurrentWriteOffset();
    if (offset != static_cast<size_t>(reader->getPosition()))
        reader->seek(offset, SEEK_SET);

    while (!reader->eof())
    {
        auto size = reader->available();

        if (!file_segment.reserve(size))
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

void CacheMetadata::cancelDownload()
{
    download_queue->cancel();
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
    LOG_DEBUG(key_metadata->log, "Submitting key {} for removal", getKey());
    key_metadata->cleanup_queue->add(getKey());
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
        else if (it->second->evicting())
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

KeyMetadata::iterator LockedKey::removeFileSegment(size_t offset, bool can_be_broken)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no offset {}", offset);

    auto file_segment = it->second->file_segment;
    return removeFileSegmentImpl(it, file_segment->lock(), can_be_broken);
}

KeyMetadata::iterator LockedKey::removeFileSegment(size_t offset, const FileSegmentGuard::Lock & segment_lock, bool can_be_broken)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no offset {} in key {}", offset, getKey());

    return removeFileSegmentImpl(it, segment_lock, can_be_broken);
}

KeyMetadata::iterator LockedKey::removeFileSegmentImpl(KeyMetadata::iterator it, const FileSegmentGuard::Lock & segment_lock, bool can_be_broken)
{
    auto file_segment = it->second->file_segment;

    LOG_DEBUG(
        key_metadata->log, "Remove from cache. Key: {}, offset: {}, size: {}",
        getKey(), file_segment->offset(), file_segment->reserved_size);

    chassert(can_be_broken || file_segment->assertCorrectnessUnlocked(segment_lock));

    if (file_segment->queue_iterator)
        file_segment->queue_iterator->invalidate();

    file_segment->detach(segment_lock, *this);

    try
    {
        const auto path = key_metadata->getFileSegmentPath(*file_segment);
        bool exists = fs::exists(path);
        if (exists)
        {
            fs::remove(path);

            /// Clear OpenedFileCache to avoid reading from incorrect file descriptor.
            int flags = file_segment->getFlagsForLocalRead();
            /// Files are created with flags from file_segment->getFlagsForLocalRead()
            /// plus optionally O_DIRECT is added, depends on query setting, so remove both.
            OpenedFileCache::instance().remove(path, flags);
            OpenedFileCache::instance().remove(path, flags | O_DIRECT);

            LOG_TEST(key_metadata->log, "Removed file segment at path: {}", path);
        }
        else if (file_segment->downloaded_size && !can_be_broken)
        {
#ifdef ABORT_ON_LOGICAL_ERROR
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected path {} to exist", path);
#else
            LOG_WARNING(key_metadata->log, "Expected path {} to exist, while removing {}:{}",
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
        CreateFileSegmentSettings(file_segment->getKind()),
        file_segment->cache, key_metadata, file_segment->queue_iterator);

    if (diff)
        metadata->getQueueIterator()->updateSize(-diff);

    chassert(file_segment->assertCorrectnessUnlocked(segment_lock));
}

void LockedKey::addToDownloadQueue(size_t offset, const FileSegmentGuard::Lock &)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    key_metadata->download_queue->add(it->second->file_segment);
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

FileSegments LockedKey::sync()
{
    FileSegments broken;
    for (auto it = key_metadata->begin(); it != key_metadata->end();)
    {
        if (it->second->evicting() || !it->second->releasable())
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
                key_metadata->log,
                "File segment has DOWNLOADED state, but file does not exist ({})",
                file_segment->getInfoForLog());

            broken.push_back(FileSegment::getSnapshot(file_segment));
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
            key_metadata->log,
            "File segment has unexpected size. Having {}, expected {} ({})",
            actual_size, expected_size, file_segment->getInfoForLog());

        broken.push_back(FileSegment::getSnapshot(file_segment));
        it = removeFileSegment(file_segment->offset(), file_segment->lock(), /* can_be_broken */false);
    }
    return broken;
}

}
