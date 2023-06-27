#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Common/logger_useful.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <filesystem>

namespace fs = std::filesystem;

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
    CleanupQueue & cleanup_queue_,
    Poco::Logger * log_,
    bool created_base_directory_)
    : key(key_)
    , key_path(key_path_)
    , cleanup_queue(cleanup_queue_)
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
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockKeyMicroseconds);

    auto locked = std::make_unique<LockedKey>(shared_from_this());
    if (key_state == KeyMetadata::KeyState::ACTIVE)
        return locked;

    return nullptr;
}

bool KeyMetadata::createBaseDirectory()
{
    if (!created_base_directory.exchange(true))
    {
        try
        {
            fs::create_directories(key_path);
        }
        catch (...)
        {
            /// Avoid errors like
            /// std::__1::__fs::filesystem::filesystem_error: filesystem error: in create_directories: No space left on device
            /// and mark file segment with SKIP_CACHE state
            tryLogCurrentException(__PRETTY_FUNCTION__);
            created_base_directory = false;
            return false;
        }
    }
    return true;
}

std::string KeyMetadata::getFileSegmentPath(const FileSegment & file_segment)
{
    return fs::path(key_path)
        / CacheMetadata::getFileNameForFileSegment(file_segment.offset(), file_segment.getKind());
}


class CleanupQueue
{
    friend struct CacheMetadata;
public:
    void add(const FileCacheKey & key);
    void remove(const FileCacheKey & key);
    size_t getSize() const;

private:
    bool tryPop(FileCacheKey & key);

    std::unordered_set<FileCacheKey> keys;
    mutable std::mutex mutex;
};


CacheMetadata::CacheMetadata(const std::string & path_)
    : path(path_)
    , cleanup_queue(std::make_unique<CleanupQueue>())
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
            file_suffix = "";
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
    KeyMetadataPtr key_metadata;
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
                    key, getPathForKey(key), *cleanup_queue, log, is_initial_load)).first;
        }

        key_metadata = it->second;
    }

    {
        LockedKeyPtr locked_metadata;
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockKeyMicroseconds);
            locked_metadata = std::make_unique<LockedKey>(key_metadata);
        }

        const auto key_state = locked_metadata->getKeyState();
        if (key_state == KeyMetadata::KeyState::ACTIVE)
            return locked_metadata;

        if (key_not_found_policy == KeyNotFoundPolicy::THROW)
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

void CacheMetadata::iterate(IterateCacheMetadataFunc && func)
{
    auto lock = lockMetadata();
    for (const auto & [key, key_metadata] : *this)
    {
        LockedKeyPtr locked_key;
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockKeyMicroseconds);
            locked_key = std::make_unique<LockedKey>(key_metadata);
        }

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

void CacheMetadata::doCleanup()
{
    auto lock = lockMetadata();

    FileCacheKey cleanup_key;
    while (cleanup_queue->tryPop(cleanup_key))
    {
        auto it = find(cleanup_key);
        if (it == end())
            continue;

        LockedKeyPtr locked_metadata;
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheLockKeyMicroseconds);
            locked_metadata = std::make_unique<LockedKey>(it->second);
        }

        const auto key_state = locked_metadata->getKeyState();
        if (key_state == KeyMetadata::KeyState::ACTIVE)
        {
            /// Key was added back to cache after we submitted it to removal queue.
            continue;
        }

        locked_metadata->markAsRemoved();
        erase(it);
        LOG_DEBUG(log, "Key {} is removed from metadata", cleanup_key);

        const fs::path key_directory = getPathForKey(cleanup_key);
        const fs::path key_prefix_directory = key_directory.parent_path();

        try
        {
            if (fs::exists(key_directory))
                fs::remove_all(key_directory);
        }
        catch (...)
        {
            LOG_ERROR(log, "Error while removing key {}: {}", cleanup_key, getCurrentExceptionMessage(true));
            chassert(false);
            continue;
        }

        try
        {
            if (fs::exists(key_prefix_directory) && fs::is_empty(key_prefix_directory))
                fs::remove(key_prefix_directory);
        }
        catch (const fs::filesystem_error & e)
        {
            /// Key prefix directory can become non-empty just now, it is expected.
            if (e.code() == std::errc::directory_not_empty)
                continue;
            LOG_ERROR(log, "Error while removing key {}: {}", cleanup_key, getCurrentExceptionMessage(true));
            chassert(false);
        }
        catch (...)
        {
            LOG_ERROR(log, "Error while removing key {}: {}", cleanup_key, getCurrentExceptionMessage(true));
            chassert(false);
        }
    }
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

    key_metadata->key_state = KeyMetadata::KeyState::REMOVING;
    LOG_DEBUG(key_metadata->log, "Submitting key {} for removal", getKey());
    key_metadata->cleanup_queue.add(getKey());
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

void LockedKey::removeAllReleasable()
{
    for (auto it = key_metadata->begin(); it != key_metadata->end();)
    {
        if (!it->second->releasable())
        {
            ++it;
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
            continue;
        }

        auto file_segment = it->second->file_segment;
        it = removeFileSegment(file_segment->offset(), file_segment->lock());
    }
}

KeyMetadata::iterator LockedKey::removeFileSegment(size_t offset, const FileSegmentGuard::Lock & segment_lock)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no offset {}", offset);

    auto file_segment = it->second->file_segment;

    LOG_DEBUG(
        key_metadata->log, "Remove from cache. Key: {}, offset: {}, size: {}",
        getKey(), offset, file_segment->reserved_size);

    chassert(file_segment->assertCorrectnessUnlocked(segment_lock));

    if (file_segment->queue_iterator)
        file_segment->queue_iterator->invalidate();

    const auto path = key_metadata->getFileSegmentPath(*file_segment);
    bool exists = fs::exists(path);
    if (exists)
    {
        fs::remove(path);
        LOG_TEST(key_metadata->log, "Removed file segment at path: {}", path);
    }
    else if (file_segment->downloaded_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected path {} to exist", path);

    file_segment->detach(segment_lock, *this);
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

    const size_t downloaded_size = file_segment->getDownloadedSize(false);
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

void CleanupQueue::add(const FileCacheKey & key)
{
    std::lock_guard lock(mutex);
    keys.insert(key);
}

void CleanupQueue::remove(const FileCacheKey & key)
{
    std::lock_guard lock(mutex);
    bool erased = keys.erase(key);
    if (!erased)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key {} in removal queue", key);
}

bool CleanupQueue::tryPop(FileCacheKey & key)
{
    std::lock_guard lock(mutex);
    if (keys.empty())
        return false;
    auto it = keys.begin();
    key = *it;
    keys.erase(it);
    return true;
}

size_t CleanupQueue::getSize() const
{
    std::lock_guard lock(mutex);
    return keys.size();
}

}
