#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/LockedFileCachePriority.h>
#include <Common/logger_useful.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

FileSegmentMetadata::FileSegmentMetadata(
    FileSegmentPtr file_segment_, LockedKeyMetadata & locked_key, LockedCachePriority * locked_queue)
    : file_segment(file_segment_)
{
    switch (file_segment->state())
    {
        case FileSegment::State::DOWNLOADED:
        {
            if (!locked_queue)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Adding file segment with state DOWNLOADED requires locked queue lock");
            }
            file_segment->getQueueIterator() = locked_queue->add(
                file_segment->key(), file_segment->offset(), file_segment->range().size(), locked_key.getKeyMetadata());

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

IFileCachePriority::Iterator & FileSegmentMetadata::getQueueIterator() const
{
    return file_segment->getQueueIterator();
}

String CacheMetadata::getPathInLocalCache(const Key & key, size_t offset, FileSegmentKind segment_kind) const
{
    String file_suffix;
    switch (segment_kind)
    {
        case FileSegmentKind::Persistent:
            file_suffix = "_persistent";
            break;
        case FileSegmentKind::Temporary:
            file_suffix = "_temporary";
            break;
        case FileSegmentKind::Regular:
            file_suffix = "";
            break;
    }

    const auto key_str = key.toString();
    return fs::path(base_directory) / key_str.substr(0, 3) / key_str / (std::to_string(offset) + file_suffix);
}

String CacheMetadata::getPathInLocalCache(const Key & key) const
{
    const auto key_str = key.toString();
    return fs::path(base_directory) / key_str.substr(0, 3) / key_str;
}

LockedKeyMetadataPtr CacheMetadata::lockKeyMetadata(const FileCacheKey & key, KeyNotFoundPolicy key_not_found_policy, bool is_initial_load)
{
    KeyMetadataPtr key_metadata;
    {
        auto lock = guard.lock();

        auto it = find(key);
        if (it == end())
        {
            if (key_not_found_policy == KeyNotFoundPolicy::THROW)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key `{}` in cache", key.toString());
            else if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL)
                return nullptr;

            it = emplace(
                key,
                std::make_shared<KeyMetadata>(/* base_directory_already_exists */is_initial_load, cleanup_queue)).first;
        }

        key_metadata = it->second;
    }

    {
        auto locked_metadata = std::make_unique<LockedKeyMetadata>(key, key_metadata, getPathInLocalCache(key));
        const auto key_state = locked_metadata->getKeyState();

        if (key_state == KeyMetadata::KeyState::ACTIVE)
            return locked_metadata;

        if (key_not_found_policy == KeyNotFoundPolicy::THROW)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key `{}` in cache", key.toString());

        if (key_not_found_policy == KeyNotFoundPolicy::RETURN_NULL)
            return nullptr;

        if (key_state == KeyMetadata::KeyState::REMOVING)
        {
            locked_metadata->removeFromCleanupQueue();
            return locked_metadata;
        }

        chassert(key_state == KeyMetadata::KeyState::REMOVED);
        chassert(key_not_found_policy == KeyNotFoundPolicy::CREATE_EMPTY);
    }

    /// Not we are at a case:
    /// key_state == KeyMetadata::KeyState::REMOVED
    /// and KeyNotFoundPolicy == CREATE_EMPTY
    /// Retry.
    return lockKeyMetadata(key, key_not_found_policy);
}

LockedKeyMetadataPtr CacheMetadata::lockKeyMetadata(
    const FileCacheKey & key, KeyMetadataPtr key_metadata, bool skip_if_in_cleanup_queue) const
{
    auto locked_metadata = std::make_unique<LockedKeyMetadata>(key, key_metadata, getPathInLocalCache(key));
    const auto key_state = locked_metadata->getKeyState();

    if (key_state == KeyMetadata::KeyState::ACTIVE)
        return locked_metadata;

    if (skip_if_in_cleanup_queue
        && key_state == KeyMetadata::KeyState::REMOVING)
        return nullptr;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot lock key {}: it was removed from cache", key.toString());
}

void CacheMetadata::iterate(IterateCacheMetadataFunc && func)
{
    auto lock = guard.lock();
    for (const auto & [key, key_metadata] : *this)
    {
        auto locked_key = lockKeyMetadata(key, key_metadata, /* skip_if_in_cleanup_queue */true);
        if (!locked_key)
            continue;

        func(*locked_key);
    }
}

void CacheMetadata::doCleanup()
{
    auto lock = guard.lock();

    /// Let's mention this case.
    /// This metadata cleanup is delayed so what is we marked key as deleted and
    /// put it to deletion queue, but then the same key was added to cache before
    /// we actually performed this delayed removal?
    /// In this case it will work fine because on each attempt to add any key to cache
    /// we perform this delayed removal.

    FileCacheKey cleanup_key;
    while (cleanup_queue.tryPop(cleanup_key))
    {
        auto it = find(cleanup_key);
        if (it == end())
            continue;

        auto locked_metadata = std::make_unique<LockedKeyMetadata>(it->first, it->second, getPathInLocalCache(it->first));
        const auto key_state = locked_metadata->getKeyState();

        if (key_state == KeyMetadata::KeyState::ACTIVE)
        {
            /// Key was added back to cache after we submitted it to removal queue.
            continue;
        }

        locked_metadata->markAsRemoved();
        erase(it);

        try
        {
            const fs::path key_directory = getPathInLocalCache(cleanup_key);
            if (fs::exists(key_directory))
                fs::remove_all(key_directory);

            const fs::path key_prefix_directory = key_directory.parent_path();
            if (fs::exists(key_prefix_directory) && fs::is_empty(key_prefix_directory))
                fs::remove_all(key_prefix_directory);
        }
        catch (...)
        {
            chassert(false);
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

LockedKeyMetadata::LockedKeyMetadata(
    const FileCacheKey & key_,
    std::shared_ptr<KeyMetadata> key_metadata_,
    const std::string & key_path_)
    : key(key_)
    , key_path(key_path_)
    , key_metadata(key_metadata_)
    , lock(key_metadata->lock())
    , log(&Poco::Logger::get("LockedKeyMetadata"))
{
}

LockedKeyMetadata::~LockedKeyMetadata()
{
    if (!key_metadata->empty())
        return;

    key_metadata->key_state = KeyMetadata::KeyState::REMOVING;
    key_metadata->cleanup_queue.add(key);
}

void LockedKeyMetadata::removeFromCleanupQueue()
{
    if (key_metadata->key_state != KeyMetadata::KeyState::REMOVING)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove non-removing");

    /// Just mark key_state as "not to be removed", the cleanup thread will check it and skip the key.
    key_metadata->key_state = KeyMetadata::KeyState::ACTIVE;
}

bool LockedKeyMetadata::markAsRemoved()
{
    chassert(key_metadata->key_state != KeyMetadata::KeyState::REMOVED);

    if (key_metadata->key_state == KeyMetadata::KeyState::ACTIVE)
        return false;

    key_metadata->key_state = KeyMetadata::KeyState::REMOVED;
    return true;
}

bool LockedKeyMetadata::isLastOwnerOfFileSegment(size_t offset) const
{
    const auto * file_segment_metadata = getByOffset(offset);
    return file_segment_metadata->file_segment.use_count() == 2;
}

void LockedKeyMetadata::removeFileSegment(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    const CacheGuard::Lock & cache_lock)
{
    LOG_DEBUG(
        log, "Remove from cache. Key: {}, offset: {}",
        key.toString(), offset);

    auto * file_segment_metadata = getByOffset(offset);

    if (file_segment_metadata->getQueueIterator())
        LockedCachePriorityIterator(cache_lock, file_segment_metadata->getQueueIterator()).remove();

    const auto cache_file_path = file_segment_metadata->file_segment->getPathInLocalCache();
    file_segment_metadata->file_segment->detach(segment_lock, *this);

    key_metadata->erase(offset);

    if (fs::exists(cache_file_path))
        fs::remove(cache_file_path);
}

void LockedKeyMetadata::shrinkFileSegmentToDownloadedSize(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    const CacheGuard::Lock & cache_lock)
{
    /**
     * In case file was partially downloaded and it's download cannot be continued
     * because of no space left in cache, we need to be able to cut file segment's size to downloaded_size.
     */

    auto * file_segment_metadata = getByOffset(offset);
    const auto & file_segment = file_segment_metadata->file_segment;

    size_t downloaded_size = file_segment->downloaded_size;
    size_t full_size = file_segment->range().size();

    if (downloaded_size == full_size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Nothing to reduce, file segment fully downloaded: {}",
            file_segment->getInfoForLogUnlocked(segment_lock));
    }

    auto & entry = LockedCachePriorityIterator(cache_lock, file_segment_metadata->getQueueIterator()).getEntry();
    assert(file_segment->downloaded_size <= file_segment->reserved_size);
    assert(entry.size == file_segment->reserved_size);
    assert(entry.size >= file_segment->downloaded_size);

    CreateFileSegmentSettings create_settings(file_segment->getKind());
    file_segment_metadata->file_segment = std::make_shared<FileSegment>(
        key, offset, downloaded_size, key_metadata, file_segment->getQueueIterator(),
        file_segment->cache, FileSegment::State::DOWNLOADED, create_settings);

    if (file_segment->reserved_size > file_segment->downloaded_size)
        entry.size = downloaded_size;

    assert(file_segment->reserved_size == downloaded_size);
    assert(file_segment_metadata->size() == entry.size);
}

void LockedKeyMetadata::assertFileSegmentCorrectness(const FileSegment & file_segment) const
{
    if (file_segment.key() != key)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} = {}", file_segment.key().toString(), key.toString());

    file_segment.assertCorrectness();
}

const FileSegmentMetadata * LockedKeyMetadata::getByOffset(size_t offset) const
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return &(it->second);
}

FileSegmentMetadata * LockedKeyMetadata::getByOffset(size_t offset)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return &(it->second);
}

const FileSegmentMetadata * LockedKeyMetadata::tryGetByOffset(size_t offset) const
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        return nullptr;
    return &(it->second);
}

FileSegmentMetadata * LockedKeyMetadata::tryGetByOffset(size_t offset)
{
    auto it = key_metadata->find(offset);
    if (it == key_metadata->end())
        return nullptr;
    return &(it->second);
}

std::string LockedKeyMetadata::toString() const
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key {} in removal queue", key.toString());
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
