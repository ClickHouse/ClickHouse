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
    FileSegmentPtr file_segment_,
    LockedKeyMetadata & locked_key,
    LockedCachePriority * locked_queue)
    : file_segment(file_segment_)
{
    /**
     * File segment can be created with either DOWNLOADED or EMPTY file segment's state.
     * File segment acquires DOWNLOADING state and creates LRUQueue iterator on first
     * successful getOrSetDownaloder call.
     */

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
            queue_iterator = locked_queue->add(
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

const FileSegmentMetadata * KeyMetadata::getByOffset(size_t offset) const
{
    auto it = find(offset);
    if (it == end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return &(it->second);
}

FileSegmentMetadata * KeyMetadata::getByOffset(size_t offset)
{
    auto it = find(offset);
    if (it == end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is not offset {}", offset);
    return &(it->second);
}

const FileSegmentMetadata * KeyMetadata::tryGetByOffset(size_t offset) const
{
    auto it = find(offset);
    if (it == end())
        return nullptr;
    return &(it->second);
}

FileSegmentMetadata * KeyMetadata::tryGetByOffset(size_t offset)
{
    auto it = find(offset);
    if (it == end())
        return nullptr;
    return &(it->second);
}

std::string KeyMetadata::toString() const
{
    std::string result;
    for (auto it = begin(); it != end(); ++it)
    {
        if (it != begin())
            result += ", ";
        result += std::to_string(it->first);
    }
    return result;
}

void KeyMetadata::addToCleanupQueue(const FileCacheKey & key, const KeyGuard::Lock &)
{
    cleanup_queue.add(key);
    cleanup_state = CleanupState::SUBMITTED_TO_CLEANUP_QUEUE;
}

void KeyMetadata::removeFromCleanupQueue(const FileCacheKey & key, const KeyGuard::Lock &)
{
    cleanup_queue.remove(key);
    cleanup_state = CleanupState::NOT_SUBMITTED;
}

void CacheMetadata::doCleanup()
{
    auto lock = guard.lock();

    LOG_INFO(
        &Poco::Logger::get("FileCacheCleanupThread"),
        "Performing background cleanup (size: {})",
        cleanup_queue.getSize());

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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key {} in metadata", cleanup_key.toString());

        auto key_metadata = it->second;
        auto key_lock = key_metadata->lock();
        /// As in lockKeyMetadata we extract key metadata from cache metadata
        /// under CacheMetadataGuard::Lock, but take KeyGuard::Lock only after we
        /// released cache CacheMetadataGuard::Lock, then we must to take into
        /// account it here.
        if (key_metadata->getCleanupState(key_lock) == KeyMetadata::CleanupState::NOT_SUBMITTED)
            continue;

        erase(it);

        try
        {
            const fs::path key_directory = FileCache::getPathInLocalCache(base_directory, cleanup_key);
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
    KeyGuard::Lock && lock_,
    const std::string & key_path_)
    : key(key_)
    , key_path(key_path_)
    , key_metadata(key_metadata_)
    , lock(std::move(lock_))
    , log(&Poco::Logger::get("LockedKeyMetadata"))
{
}

LockedKeyMetadata::~LockedKeyMetadata()
{
    if (!key_metadata->empty())
        return;

    key_metadata->addToCleanupQueue(key, lock);
}

void LockedKeyMetadata::createKeyDirectoryIfNot()
{
    if (key_metadata->createdBaseDirectory(lock))
        return;

    fs::create_directories(key_path);
    key_metadata->created_base_directory = true;
}

bool LockedKeyMetadata::isLastOwnerOfFileSegment(size_t offset) const
{
    const auto * file_segment_metadata = key_metadata->getByOffset(offset);
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

    auto * file_segment_metadata = key_metadata->getByOffset(offset);

    if (file_segment_metadata->queue_iterator)
        LockedCachePriorityIterator(cache_lock, file_segment_metadata->queue_iterator).remove();

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

    auto * file_segment_metadata = key_metadata->getByOffset(offset);
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

    auto & entry = *LockedCachePriorityIterator(cache_lock, file_segment_metadata->queue_iterator);
    assert(file_segment->downloaded_size <= file_segment->reserved_size);
    assert(entry.size == file_segment->reserved_size);
    assert(entry.size >= file_segment->downloaded_size);

    CreateFileSegmentSettings create_settings(file_segment->getKind());
    file_segment_metadata->file_segment = std::make_shared<FileSegment>(
        offset, downloaded_size, key, key_metadata, file_segment->cache, FileSegment::State::DOWNLOADED, create_settings);

    if (file_segment->reserved_size > file_segment->downloaded_size)
        entry.size = downloaded_size;

    assert(file_segment->reserved_size == downloaded_size);
    assert(file_segment_metadata->size() == entry.size);
}

void CleanupQueue::add(const FileCacheKey & key)
{
    std::lock_guard lock(mutex);
    auto [_, inserted] = keys.insert(key);
    if (!inserted)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Key {} is already in removal queue", key.toString());
    }
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
