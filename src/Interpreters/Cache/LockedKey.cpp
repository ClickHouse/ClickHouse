#include <Interpreters/Cache/LockedKey.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/LockedFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Common/logger_useful.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LockedKey::LockedKey(
    const FileCacheKey & key_,
    KeyMetadata & key_metadata_,
    KeyGuard::Lock && lock_,
    const std::string & key_path_,
    KeysQueuePtr cleanup_keys_metadata_queue_)
    : key(key_)
    , key_path(key_path_)
    , lock(std::move(lock_))
    , key_metadata(key_metadata_)
    , cleanup_keys_metadata_queue(cleanup_keys_metadata_queue_)
    , log(&Poco::Logger::get("LockedKey"))
{
}

LockedKey::~LockedKey()
{
    removeKeyIfEmpty();
}

bool LockedKey::isLastOwnerOfFileSegment(size_t offset) const
{
    const auto * file_segment_metadata = key_metadata.getByOffset(offset);
    return file_segment_metadata->file_segment.use_count() == 2;
}

void LockedKey::removeFileSegment(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    const CacheGuard::Lock & cache_lock)
{
    LOG_DEBUG(
        log, "Remove from cache. Key: {}, offset: {}",
        key.toString(), offset);

    auto * file_segment_metadata = key_metadata.getByOffset(offset);

    if (file_segment_metadata->queue_iterator)
        LockedCachePriorityIterator(cache_lock, file_segment_metadata->queue_iterator).remove();

    const auto cache_file_path = file_segment_metadata->file_segment->getPathInLocalCache();
    file_segment_metadata->file_segment->detach(segment_lock, *this);

    key_metadata.erase(offset);

    if (fs::exists(cache_file_path))
    {
        try
        {
            fs::remove(cache_file_path);
        }
        catch (...)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Removal of cached file failed. Key: {}, offset: {}, path: {}, error: {}",
                key.toString(), offset, cache_file_path, getCurrentExceptionMessage(false));
        }
    }
}

void LockedKey::shrinkFileSegmentToDownloadedSize(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    const CacheGuard::Lock & cache_lock)
{
    /**
     * In case file was partially downloaded and it's download cannot be continued
     * because of no space left in cache, we need to be able to cut file segment's size to downloaded_size.
     */

    auto * file_segment_metadata = key_metadata.getByOffset(offset);
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
        offset, downloaded_size, key, &key_metadata, file_segment->cache, FileSegment::State::DOWNLOADED, create_settings);

    if (file_segment->reserved_size > file_segment->downloaded_size)
        entry.size = downloaded_size;

    assert(file_segment->reserved_size == downloaded_size);
    assert(file_segment_metadata->size() == entry.size);
}

void LockedKey::removeKeyIfEmpty() const
{
    /// Someone might still need this directory.
    if (!key_metadata.empty())
        return;

    key_metadata.removed = true;

    /// Now `key_metadata` empty and the key lock is still locked.
    /// So it is guaranteed that no one will add something.
    if (fs::exists(key_path))
    {
        key_metadata.created_base_directory = false;
        fs::remove_all(key_path);
    }
    cleanup_keys_metadata_queue->add(key);
}

void KeysQueue::add(const FileCacheKey & key)
{
    std::lock_guard lock(mutex);
    keys.insert(key);
}

void KeysQueue::remove(const FileCacheKey & key)
{
    std::lock_guard lock(mutex);
    bool erased = keys.erase(key);
    if (!erased)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No such key to erase: {}", key.toString());
}

bool KeysQueue::tryPop(FileCacheKey & key)
{
    std::lock_guard lock(mutex);
    if (keys.empty())
        return false;
    auto it = keys.begin();
    key = *it;
    keys.erase(it);
    return true;
}

}
