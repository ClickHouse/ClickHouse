#include <Interpreters/Cache/LockedKey.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/LockedFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Common/logger_useful.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

LockedKeyPtr LockedKeyCreator::create()
{
    return std::make_unique<LockedKey>(key, key_metadata, key_metadata.guard->lock(), cleanup_keys_metadata_queue, cache);
}

LockedKey::LockedKey(
    const FileCacheKey & key_,
    KeyMetadata & key_metadata_,
    KeyGuard::Lock && lock_,
    KeysQueuePtr cleanup_keys_metadata_queue_,
    const FileCache * cache_)
    : key(key_)
    , cache(cache_)
    , lock(std::move(lock_))
    , key_metadata(key_metadata_)
    , cleanup_keys_metadata_queue(cleanup_keys_metadata_queue_)
    , log(&Poco::Logger::get("LockedKey"))
{
}

LockedKey::~LockedKey()
{
    cleanupKeyDirectory();
}

void LockedKey::remove(FileSegmentPtr file_segment, const CacheGuard::Lock & cache_lock)
{
    /// We must hold pointer to file segment while removing it.
    chassert(file_segment->key() == key);
    remove(file_segment->offset(), file_segment->lock(), cache_lock);
}

bool LockedKey::isLastHolder(size_t offset)
{
    const auto * cell = getKeyMetadata().getByOffset(offset);
    return cell->file_segment.use_count() == 2;
}

void LockedKey::remove(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    const CacheGuard::Lock & cache_lock)
{
    LOG_DEBUG(
        log, "Remove from cache. Key: {}, offset: {}",
        key.toString(), offset);

    auto * cell = key_metadata.getByOffset(offset);

    if (cell->queue_iterator)
        LockedCachePriorityIterator(cache_lock, cell->queue_iterator).remove();

    const auto cache_file_path = cell->file_segment->getPathInLocalCache();
    cell->file_segment->detach(segment_lock, *this);

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

void LockedKey::reduceSizeToDownloaded(
    size_t offset,
    const FileSegmentGuard::Lock & segment_lock,
    const CacheGuard::Lock & cache_lock)
{
    /**
     * In case file was partially downloaded and it's download cannot be continued
     * because of no space left in cache, we need to be able to cut cell's size to downloaded_size.
     */

    auto * cell = key_metadata.getByOffset(offset);
    const auto & file_segment = cell->file_segment;

    size_t downloaded_size = file_segment->downloaded_size;
    size_t full_size = file_segment->range().size();

    if (downloaded_size == full_size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Nothing to reduce, file segment fully downloaded: {}",
            file_segment->getInfoForLogUnlocked(segment_lock));
    }

    [[maybe_unused]] const auto & entry = *LockedCachePriorityIterator(cache_lock, cell->queue_iterator);
    assert(file_segment->downloaded_size <= file_segment->reserved_size);
    assert(entry.size == file_segment->reserved_size);
    assert(entry.size >= file_segment->downloaded_size);

    if (file_segment->reserved_size > file_segment->downloaded_size)
    {
        int64_t extra_size = static_cast<ssize_t>(cell->file_segment->reserved_size) - static_cast<ssize_t>(file_segment->downloaded_size);
        LockedCachePriorityIterator(cache_lock, cell->queue_iterator).incrementSize(-extra_size);
    }

    CreateFileSegmentSettings create_settings(file_segment->getKind());
    cell->file_segment = std::make_shared<FileSegment>(
        offset, downloaded_size, key, getCreator(), file_segment->cache,
        FileSegment::State::DOWNLOADED, create_settings);

    assert(file_segment->reserved_size == downloaded_size);
    assert(cell->size() == entry.size);
}

void LockedKey::cleanupKeyDirectory() const
{
    /// We cannot remove key directory, because if cache is not initialized,
    /// it means we are currently iterating it.
    if (!cache->isInitialized())
        return;

    /// Someone might still need this directory.
    if (!key_metadata.empty())
        return;

    key_metadata.removed = true;

    /// Now `key_metadata` empty and the key lock is still locked.
    /// So it is guaranteed that no one will add something.

    fs::path key_path = cache->getPathInLocalCache(key);
    if (fs::exists(key_path))
    {
        key_metadata.created_base_directory = false;
        fs::remove_all(key_path);
    }
    cleanup_keys_metadata_queue->add(key);
}

}
