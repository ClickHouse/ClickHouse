#pragma once
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/Guards.h>
#include <Common/ConcurrentBoundedQueue.h>

namespace DB
{

class FileCache;
struct LockedKey;
using LockedKeyPtr = std::shared_ptr<LockedKey>;
struct KeysQueue;
using KeysQueuePtr = std::shared_ptr<KeysQueue>;

/**
 * `LockedKey` is an object which makes sure that as long as it exists the following is true:
 * 1. the key cannot be removed from cache
 * 2. the key cannot be modified, e.g. new offsets cannot be added to key; already existing
 *    offsets cannot be deleted from the key
 * And also provides some methods which allow the owner of this LockedKey object to do such
 * modification of the key (adding/deleting offsets) and deleting the key from cache.
 */
struct LockedKey : private boost::noncopyable
{
    LockedKey(
        const FileCacheKey & key_,
        std::weak_ptr<KeyMetadata> key_metadata_,
        KeyGuard::Lock && key_lock_,
        KeysQueuePtr cleanup_keys_metadata_queue_,
        const FileCache * cache_);

    ~LockedKey();

    void reduceSizeToDownloaded(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    void remove(FileSegmentPtr file_segment, const CacheGuard::Lock &);

    void remove(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    bool isLastHolder(size_t offset) const;

    KeyMetadataPtr getKeyMetadata() const { return key_metadata.lock(); }

    std::vector<size_t> delete_offsets;

private:
    void removeKeyIfEmpty() const;

    FileCacheKey key;
    const FileCache * cache;

    KeyGuard::Lock lock;
    mutable std::weak_ptr<KeyMetadata> key_metadata;
    KeysQueuePtr cleanup_keys_metadata_queue;

    Poco::Logger * log;
};

struct KeysQueue
{
public:
    void add(const FileCacheKey & key);
    void remove(const FileCacheKey & key);
    bool tryPop(FileCacheKey & key);

private:
    std::unordered_set<FileCacheKey> keys;
    std::mutex mutex;
};

}
