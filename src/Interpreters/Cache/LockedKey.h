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
 *    (Why: this LockedKey locks key metadata mutex in ctor, unlocks it in dtor, and so
 *    when key is going to be deleted, key mutex is also locked.
 *    Why it cannot be the other way round? E.g. that ctor of LockedKey locks the key
 *    right after it was deleted? This case it taken into consideration in createLockedKey())
 * 2. the key cannot be modified, e.g. new offsets cannot be added to key; already existing
 *    offsets cannot be deleted from the key
 * And also provides some methods which allow the owner of this LockedKey object to do such
 * modification of the key (adding/deleting offsets) and deleting the key from cache.
 */
struct LockedKey : private boost::noncopyable
{
    LockedKey(
        const FileCacheKey & key_,
        std::shared_ptr<KeyMetadata> key_metadata_,
        KeyGuard::Lock && key_lock_,
        const std::string & key_path_,
        KeysQueuePtr cleanup_keys_metadata_queue_);

    ~LockedKey();

    const FileCacheKey & getKey() const { return key; }

    KeyMetadataPtr getKeyMetadata() const { return key_metadata; }

    void removeFileSegment(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    void shrinkFileSegmentToDownloadedSize(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    bool isLastOwnerOfFileSegment(size_t offset) const;

private:
    void removeKeyIfEmpty() const;

    const FileCacheKey key;
    const std::string key_path;

    KeyGuard::Lock lock;
    const std::shared_ptr<KeyMetadata> key_metadata;
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
