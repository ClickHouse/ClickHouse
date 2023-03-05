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
    void cleanupKeyDirectory() const;

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
