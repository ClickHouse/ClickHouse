#pragma once
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/CacheMetadata.h>
#include <Interpreters/Cache/Guards.h>

namespace DB
{

class FileCache;
struct LockedKey;
using LockedKeyPtr = std::shared_ptr<LockedKey>;
struct KeysQueue;
using KeysQueuePtr = std::shared_ptr<KeysQueue>;

struct LockedKeyCreator
{
    LockedKeyCreator(
        const FileCacheKey & key_,
        KeyMetadata & key_metadata_,
        KeysQueuePtr cleanup_keys_metadata_queue_,
        const FileCache * cache_)
        : key(key_)
        , key_metadata(key_metadata_)
        , cleanup_keys_metadata_queue(cleanup_keys_metadata_queue_)
        , cache(cache_) {}

    LockedKeyPtr create();

    FileCacheKey key;
    KeyMetadata & key_metadata;
    KeysQueuePtr cleanup_keys_metadata_queue;
    const FileCache * cache;
};
using LockedKeyCreatorPtr = std::unique_ptr<LockedKeyCreator>;


struct LockedKey : private boost::noncopyable
{
    LockedKey(
        const FileCacheKey & key_,
        KeyMetadata & key_metadata_,
        KeyGuard::Lock && key_lock_,
        KeysQueuePtr cleanup_keys_metadata_queue_,
        const FileCache * cache_);

    ~LockedKey();

    LockedKeyCreatorPtr getCreator() const { return std::make_unique<LockedKeyCreator>(key, key_metadata, cleanup_keys_metadata_queue, cache); }

    void reduceSizeToDownloaded(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    void remove(FileSegmentPtr file_segment, const CacheGuard::Lock &);

    void remove(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    bool isLastHolder(size_t offset);

    KeyMetadata & getKeyMetadata() { return key_metadata; }
    const KeyMetadata & getKeyMetadata() const { return key_metadata; }

    std::vector<size_t> delete_offsets;

private:
    void cleanupKeyDirectory() const;
    void relockWithLockedCache();

    FileCacheKey key;
    const FileCache * cache;

    KeyGuard::Lock lock;
    KeyMetadata & key_metadata;
    KeysQueuePtr cleanup_keys_metadata_queue;

    Poco::Logger * log;
};

struct KeysQueue
{
    std::unordered_set<FileCacheKey> keys;
    std::mutex mutex;

    void add(const FileCacheKey & key)
    {
        std::lock_guard lock(mutex);
        keys.insert(key);
    }

    void clear(std::function<void(const FileCacheKey &)> && func)
    {
        std::lock_guard lock(mutex);
        for (auto it = keys.begin(); it != keys.end();)
        {
            func(*it);
            it = keys.erase(it);
        }
    }
};

}
