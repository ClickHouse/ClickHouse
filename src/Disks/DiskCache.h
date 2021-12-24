#pragma once

#include "DiskLocal.h"
#include "DiskCacheMetadata.h"
#include <unordered_map>
#include <list>
#include <functional>

namespace DB
{

class DiskCachePolicy
{
public:
    virtual ~DiskCachePolicy() {}

    struct CachePart
    {
        enum class CachePartType
        {
            CACHED,
            ABSENT,
            ABSENT_NO_CACHE,
            EMPTY,
        };
        CachePart(size_t offset_, size_t size_) : offset(offset_), size(size_) {}
        size_t offset;
        size_t size;
        CachePartType type = CachePartType::CACHED;
    };

    typedef std::list<CachePart> CachePartList;

    /// fing object parts in cache
    /// wait if some part is in progress
    /// return next part to read or download
    /// returned part marked as in progress
    virtual CachePart find(const String & key, size_t offset, size_t size) = 0;

    /// reserve some size in cache, with eviction old records if needs
    /// returns list of keys to remove from cache
    virtual std::list<std::pair<String, size_t>> reserve(size_t size) = 0;

    /// insert file in cache index
    /// returns list of keys to remove from cache (as reserve)
    virtual std::list<std::pair<String, size_t>> add(const String & key, size_t file_size,
        size_t offset, size_t size, bool restore) = 0;

    /// call on download error
    virtual void error(const String & key, size_t offset) = 0;

    /// remove file from cache index
    virtual CachePartList remove(const String & key) = 0;

    /// remove one record from cache index
    virtual void remove(const String & key, size_t offset, size_t size) = 0;

    /// mark record as used (for LRU, etc.)
    virtual void read(const String & key, size_t offset, size_t size) = 0;
};

class DiskCacheLRUPolicy : public DiskCachePolicy
{
public:
    DiskCacheLRUPolicy(size_t cache_size_limit_, size_t nodes_limit_);
    ~DiskCacheLRUPolicy() override {}

    CachePart find(const String & key, size_t offset, size_t size) override;
    std::list<std::pair<String, size_t>> reserve(size_t size) override;
    std::list<std::pair<String, size_t>> add(const String & key, size_t file_size,
        size_t offset, size_t size, bool restore) override;
    void error(const String & key, size_t offset) override;
    CachePartList remove(const String & key) override;
    void remove(const String & key, size_t offset, size_t size) override;
    void read(const String & key, size_t offset, size_t size) override;

private:
    void complete(const String & key, size_t offset, FileDownloadStatus status);
    std::list<std::pair<String, size_t>> reserve_unsafe(size_t size, bool free_only = false);

    size_t cache_size_limit = 0;
    size_t nodes_limit = 0;
    size_t cache_size = 0;
    size_t reserved_size = 0;

    struct CacheEntry
    {
        CacheEntry(const String & key_, size_t offset_, size_t size_) : key(key_), offset(offset_), size(size_) {}
        String key;
        // part offset
        size_t offset;
        // part size in cache
        size_t size;
    };

    struct FileEntry
    {
        // file size on remote storage
        size_t size = 0;
        // parts[offset]
        std::map<size_t, std::list<CacheEntry>::iterator> parts;
    };

    struct CacheInProgressEntry : public FileDownloadMetadata
    {
        CacheInProgressEntry(size_t size_);

        size_t size;
    };

    std::mutex cache_mutex;

    // cache_list ordered by LRU
    std::list<CacheEntry> cache_list;

    // cache_map[key]
    std::unordered_map<String, FileEntry> cache_map;

    // cache_in_progress[key][offset]
    std::unordered_map<String, std::map<size_t, std::shared_ptr<CacheInProgressEntry>>> cache_in_progress;

    std::unordered_map<uint64_t, uint32_t> read_thread_ids;

    Poco::Logger * log = nullptr;
};

class DiskCacheDownloader
{
public:
    virtual ~DiskCacheDownloader() {}

    struct RemoteFSStream
    {
        std::unique_ptr<ReadBuffer> stream;
        size_t file_size = 0;
        size_t expected_size = 0;
    };

    virtual RemoteFSStream get(size_t offset, size_t size) = 0;
    virtual String getFilePath() const = 0;
};

class DiskCache
{
public:
    DiskCache(std::shared_ptr<DiskLocal> cache_disk_, std::shared_ptr<DiskCachePolicy> cache_policy_);

    // find object in cache, part_size = 0 - unknown (till end of object)
    // TODO: return ReadBuffer OR list or ranges to download
    std::unique_ptr<ReadBuffer> find(const String & path, size_t offset, size_t part_size, std::shared_ptr<DiskCacheDownloader> downloader);
    // insert object in cache, should be known part_size > 0 and total_size > 0 here
    //std::unique_ptr<ReadBuffer> insert(const String & path, size_t offset, size_t part_size, size_t total_size, std::unique_ptr<ReadBuffer> & from);
    // remove object from cache
    void remove(const String & path);

    static String getCacheBasePath(const String & key);

private:
    static String getKey(const String & path);

    // reload cache from disk after restart
    void reload();

    std::shared_ptr<DiskLocal> cache_disk;
    std::shared_ptr<DiskCachePolicy> cache_policy;
};

}
