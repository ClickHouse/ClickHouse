#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <boost/functional/hash.hpp>
#include <boost/noncopyable.hpp>

#include <Core/Types.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <IO/ReadSettings.h>
#include <Interpreters/Cache/LockedFileCachePriority.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/Guards.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

struct KeyTransaction;
using KeyTransactionPtr = std::shared_ptr<KeyTransaction>;
using KeyTransactionsMap = std::unordered_map<FileCacheKey, KeyTransactionPtr>;
struct KeysQueue;
using KeysQueuePtr = std::shared_ptr<KeysQueue>;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
/// Different caching algorithms are implemented using IFileCachePriority.
class FileCache : private boost::noncopyable
{
public:
    using Key = DB::FileCacheKey;

    FileCache(const String & cache_base_path_, const FileCacheSettings & cache_settings_);

    ~FileCache() = default;

    void initialize();

    const String & getBasePath() const { return cache_base_path; }

    static Key createKeyForPath(const String & path);

    /**
     * Given an `offset` and `size` representing [offset, offset + size) bytes interval,
     * return list of cached non-overlapping non-empty
     * file segments `[segment1, ..., segmentN]` which intersect with given interval.
     *
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * As long as pointers to returned file segments are hold
     * it is guaranteed that these file segments are not removed from cache.
     */
    FileSegmentsHolderPtr getOrSet(const Key & key, size_t offset, size_t size, const CreateFileSegmentSettings & settings);

    /**
     * Segments in returned list are ordered in ascending order and represent a full contiguous
     * interval (no holes). Each segment in returned list has state: DOWNLOADED, DOWNLOADING or EMPTY.
     *
     * If file segment has state EMPTY, then it is also marked as "detached". E.g. it is "detached"
     * from cache (not owned by cache), and as a result will never change it's state and will be destructed
     * with the destruction of the holder, while in getOrSet() EMPTY file segments can eventually change
     * it's state (and become DOWNLOADED).
     */
    FileSegmentsHolderPtr get(const Key & key, size_t offset, size_t size);

    FileSegmentsHolderPtr set(const Key & key, size_t offset, size_t size, const CreateFileSegmentSettings & settings);

    /// Remove files by `key`. Removes files which might be used at the moment.
    void removeKeyIfExists(const Key & key);

    /// Remove files by `key`. Will not remove files which are used at the moment.
    void removeAllReleasable();

    String getPathInLocalCache(const Key & key, size_t offset, FileSegmentKind segment_kind) const;

    String getPathInLocalCache(const Key & key) const;

    std::vector<String> tryGetCachePaths(const Key & key);

    size_t getUsedCacheSize() const;

    size_t getFileSegmentsNum() const;

    size_t getMaxFileSegmentSize() const { return max_file_segment_size; }

    static bool readThrowCacheAllowed();

    bool tryReserve(const Key & key, size_t offset, size_t size);

    struct QueryContextHolder;
    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;
    QueryContextHolderPtr getQueryContextHolder(const String & query_id, const ReadSettings & settings);

    FileSegmentsHolderPtr getSnapshot();

    FileSegmentsHolderPtr getSnapshot(const Key & key);

    FileSegmentsHolderPtr dumpQueue();

    bool isInitialized() const { return is_initialized; }

    void assertCacheCorrectness();

    CacheGuard::Lock createCacheTransaction() { return cache_guard.lock(); }

private:
    using KeyAndOffset = FileCacheKeyAndOffset;

    const String cache_base_path;
    const size_t max_file_segment_size;
    const bool allow_persistent_files;
    const size_t bypass_cache_threshold = 0;

    Poco::Logger * log;

    struct FileSegmentCell : private boost::noncopyable
    {
        FileSegmentPtr file_segment;

        /// Iterator is put here on first reservation attempt, if successful.
        IFileCachePriority::Iterator queue_iterator;

        /// Pointer to file segment is always hold by the cache itself.
        /// Apart from pointer in cache, it can be hold by cache users, when they call
        /// getorSet(), but cache users always hold it via FileSegmentsHolder.
        bool releasable() const { return file_segment.unique(); }

        size_t size() const { return file_segment->reserved_size; }

        FileSegmentCell(
            FileSegmentPtr file_segment_,
            KeyTransaction & key_transaction,
            LockedCachePriority * locked_queue);

        FileSegmentCell(FileSegmentCell && other) noexcept
            : file_segment(std::move(other.file_segment)), queue_iterator(std::move(other.queue_iterator)) {}
    };

public:
    struct KeyMetadata : public std::map<size_t, FileSegmentCell>
    {
        const FileSegmentCell * get(size_t offset) const;
        FileSegmentCell * get(size_t offset);

        const FileSegmentCell * tryGet(size_t offset) const;
        FileSegmentCell * tryGet(size_t offset);

        std::string toString() const;

        KeyGuardPtr guard = std::make_shared<KeyGuard>();
        bool created_base_directory = false;
    };
    using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;

private:
    std::exception_ptr init_exception;
    std::atomic<bool> is_initialized = false;
    mutable std::mutex init_mutex;

    using CacheMetadata = std::unordered_map<Key, KeyMetadataPtr>;
    CacheMetadata metadata;
    CacheMetadataGuard metadata_guard;

    enum class KeyNotFoundPolicy
    {
        THROW,
        CREATE_EMPTY,
        RETURN_NULL,
    };

    KeyTransactionPtr createKeyTransaction(const Key & key, KeyNotFoundPolicy key_not_found_policy);

    KeyTransactionCreatorPtr getKeyTransactionCreator(const Key & key, KeyTransaction & key_transaction);

    KeysQueuePtr cleanup_keys_metadata_queue;

    FileCachePriorityPtr main_priority;
    mutable CacheGuard cache_guard;

    struct HitsCountStash
    {
        HitsCountStash(size_t hits_threashold_, size_t queue_size_)
            : hits_threshold(hits_threashold_), queue(std::make_unique<LRUFileCachePriority>(0, queue_size_))
        {
            if (!queue_size_)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Queue size for hits queue must be non-zero");
        }

        const size_t hits_threshold;
        FileCachePriorityPtr queue;
        using Records = std::unordered_map<KeyAndOffset, IFileCachePriority::Iterator, FileCacheKeyAndOffsetHash>;
        Records records;
    };

    mutable std::unique_ptr<HitsCountStash> stash;

    class QueryLimit
    {
    friend class FileCache;
    public:
        class QueryContext;
        using QueryContextPtr = std::shared_ptr<QueryContext>;
        class LockedQueryContext;
        using LockedQueryContextPtr = std::unique_ptr<LockedQueryContext>;

        LockedQueryContextPtr tryGetQueryContext(const CacheGuard::Lock & lock);

        QueryContextPtr getOrSetQueryContext(
            const std::string & query_id, const ReadSettings & settings, const CacheGuard::Lock &);

        void removeQueryContext(const std::string & query_id, const CacheGuard::Lock &);

    private:
        using QueryContextMap = std::unordered_map<String, QueryContextPtr>;
        QueryContextMap query_map;

    public:
        class QueryContext
        {
        public:
            QueryContext(size_t query_cache_size, bool recache_on_query_limit_exceeded_)
                : priority(std::make_unique<LRUFileCachePriority>(query_cache_size, 0))
                , recache_on_query_limit_exceeded(recache_on_query_limit_exceeded_) {}

        private:
            friend class QueryLimit::LockedQueryContext;

            using Records = std::unordered_map<KeyAndOffset, IFileCachePriority::Iterator, FileCacheKeyAndOffsetHash>;
            Records records;
            FileCachePriorityPtr priority;
            const bool recache_on_query_limit_exceeded;
        };

        class LockedQueryContext
        {
        public:
            LockedQueryContext(QueryContextPtr context_, const CacheGuard::Lock & lock_)
                : context(context_), lock(lock_), priority(lock_, *context->priority) {}

            IFileCachePriority & getPriority() { return *context->priority; }
            const IFileCachePriority & getPriority() const { return *context->priority; }

            size_t getSize() const { return priority.getSize(); }

            size_t getSizeLimit() const { return priority.getSizeLimit(); }

            bool recacheOnQueryLimitExceeded() const { return context->recache_on_query_limit_exceeded; }

            IFileCachePriority::Iterator tryGet(const Key & key, size_t offset);

            void add(const Key & key, size_t offset, IFileCachePriority::Iterator iterator);

            void remove(const Key & key, size_t offset);

        private:
            QueryContextPtr context;
            const CacheGuard::Lock & lock;
            LockedCachePriority priority;
        };
    };

    using QueryLimitPtr = std::unique_ptr<QueryLimit>;
    QueryLimitPtr query_limit;

public:
    /// For per query cache limit.
    struct QueryContextHolder : private boost::noncopyable
    {
        QueryContextHolder(const String & query_id_, FileCache * cache_, QueryLimit::QueryContextPtr context_);

        QueryContextHolder() = default;

        ~QueryContextHolder();

        String query_id;
        FileCache * cache = nullptr;
        QueryLimit::QueryContextPtr context;
    };

private:
    void assertInitialized() const;

    void loadMetadata();

    FileSegments getImpl(
        const Key & key,
        const FileSegment::Range & range,
        const KeyTransaction & key_transaction);

    FileSegments splitRangeIntoCells(
        const Key & key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        const CreateFileSegmentSettings & create_settings,
        KeyTransaction & key_transaction);

    void fillHolesWithEmptyFileSegments(
        FileSegments & file_segments,
        const Key & key,
        const FileSegment::Range & range,
        bool fill_with_detached_file_segments,
        const CreateFileSegmentSettings & settings,
        KeyTransaction & key_transaction);

    KeyMetadata::iterator addCell(
        const Key & key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        const CreateFileSegmentSettings & create_settings,
        KeyTransaction & key_transaction,
        const CacheGuard::Lock *);

    bool tryReserveUnlocked(
        const Key & key,
        size_t offset,
        size_t size,
        KeyTransactionPtr key_transaction,
        const CacheGuard::Lock &);

    bool tryReserveImpl(
        IFileCachePriority & priority_queue,
        const Key & key,
        size_t offset,
        size_t size,
        KeyTransactionPtr key_transaction,
        QueryLimit::LockedQueryContext * query_context,
        const CacheGuard::Lock &);

    struct IterateAndLockResult
    {
        IFileCachePriority::IterationResult iteration_result;
        bool lock_key = false;
    };
    using IterateAndCollectLocksFunc = std::function<IterateAndLockResult(const IFileCachePriority::Entry &, KeyTransaction &)>;
    static void iterateAndCollectKeyLocks(
        LockedCachePriority & priority,
        IterateAndCollectLocksFunc && func,
        KeyTransactionsMap & locked_map);
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

struct KeyTransactionCreator
{
    KeyTransactionCreator(
        const FileCacheKey & key_,
        FileCache::KeyMetadataPtr offsets_,
        KeysQueuePtr cleanup_keys_metadata_queue_,
        const FileCache * cache_)
        : key(key_), offsets(offsets_), cleanup_keys_metadata_queue(cleanup_keys_metadata_queue_), cache(cache_) {}

    KeyTransactionPtr create();

    FileCacheKey key;
    FileCache::KeyMetadataPtr offsets;
    KeysQueuePtr cleanup_keys_metadata_queue;
    const FileCache * cache;
};
using KeyTransactionCreatorPtr = std::unique_ptr<KeyTransactionCreator>;

struct KeyTransaction : private boost::noncopyable
{
    using Key = FileCacheKey;

    KeyTransaction(
        const Key & key_,
        FileCache::KeyMetadataPtr offsets_,
        KeysQueuePtr cleanup_keys_metadata_queue_,
        const FileCache * cache_);

    ~KeyTransaction();

    KeyTransactionCreatorPtr getCreator() const { return std::make_unique<KeyTransactionCreator>(key, offsets, cleanup_keys_metadata_queue, cache); }

    void reduceSizeToDownloaded(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    void remove(FileSegmentPtr file_segment, const CacheGuard::Lock &);

    void remove(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    bool isLastHolder(size_t offset);

    FileCache::KeyMetadata & getOffsets() { return *offsets; }
    const FileCache::KeyMetadata & getOffsets() const { return *offsets; }

    std::vector<size_t> delete_offsets;

private:
    void cleanupKeyDirectory() const;
    void relockWithLockedCache();

    Key key;
    const FileCache * cache;

    KeyGuardPtr guard;
    KeyGuard::Lock lock;

    FileCache::KeyMetadataPtr offsets;
    KeysQueuePtr cleanup_keys_metadata_queue;

    Poco::Logger * log;
};

}
