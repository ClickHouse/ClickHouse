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
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/Guards.h>


namespace DB
{

struct KeyTransaction;
using KeyTransactionPtr = std::shared_ptr<KeyTransaction>;
using KeyPrefix = std::string;
using KeyTransactionsMap = std::unordered_map<KeyPrefix, KeyTransactionPtr>;


/// Local cache for remote filesystem files, represented as a set of non-overlapping non-empty file segments.
/// Different caching algorithms are implemented using IFileCachePriority.
class FileCache : private boost::noncopyable
{
friend class FileSegmentRangeWriter;
friend struct KeyTransaction;
friend struct KeyTransactionCreator;
friend struct FileSegmentsHolder;
friend class FileSegment;

public:
    using Key = DB::FileCacheKey;

    FileCache(const String & cache_base_path_, const FileCacheSettings & cache_settings_);

    ~FileCache() = default;

    void initialize();

    const String & getBasePath() const { return cache_base_path; }

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
    void removeIfExists(const Key & key);

    /// Remove files by `key`. Will not remove files which are used at the moment.
    void removeAllReleasable();

    bool tryReserve(const Key & key, size_t offset, size_t size);

    static Key createKeyForPath(const String & path);

    String getPathInLocalCache(const Key & key, size_t offset, FileSegmentKind segment_kind) const;

    String getPathInLocalCache(const Key & key) const;

    std::vector<String> tryGetCachePaths(const Key & key);

    size_t getUsedCacheSize() const;

    size_t getFileSegmentsNum() const;

    static bool readThrowCacheAllowed();

    struct QueryContextHolder;
    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;
    QueryContextHolderPtr getQueryContextHolder(const String & query_id, const ReadSettings & settings);

    FileSegmentsHolderPtr getSnapshot();

    FileSegmentsHolderPtr getSnapshot(const Key & key);

    FileSegmentsHolderPtr dumpQueue();

    void assertCacheCorrectness();

private:
    using KeyAndOffset = FileCacheKeyAndOffset;

    String cache_base_path;

    const size_t max_size;
    const size_t max_element_size;
    const size_t max_file_segment_size;

    const bool allow_persistent_files;
    const bool enable_bypass_cache_with_threshold;
    const size_t bypass_cache_threshold;

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
            IFileCachePriority & priority_queue);

        FileSegmentCell(FileSegmentCell && other) noexcept
            : file_segment(std::move(other.file_segment)), queue_iterator(std::move(other.queue_iterator)) {}
    };

    struct CacheCells : public std::map<size_t, FileSegmentCell>
    {
        const FileSegmentCell * get(size_t offset) const;
        FileSegmentCell * get(size_t offset);

        const FileSegmentCell * tryGet(size_t offset) const;
        FileSegmentCell * tryGet(size_t offset);

        std::string toString() const;

        bool created_base_directory = false;
    };
    using CacheCellsPtr = std::shared_ptr<CacheCells>;

    mutable CacheGuard cache_guard;

    enum class InitializationState
    {
        NOT_INITIALIZED,
        INITIALIZING,
        INITIALIZED,
        FAILED,
    };
    InitializationState initialization_state = InitializationState::NOT_INITIALIZED;
    mutable std::condition_variable initialization_cv;
    std::exception_ptr initialization_exception;

    using CachedFiles = std::unordered_map<Key, CacheCellsPtr>;
    CachedFiles files;

    using KeyPrefix = std::string;
    using KeysLocksMap = std::unordered_map<KeyPrefix, KeyPrefixGuardPtr>;
    KeysLocksMap keys_locks;

    enum class KeyNotFoundPolicy
    {
        THROW,
        CREATE_EMPTY,
        RETURN_NULL,
    };

    KeyTransactionPtr createKeyTransaction(const Key & key, KeyNotFoundPolicy key_not_found_policy, bool assert_initialized = true);

    KeyTransactionCreatorPtr getKeyTransactionCreator(const Key & key, KeyTransaction & key_transaction);

    FileCachePriorityPtr main_priority;

    struct HitsCountStash
    {
        HitsCountStash(size_t max_stash_queue_size_, size_t cache_hits_threshold_, FileCachePriorityPtr queue_)
            : max_stash_queue_size(max_stash_queue_size_)
            , cache_hits_threshold(cache_hits_threshold_)
            , queue(std::move(queue_)) {}

        const size_t max_stash_queue_size;
        const size_t cache_hits_threshold;

        auto lock() const { return queue->lock(); }

        FileCachePriorityPtr queue;

        using Records = std::unordered_map<KeyAndOffset, IFileCachePriority::Iterator, FileCacheKeyAndOffsetHash>;
        Records records;
    };

    mutable std::unique_ptr<HitsCountStash> stash;

    struct QueryLimit
    {
        /// Used to track and control the cache access of each query.
        /// Through it, we can realize the processing of different queries by the cache layer.
        struct QueryContext
        {
            std::mutex mutex;
            HitsCountStash::Records records;
            FileCachePriorityPtr priority;

            size_t cache_size = 0;
            size_t max_cache_size;

            bool skip_download_if_exceeds_query_cache;

            QueryContext(size_t max_cache_size_, bool skip_download_if_exceeds_query_cache_);

            size_t getMaxCacheSize() const { return max_cache_size; }

            size_t getCacheSize() const { return cache_size; }

            IFileCachePriority & getPriority() const { return *priority; }

            bool isSkipDownloadIfExceed() const { return skip_download_if_exceeds_query_cache; }

            void remove(const Key & key, size_t offset, size_t size, KeyTransaction & key_transaction);

            void reserve(const Key & key, size_t offset, size_t size, KeyTransaction & key_transaction);

            void use(const Key & key, size_t offset, KeyTransaction & key_transaction);
        };

        using QueryContextPtr = std::shared_ptr<QueryContext>;
        using QueryContextMap = std::unordered_map<String, QueryContextPtr>;

        QueryContextMap query_map;

        QueryContextPtr tryGetQueryContext(const CachePriorityQueueGuard::Lock & lock);

        void removeQueryContext(const std::string & query_id, const CachePriorityQueueGuard::Lock & lock);

        QueryContextPtr getOrSetQueryContext(
            const std::string & query_id,
            const ReadSettings & settings,
            const CachePriorityQueueGuard::Lock & lock);
    };

    using QueryLimitPtr = std::unique_ptr<QueryLimit>;
    QueryLimitPtr query_limit;

public:
    /// Save a query context information, and adopt different cache policies
    /// for different queries through the context cache layer.
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
    void assertInitializedUnlocked(CacheGuard::Lock &) const;

    void loadCacheInfoIntoMemory();

    FileSegments getImpl(
        const Key & key,
        const FileSegment::Range & range,
        const KeyTransaction & key_transaction) const;

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

    CacheCells::iterator addCell(
        const Key & key,
        size_t offset,
        size_t size,
        FileSegment::State state,
        const CreateFileSegmentSettings & create_settings,
        KeyTransaction & key_transaction);

    bool tryReserveUnlocked(
        const Key & key,
        size_t offset,
        size_t size,
        KeyTransactionPtr key_transaction);

    bool tryReserveInCache(
        const Key & key,
        size_t offset,
        size_t size,
        QueryLimit::QueryContextPtr query_context,
        KeyTransactionPtr key_transaction);

    bool tryReserveInQueryCache(
        const Key & key,
        size_t offset,
        size_t size,
        QueryLimit::QueryContextPtr query_context,
        KeyTransactionPtr key_transaction);

    void removeKeyDirectoryIfExists(const Key & key, const KeyPrefixGuard::Lock & lock) const;

    struct IterateAndLockResult
    {
        IFileCachePriority::IterationResult iteration_result;
        bool lock_key = false;
    };
    using IterateAndCollectLocksFunc = std::function<IterateAndLockResult(const IFileCachePriority::Entry &, KeyTransaction &)>;
    static void iterateAndCollectKeyLocks(
        IFileCachePriority & priority,
        IterateAndCollectLocksFunc && func,
        const CachePriorityQueueGuard::Lock & lock,
        KeyTransactionsMap & locked_map);
};

struct KeyTransactionCreator
{
    KeyTransactionCreator(
        KeyPrefixGuardPtr guard_, FileCache::CacheCellsPtr offsets_)
        : guard(guard_) , offsets(offsets_) {}

    KeyTransactionPtr create();

    KeyPrefixGuardPtr guard;
    FileCache::CacheCellsPtr offsets;
};
using KeyTransactionCreatorPtr = std::unique_ptr<KeyTransactionCreator>;

struct KeyTransaction : private boost::noncopyable
{
    using Key = FileCacheKey;

    KeyTransaction(KeyPrefixGuardPtr guard_, FileCache::CacheCellsPtr offsets_, std::shared_ptr<CachePriorityQueueGuard::Lock> queue_lock_ = nullptr);

    KeyTransactionCreatorPtr getCreator() { return std::make_unique<KeyTransactionCreator>(guard, offsets); }

    void remove(FileSegmentPtr file_segment);

    void reduceSizeToDownloaded(const Key & key, size_t offset, const FileSegmentGuard::Lock &);

    void remove(const Key & key, size_t offset, const FileSegmentGuard::Lock &);

    bool isLastHolder(size_t offset);

    FileCache::CacheCells & getOffsets() { return *offsets; }
    const FileCache::CacheCells & getOffsets() const { return *offsets; }

    std::vector<size_t> delete_offsets;

    const CachePriorityQueueGuard::Lock & getQueueLock() const
    {
        if (!queue_lock)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Queue is not locked");
        return *queue_lock;
    }

private:
    KeyPrefixGuardPtr guard;
    const KeyPrefixGuard::Lock lock;

    FileCache::CacheCellsPtr offsets;

    Poco::Logger * log;
public:
    std::shared_ptr<CachePriorityQueueGuard::Lock> queue_lock;

};

}
