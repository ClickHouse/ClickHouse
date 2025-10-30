#pragma once


#include <boost/noncopyable.hpp>
#include <base/isSharedPtrUnique.h>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>
#include <Common/ThreadPool.h>

#include <memory>
#include <shared_mutex>

namespace DB
{

class CleanupQueue;
using CleanupQueuePtr = std::shared_ptr<CleanupQueue>;
class DownloadQueue;
using DownloadQueuePtr = std::shared_ptr<DownloadQueue>;

using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;
class CacheMetadata;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct FileSegmentMetadata : private boost::noncopyable
{
    using Priority = IFileCachePriority;

    explicit FileSegmentMetadata(FileSegmentPtr && file_segment_);

    bool releasable() const { return isSharedPtrUnique(file_segment); }

    size_t size() const;

    bool isEvictingOrRemoved(const CachePriorityGuard::Lock & lock) const
    {
        auto iterator = getQueueIterator();
        if (!iterator || removed)
            return false;
        return iterator->getEntry()->isEvicting(lock);
    }

    bool isEvictingOrRemoved(const LockedKey & lock) const
    {
        auto iterator = getQueueIterator();
        if (!iterator || removed)
            return false;
        return iterator->getEntry()->isEvicting(lock);
    }

    void setEvictingFlag(const LockedKey & locked_key, const CachePriorityGuard::Lock & lock) const
    {
        auto iterator = getQueueIterator();
        if (!iterator)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Iterator is not set");
        iterator->getEntry()->setEvictingFlag(locked_key, lock);
    }

    void setRemovedFlag(const LockedKey &, const CachePriorityGuard::Lock &)
    {
        removed = true;
    }

    void resetEvictingFlag() const
    {
        auto iterator = getQueueIterator();
        if (!iterator)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Iterator is not set");

        const auto & entry = iterator->getEntry();
        chassert(size() == entry->size);
        entry->resetEvictingFlag();
    }

    Priority::IteratorPtr getQueueIterator() const { return file_segment->getQueueIterator(); }

    FileSegmentPtr file_segment;
private:
    bool removed = false;
};

using FileSegmentMetadataPtr = std::shared_ptr<FileSegmentMetadata>;


struct KeyMetadata : private std::map<size_t, FileSegmentMetadataPtr>,
                     private boost::noncopyable,
                     public std::enable_shared_from_this<KeyMetadata>
{
    friend class CacheMetadata;
    friend struct LockedKey;

    using Key = FileCacheKey;
    using iterator = iterator;
    using UserInfo = FileCacheUserInfo;
    using UserID = UserInfo::UserID;

    KeyMetadata(
        const Key & key_,
        const UserInfo & user_id_,
        const CacheMetadata * cache_metadata_,
        bool created_base_directory_ = false);

    enum class KeyState : uint8_t
    {
        ACTIVE,
        REMOVING,
        REMOVED,
    };

    const Key key;
    const UserInfo user;

    LockedKeyPtr lock();

    LockedKeyPtr tryLock();

    bool createBaseDirectory(bool throw_if_failed = false);

    std::string getPath() const;

    std::string getFileSegmentPath(const FileSegment & file_segment) const;

    bool checkAccess(const UserID & user_id_) const;

    void assertAccess(const UserID & user_id_) const;

    /// This method is used for loadMetadata() on server startup,
    /// where we know there is no concurrency on Key and we do not want therefore taking a KeyGuard::Lock,
    /// therefore we use this Unlocked version. This method should not be used anywhere else.
    template< class... Args >
    auto emplaceUnlocked(Args &&... args) { return emplace(std::forward<Args>(args)...); }
    size_t sizeUnlocked() const { return size(); }

private:
    const CacheMetadata * cache_metadata;

    KeyState key_state = KeyState::ACTIVE;
    KeyGuard guard;

    std::atomic<bool> created_base_directory = false;

    LockedKeyPtr lockNoStateCheck();
    LoggerPtr logger() const;
    bool addToDownloadQueue(FileSegmentPtr file_segment);
    void addToCleanupQueue();
};

using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;


class CacheMetadata : private boost::noncopyable
{
    friend struct KeyMetadata;
public:
    using Key = FileCacheKey;
    using IterateFunc = std::function<void(LockedKey &)>;
    using UserInfo = FileCacheUserInfo;
    using UserID = UserInfo::UserID;

    explicit CacheMetadata(
        const std::string & path_,
        size_t background_download_queue_size_limit_,
        size_t background_download_threads_,
        bool write_cache_per_user_directory_);

    void startup();

    bool isEmpty() const;

    const String & getBaseDirectory() const { return path; }

    String getKeyPath(const Key & key, const UserInfo & user) const;

    String getFileSegmentPath(
        const Key & key,
        size_t offset,
        FileSegmentKind segment_kind,
        const UserInfo & user) const;

    void iterate(IterateFunc && func, const UserID & user_id);

    enum class KeyNotFoundPolicy : uint8_t
    {
        THROW,
        THROW_LOGICAL,
        CREATE_EMPTY,
        RETURN_NULL,
    };

    KeyMetadataPtr getKeyMetadata(
        const Key & key,
        KeyNotFoundPolicy key_not_found_policy,
        const UserInfo & user,
        bool is_initial_load = false);

    LockedKeyPtr lockKeyMetadata(
        const Key & key,
        KeyNotFoundPolicy key_not_found_policy,
        const UserInfo & user,
        bool is_initial_load = false);

    void removeKey(const Key & key, bool if_exists, bool if_releasable, const UserID & user_id);
    void removeAllKeys(bool if_releasable, const UserID & user_id);

    void shutdown();

    bool setBackgroundDownloadThreads(size_t threads_num);
    size_t getBackgroundDownloadThreads() const { return download_threads.size(); }

    bool setBackgroundDownloadQueueSizeLimit(size_t size);

    bool isBackgroundDownloadEnabled();

private:
    static constexpr size_t buckets_num = 1024;

    const std::string path;
    const CleanupQueuePtr cleanup_queue;
    const DownloadQueuePtr download_queue;
    const bool write_cache_per_user_directory;

    LoggerPtr log;
    mutable std::shared_mutex key_prefix_directory_mutex;

    struct MetadataBucket : public std::unordered_map<FileCacheKey, KeyMetadataPtr>
    {
        CacheMetadataGuard::Lock lock() const;
    private:
        mutable CacheMetadataGuard guard;
    };

    std::vector<MetadataBucket> metadata_buckets{buckets_num};

    struct DownloadThread
    {
        std::unique_ptr<ThreadFromGlobalPool> thread;
        bool stop_flag{false};
    };

    std::atomic<size_t> download_threads_num;
    std::vector<std::shared_ptr<DownloadThread>> download_threads;
    std::unique_ptr<ThreadFromGlobalPool> cleanup_thread;

    static String getFileNameForFileSegment(size_t offset, FileSegmentKind segment_kind);

    MetadataBucket & getMetadataBucket(const Key & key);
    void downloadImpl(FileSegment & file_segment, std::optional<Memory<>> & memory) const;
    MetadataBucket::iterator removeEmptyKey(
        MetadataBucket & bucket,
        MetadataBucket::iterator it,
        LockedKey &,
        const CacheMetadataGuard::Lock &);

    void downloadThreadFunc(const bool & stop_flag);

    /// Firstly, this cleanup does not delete cache files,
    /// but only empty keys from cache_metadata_map and key (prefix) directories from fs.
    /// Secondly, it deletes those only if arose as a result of
    /// (1) eviction in FileCache::tryReserve();
    /// (2) removal of cancelled non-downloaded file segments after FileSegment::complete().
    /// which does not include removal of cache files because of FileCache::removeKey/removeAllKeys,
    /// triggered by removal of source files from objects storage.
    /// E.g. number of elements submitted to background cleanup should remain low.
    void cleanupThreadFunc();
};


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
    using Key = FileCacheKey;

    explicit LockedKey(std::shared_ptr<KeyMetadata> key_metadata_);

    ~LockedKey();

    const Key & getKey() const { return key_metadata->key; }

    auto begin() const { return key_metadata->begin(); }
    auto rbegin() const { return key_metadata->rbegin(); }

    auto end() const { return key_metadata->end(); }
    auto rend() const { return key_metadata->rend(); }

    bool empty() const { return key_metadata->empty(); }
    auto lower_bound(size_t size) const { return key_metadata->lower_bound(size); } /// NOLINT
    template< class... Args >
    auto emplace(Args &&... args) { return key_metadata->emplace(std::forward<Args>(args)...); }

    std::shared_ptr<const FileSegmentMetadata> getByOffset(size_t offset) const;
    std::shared_ptr<FileSegmentMetadata> getByOffset(size_t offset);

    std::shared_ptr<const FileSegmentMetadata> tryGetByOffset(size_t offset) const;
    std::shared_ptr<FileSegmentMetadata> tryGetByOffset(size_t offset);

    KeyMetadata::KeyState getKeyState() const { return key_metadata->key_state; }

    std::shared_ptr<const KeyMetadata> getKeyMetadata() const { return key_metadata; }
    std::shared_ptr<KeyMetadata> getKeyMetadata() { return key_metadata; }

    bool removeAllFileSegments(bool if_releasable = true);

    KeyMetadata::iterator removeFileSegment(
        size_t offset,
        const FileSegmentGuard::Lock &,
        bool can_be_broken = false,
        bool invalidate_queue_entry = true);

    KeyMetadata::iterator removeFileSegment(
        size_t offset,
        bool can_be_broken = false,
        bool invalidate_queue_entry = true);

    bool addToDownloadQueue(size_t offset, const FileSegmentGuard::Lock &);

    bool isLastOwnerOfFileSegment(size_t offset) const;

    std::optional<FileSegment::Range> hasIntersectingRange(const FileSegment::Range & range) const;

    void removeFromCleanupQueue();

    void markAsRemoved();

    std::vector<FileSegment::Info> sync();

    std::string toString() const;

private:
    KeyMetadata::iterator removeFileSegmentImpl(
        KeyMetadata::iterator it,
        const FileSegmentGuard::Lock &,
        bool can_be_broken = false,
        bool invalidate_queue_entry = true);

    const std::shared_ptr<KeyMetadata> key_metadata;
    KeyGuard::Lock lock; /// `lock` must be destructed before `key_metadata`.
};

}
