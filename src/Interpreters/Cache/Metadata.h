#pragma once
#include <boost/noncopyable.hpp>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>
#include <shared_mutex>

namespace DB
{

class CleanupQueue;
using CleanupQueuePtr = std::shared_ptr<CleanupQueue>;
class DownloadQueue;
using DownloadQueuePtr = std::shared_ptr<DownloadQueue>;
using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;


struct FileSegmentMetadata : private boost::noncopyable
{
    using Priority = IFileCachePriority;

    explicit FileSegmentMetadata(FileSegmentPtr && file_segment_);

    bool releasable() const { return file_segment.unique(); }

    size_t size() const;

    bool evicting() const { return removal_candidate.load(); }

    Priority::Iterator getQueueIterator() const { return file_segment->getQueueIterator(); }

    FileSegmentPtr file_segment;
    std::atomic<bool> removal_candidate{false};
};

using FileSegmentMetadataPtr = std::shared_ptr<FileSegmentMetadata>;


struct KeyMetadata : public std::map<size_t, FileSegmentMetadataPtr>,
                     private boost::noncopyable,
                     public std::enable_shared_from_this<KeyMetadata>
{
    friend struct LockedKey;
    using Key = FileCacheKey;

    KeyMetadata(
        const Key & key_,
        const std::string & key_path_,
        CleanupQueuePtr cleanup_queue_,
        DownloadQueuePtr download_queue_,
        Poco::Logger * log_,
        std::shared_mutex & key_prefix_directory_mutex_,
        bool created_base_directory_ = false);

    enum class KeyState
    {
        ACTIVE,
        REMOVING,
        REMOVED,
    };

    const Key key;
    const std::string key_path;

    LockedKeyPtr lock();

    /// Return nullptr if key has non-ACTIVE state.
    LockedKeyPtr tryLock();

    LockedKeyPtr lockNoStateCheck();

    bool createBaseDirectory();

    std::string getFileSegmentPath(const FileSegment & file_segment) const;

private:
    KeyState key_state = KeyState::ACTIVE;
    KeyGuard guard;
    const CleanupQueuePtr cleanup_queue;
    const DownloadQueuePtr download_queue;
    std::shared_mutex & key_prefix_directory_mutex;
    std::atomic<bool> created_base_directory = false;
    Poco::Logger * log;
};

using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;


struct CacheMetadata : public std::unordered_map<FileCacheKey, KeyMetadataPtr>, private boost::noncopyable
{
public:
    using Key = FileCacheKey;
    using IterateFunc = std::function<void(LockedKey &)>;

    explicit CacheMetadata(const std::string & path_);

    const String & getBaseDirectory() const { return path; }

    String getPathForFileSegment(
        const Key & key,
        size_t offset,
        FileSegmentKind segment_kind) const;

    String getPathForKey(const Key & key) const;
    static String getFileNameForFileSegment(size_t offset, FileSegmentKind segment_kind);

    void iterate(IterateFunc && func);

    enum class KeyNotFoundPolicy
    {
        THROW,
        THROW_LOGICAL,
        CREATE_EMPTY,
        RETURN_NULL,
    };

    KeyMetadataPtr getKeyMetadata(
        const Key & key,
        KeyNotFoundPolicy key_not_found_policy,
        bool is_initial_load = false);

    LockedKeyPtr lockKeyMetadata(
        const Key & key,
        KeyNotFoundPolicy key_not_found_policy,
        bool is_initial_load = false);

    void removeKey(const Key & key, bool if_exists, bool if_releasable);
    void removeAllKeys(bool if_releasable);

    void cancelCleanup();

    /// Firstly, this cleanup does not delete cache files,
    /// but only empty keys from cache_metadata_map and key (prefix) directories from fs.
    /// Secondly, it deletes those only if arose as a result of
    /// (1) eviction in FileCache::tryReserve();
    /// (2) removal of cancelled non-downloaded file segments after FileSegment::complete().
    /// which does not include removal of cache files because of FileCache::removeKey/removeAllKeys,
    /// triggered by removal of source files from objects storage.
    /// E.g. number of elements submitted to background cleanup should remain low.
    void cleanupThreadFunc();

    void downloadThreadFunc();

    void cancelDownload();

private:
    CacheMetadataGuard::Lock lockMetadata() const;
    const std::string path; /// Cache base path
    mutable CacheMetadataGuard guard;
    CleanupQueuePtr cleanup_queue;
    DownloadQueuePtr download_queue;
    std::shared_mutex key_prefix_directory_mutex;
    Poco::Logger * log;

    void downloadImpl(FileSegment & file_segment, std::optional<Memory<>> & memory);
    iterator removeEmptyKey(iterator it, LockedKey &, const CacheMetadataGuard::Lock &);
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
    auto end() const { return key_metadata->end(); }

    std::shared_ptr<const FileSegmentMetadata> getByOffset(size_t offset) const;
    std::shared_ptr<FileSegmentMetadata> getByOffset(size_t offset);

    std::shared_ptr<const FileSegmentMetadata> tryGetByOffset(size_t offset) const;
    std::shared_ptr<FileSegmentMetadata> tryGetByOffset(size_t offset);

    KeyMetadata::KeyState getKeyState() const { return key_metadata->key_state; }

    std::shared_ptr<const KeyMetadata> getKeyMetadata() const { return key_metadata; }
    std::shared_ptr<KeyMetadata> getKeyMetadata() { return key_metadata; }

    bool removeAllFileSegments(bool if_releasable = true);

    KeyMetadata::iterator removeFileSegment(size_t offset, const FileSegmentGuard::Lock &, bool can_be_broken = false);
    KeyMetadata::iterator removeFileSegment(size_t offset, bool can_be_broken = false);

    void shrinkFileSegmentToDownloadedSize(size_t offset, const FileSegmentGuard::Lock &);

    void addToDownloadQueue(size_t offset, const FileSegmentGuard::Lock &);

    bool isLastOwnerOfFileSegment(size_t offset) const;

    std::optional<FileSegment::Range> hasIntersectingRange(const FileSegment::Range & range) const;

    void removeFromCleanupQueue();

    void markAsRemoved();

    FileSegments sync();

    std::string toString() const;

private:
    KeyMetadata::iterator removeFileSegmentImpl(KeyMetadata::iterator it, const FileSegmentGuard::Lock &, bool can_be_broken = false);

    const std::shared_ptr<KeyMetadata> key_metadata;
    KeyGuard::Lock lock; /// `lock` must be destructed before `key_metadata`.
};

}
