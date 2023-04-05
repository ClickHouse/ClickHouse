#pragma once
#include <boost/noncopyable.hpp>
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileSegment.h>

namespace DB
{
class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
struct LockedKeyMetadata;
using LockedKeyMetadataPtr = std::shared_ptr<LockedKeyMetadata>;
class LockedCachePriority;
struct KeysQueue;
struct CleanupQueue;


struct FileSegmentMetadata : private boost::noncopyable
{
    FileSegmentMetadata(
        FileSegmentPtr file_segment_,
        LockedKeyMetadata & locked_key,
        LockedCachePriority * locked_queue);

    /// Pointer to file segment is always hold by the cache itself.
    /// Apart from pointer in cache, it can be hold by cache users, when they call
    /// getorSet(), but cache users always hold it via FileSegmentsHolder.
    bool releasable() const { return file_segment.unique(); }

    IFileCachePriority::Iterator & getQueueIterator() const;

    bool valid() const { return *evict_flag == false; }

    size_t size() const;

    FileSegmentPtr file_segment;

    using EvictFlag = std::shared_ptr<std::atomic<bool>>;
    EvictFlag evict_flag = std::make_shared<std::atomic<bool>>(false);
    struct EvictHolder : boost::noncopyable
    {
        explicit EvictHolder(EvictFlag evict_flag_) : evict_flag(evict_flag_) { *evict_flag = true; }
        ~EvictHolder() { *evict_flag = false; }
        EvictFlag evict_flag;
    };
    using EvictHolderPtr = std::unique_ptr<EvictHolder>;

    EvictHolderPtr getEvictHolder() { return std::make_unique<EvictHolder>(evict_flag); }
};

struct KeyMetadata : public std::map<size_t, FileSegmentMetadata>, private boost::noncopyable
{
    friend struct LockedKeyMetadata;
public:
    explicit KeyMetadata(bool created_base_directory_, CleanupQueue & cleanup_queue_)
        : created_base_directory(created_base_directory_), cleanup_queue(cleanup_queue_) {}

    enum class KeyState
    {
        ACTIVE,
        REMOVING,
        REMOVED,
    };

    bool created_base_directory = false;

private:
    KeyGuard::Lock lock() const { return guard.lock(); }

    KeyState key_state = KeyState::ACTIVE;

    mutable KeyGuard guard;
    CleanupQueue & cleanup_queue;
};

using KeyMetadataPtr = std::shared_ptr<KeyMetadata>;

struct CleanupQueue
{
    friend struct CacheMetadata;
public:
    void add(const FileCacheKey & key);
    void remove(const FileCacheKey & key);
    size_t getSize() const;

private:
    bool tryPop(FileCacheKey & key);

    std::unordered_set<FileCacheKey> keys;
    mutable std::mutex mutex;
};

struct CacheMetadata : public std::unordered_map<FileCacheKey, KeyMetadataPtr>, private boost::noncopyable
{
public:
    using Key = FileCacheKey;

    explicit CacheMetadata(const std::string & base_directory_) : base_directory(base_directory_) {}

    String getPathInLocalCache(const Key & key, size_t offset, FileSegmentKind segment_kind) const;

    String getPathInLocalCache(const Key & key) const;

    enum class KeyNotFoundPolicy
    {
        THROW,
        CREATE_EMPTY,
        RETURN_NULL,
    };
    LockedKeyMetadataPtr lockKeyMetadata(const Key & key, KeyNotFoundPolicy key_not_found_policy, bool is_initial_load = false);

    LockedKeyMetadataPtr lockKeyMetadata(const Key & key, KeyMetadataPtr key_metadata, bool skip_if_in_cleanup_queue = false) const;

    using IterateCacheMetadataFunc = std::function<void(const LockedKeyMetadata &)>;
    void iterate(IterateCacheMetadataFunc && func);

    void doCleanup();

private:
    const std::string base_directory;
    CacheMetadataGuard guard;
    CleanupQueue cleanup_queue;
};


/**
 * `LockedKeyMetadata` is an object which makes sure that as long as it exists the following is true:
 * 1. the key cannot be removed from cache
 *    (Why: this LockedKeyMetadata locks key metadata mutex in ctor, unlocks it in dtor, and so
 *    when key is going to be deleted, key mutex is also locked.
 *    Why it cannot be the other way round? E.g. that ctor of LockedKeyMetadata locks the key
 *    right after it was deleted? This case it taken into consideration in createLockedKeyMetadata())
 * 2. the key cannot be modified, e.g. new offsets cannot be added to key; already existing
 *    offsets cannot be deleted from the key
 * And also provides some methods which allow the owner of this LockedKeyMetadata object to do such
 * modification of the key (adding/deleting offsets) and deleting the key from cache.
 */
struct LockedKeyMetadata : private boost::noncopyable
{
    LockedKeyMetadata(
        const FileCacheKey & key_,
        std::shared_ptr<KeyMetadata> key_metadata_,
        const std::string & key_path_);

    ~LockedKeyMetadata();

    const FileCacheKey & getKey() const { return key; }

    auto begin() const { return key_metadata->begin(); }
    auto end() const { return key_metadata->end(); }

    const FileSegmentMetadata * getByOffset(size_t offset) const;
    FileSegmentMetadata * getByOffset(size_t offset);

    const FileSegmentMetadata * tryGetByOffset(size_t offset) const;
    FileSegmentMetadata * tryGetByOffset(size_t offset);

    KeyMetadata::KeyState getKeyState() const { return key_metadata->key_state; }

    KeyMetadataPtr getKeyMetadata() const { return key_metadata; }
    KeyMetadataPtr getKeyMetadata() { return key_metadata; }

    void removeFileSegment(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    void shrinkFileSegmentToDownloadedSize(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    bool isLastOwnerOfFileSegment(size_t offset) const;

    void assertFileSegmentCorrectness(const FileSegment & file_segment) const;

    bool isRemovalCandidate() const;

    bool markAsRemovalCandidate(size_t offset);

    void removeFromCleanupQueue();

    bool markAsRemoved();

    std::string toString() const;

private:
    const FileCacheKey key;
    const std::string key_path;
    const std::shared_ptr<KeyMetadata> key_metadata;
    KeyGuard::Lock lock; /// `lock` must be destructed before `key_metadata`.
    Poco::Logger * log;
};

}
