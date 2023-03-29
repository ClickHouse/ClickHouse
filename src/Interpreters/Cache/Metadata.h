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
    FileSegmentPtr file_segment;

    /// Iterator is put here on first reservation attempt, if successful.
    IFileCachePriority::Iterator queue_iterator;

    /// Pointer to file segment is always hold by the cache itself.
    /// Apart from pointer in cache, it can be hold by cache users, when they call
    /// getorSet(), but cache users always hold it via FileSegmentsHolder.
    bool releasable() const { return file_segment.unique(); }

    size_t size() const;

    FileSegmentMetadata(
        FileSegmentPtr file_segment_,
        LockedKeyMetadata & locked_key,
        LockedCachePriority * locked_queue);

    FileSegmentMetadata(FileSegmentMetadata && other) noexcept
        : file_segment(std::move(other.file_segment)), queue_iterator(std::move(other.queue_iterator)) {}
};

struct KeyMetadata : public std::map<size_t, FileSegmentMetadata>, private boost::noncopyable
{
    friend struct LockedKeyMetadata;
    friend struct CacheMetadata;
public:
    explicit KeyMetadata(bool created_base_directory_, CleanupQueue & cleanup_queue_)
        : created_base_directory(created_base_directory_), cleanup_queue(cleanup_queue_) {}

    const FileSegmentMetadata * getByOffset(size_t offset) const;
    FileSegmentMetadata * getByOffset(size_t offset);

    const FileSegmentMetadata * tryGetByOffset(size_t offset) const;
    FileSegmentMetadata * tryGetByOffset(size_t offset);

    std::string toString() const;

    KeyGuard::Lock lock() const { return guard.lock(); }

    bool createdBaseDirectory(const KeyGuard::Lock &) const { return created_base_directory; }

    enum class CleanupState
    {
        NOT_SUBMITTED,
        SUBMITTED_TO_CLEANUP_QUEUE,
        CLEANED_BY_CLEANUP_THREAD,
    };

    CleanupState getCleanupState(const KeyGuard::Lock &) const { return cleanup_state; }

    void addToCleanupQueue(const FileCacheKey & key, const KeyGuard::Lock &);

    void removeFromCleanupQueue(const FileCacheKey & key, const KeyGuard::Lock &);

private:
    mutable KeyGuard guard;
    bool created_base_directory = false;
    CleanupState cleanup_state = CleanupState::NOT_SUBMITTED;
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

    LockedKeyMetadataPtr lockKeyMetadata(const Key & key, KeyMetadataPtr key_metadata, bool return_null_if_in_cleanup_queue = false) const;

    using IterateCacheMetadataFunc = std::function<void(const LockedKeyMetadata &)>;
    void iterate(IterateCacheMetadataFunc && func);

    void doCleanup();

private:
    void addToCleanupQueue(const Key & key, const KeyGuard::Lock &);
    void removeFromCleanupQueue(const Key & key, const KeyGuard::Lock &);

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
        KeyGuard::Lock && key_lock_,
        const std::string & key_path_);

    ~LockedKeyMetadata();

    const FileCacheKey & getKey() const { return key; }

    void createKeyDirectoryIfNot();

    KeyMetadataPtr getKeyMetadata() const { return key_metadata; }

    void removeFileSegment(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    void shrinkFileSegmentToDownloadedSize(size_t offset, const FileSegmentGuard::Lock &, const CacheGuard::Lock &);

    bool isLastOwnerOfFileSegment(size_t offset) const;

    void assertFileSegmentCorrectness(const FileSegment & file_segment) const;

    KeyMetadata::CleanupState getCleanupState() const { return key_metadata->cleanup_state; }

private:
    const FileCacheKey key;
    const std::string key_path;
    const std::shared_ptr<KeyMetadata> key_metadata;
    KeyGuard::Lock lock; /// `lock` must be destructed before `key_metadata`.
    Poco::Logger * log;
};

}
