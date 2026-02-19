#pragma once
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/UserCacheUsage.h>
#include <Interpreters/Cache/UserInfo.h>
#include <deque>

namespace DB
{

/// Eviction info:
/// - contains information about how much size/elements is needed to be evicted
/// - holds "space holders", for space which was already available
///   and will now be "held" as reserved, while we are evicting remaining space.
/// If releaseHoldSpace() is not called,
/// held space will be automatically released in destructor of HoldSpacePtr.
struct QueueEvictionInfo;
using QueueEvictionInfoPtr = std::unique_ptr<QueueEvictionInfo>;

struct QueueEvictionInfo
{
    explicit QueueEvictionInfo(
        const std::string & description_,
        const FileCacheUserInfo::UserID & user_id_) : description(description_), user_id(user_id_) {}

    const std::string description;
    const FileCacheUserInfo::UserID user_id;

    size_t size_to_evict = 0;
    size_t elements_to_evict = 0;
    IFileCachePriority::HoldSpacePtr hold_space;

    void merge(QueueEvictionInfoPtr other);

    std::string toString() const;
    /// Whether actual eviction is needed to be done.
    bool requiresEviction() const { return size_to_evict || elements_to_evict; }
    /// Whether we "hold" some space.
    bool hasHoldSpace() const { return hold_space != nullptr; }
    /// Release hold space if still hold.
    void releaseHoldSpace(const CacheStateGuard::Lock & lock);
};
using QueueID = size_t;

class EvictionInfo;
using EvictionInfoPtr = std::unique_ptr<EvictionInfo>;

/// Aggregated eviction info:
/// - contains QueueEvictionInfo per queue_id
/// - aggregates all methods among all QueueEvictionInfo's.
class EvictionInfo : public std::map<QueueID, QueueEvictionInfoPtr>, private boost::noncopyable
{
public:
    EvictionInfo() = default;
    /// Creates eviction info from a single QueueEvictionInfo.
    /// More infos can be added via add() method.
    explicit EvictionInfo(QueueID queue_id, QueueEvictionInfoPtr info);

    /// Get eviction info by queue id.
    const QueueEvictionInfo & get(const QueueID & queue_id) const;
    /// Add eviction info under the queue_id.
    /// Throws exception if eviction info with the same queue_id already exists.
    void add(EvictionInfoPtr && info);
    void addOrUpdate(EvictionInfoPtr && info);

    size_t getSizeToEvict() const { return size_to_evict; }
    size_t getElementsToEvict() const { return elements_to_evict; }
    /// Whether actual eviction is needed to be done.
    bool requiresEviction() const { return size_to_evict || elements_to_evict; }
    /// Whether we "hold" some space.
    bool hasHoldSpace() const;
    /// Release hold space if still hold.
    void releaseHoldSpace(const CacheStateGuard::Lock & lock);

    std::string toString() const;

    void setCacheUsage(std::vector<CacheUsagePtr> && usage) { sorted_cache_usage = std::move(usage); }
    std::vector<CacheUsagePtr> getCacheUsage() const { return sorted_cache_usage; }

private:
    /// If `merge_if_exists` is true
    /// (meaning that eviction info by `queue_id` already exists),
    /// combine two eviction info's into one.
    void addImpl(const QueueID & queue_id, QueueEvictionInfoPtr info, bool merge_if_exists);

    size_t size_to_evict = 0; /// Total size to evict among all eviction infos.
    size_t elements_to_evict = 0; /// Total elements to evict among all eviction infos.

    std::vector<CacheUsagePtr> sorted_cache_usage;
};

class EvictionCandidates : private boost::noncopyable
{
public:
    using AfterEvictWriteFunc = std::function<void(const CachePriorityGuard::WriteLock & lk)>;
    using AfterEvictStateFunc = std::function<void(const CacheStateGuard::Lock & lk)>;

    EvictionCandidates();
    ~EvictionCandidates();

    /// Total number of eviction candidates.
    size_t size() const { return candidates_size; }
    /// Total size in bytes of all eviction candidates.
    size_t bytes() const { return candidates_bytes; }

    auto begin(this auto && self) { return self.candidates.begin(); }
    auto end(this auto&& self) { return self.candidates.end(); }

    /// Add a new eviction candidate.
    void add(const FileSegmentMetadataPtr & candidate, LockedKey & locked_key);
    /// Set a callback to be executed after eviction is finished.
    /// "write" func modifies priority queue structure.
    /// "state" func modifies cache size/elements counters.
    void setAfterEvictWriteFunc(AfterEvictWriteFunc && func) { after_evict_write_func = std::move(func); }
    void setAfterEvictStateFunc(AfterEvictStateFunc && func) { after_evict_state_func = std::move(func); }

    /// Evict all candidates, which were added before via add().
    void evict();
    /// Execute "after eviction callbacks".
    /// "write" callback must be executed before "state" callback.
    void afterEvictWrite(const CachePriorityGuard::WriteLock & lock);
    void afterEvictState(const CacheStateGuard::Lock & lock);

    /// Whether calling afterEvictWrite() is required.
    /// (Can be used to avoid taking write lock)
    bool requiresAfterEvictWrite() const { return bool(after_evict_write_func); }
    /// Whether calling afterEvictState() is required.
    /// (Can be used to avoid taking state lock)
    bool requiresAfterEvictState() const { return bool(after_evict_state_func) || !queue_entries_to_invalidate.empty(); }

    /// Used only for dynamic cache resize,
    /// allows to remove queue entries in advance.
    void removeQueueEntries(const CachePriorityGuard::WriteLock &);

    struct KeyCandidates
    {
        KeyMetadataPtr key_metadata;
        std::vector<FileSegmentMetadataPtr> candidates;
        std::vector<std::string> error_messages;
    };
    /// Get eviction candidates which failed to be evicted during evict().
    struct FailedCandidates
    {
        std::vector<KeyCandidates> failed_candidates_per_key;
        size_t total_cache_size = 0;
        size_t total_cache_elements = 0;

        size_t size() const { return failed_candidates_per_key.size(); }

        std::string getFirstErrorMessage() const;
    };

    FailedCandidates getFailedCandidates() const { return failed_candidates; }

private:

    std::unordered_map<FileCacheKey, KeyCandidates> candidates;
    size_t candidates_size = 0;
    size_t candidates_bytes = 0;
    FailedCandidates failed_candidates;

    AfterEvictWriteFunc after_evict_write_func;
    AfterEvictStateFunc after_evict_state_func;

    std::vector<IFileCachePriority::IteratorPtr> queue_entries_to_invalidate;
    bool removed_queue_entries = false;

    IFileCachePriority::HoldSpacePtr hold_space;

    LoggerPtr log;
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
