#pragma once
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Common/logger_useful.h>

namespace DB
{

/// Evictoin info, which contains information about
/// how much size/elements is needed to be evicted,
/// plus holds "space holders", for space which was already available
/// and will now be "hold" as reserved, while we are evicting remaining space.
struct QueueEvictionInfo
{
    size_t size_to_evict = 0;
    size_t elements_to_evict = 0;
    IFileCachePriority::HoldSpacePtr hold_space;

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

/// Aggregated eviction info,
/// contains QueueEvictionInfo per queue_id,
/// aggregates all methods among all queue eviction infos.
class EvictionInfo : public std::map<QueueID, QueueEvictionInfo>, private boost::noncopyable
{
public:
    explicit EvictionInfo(QueueID queue_id, QueueEvictionInfo && info)
    {
        addImpl(queue_id, std::move(info));
    }

    ~EvictionInfo()
    {
        if (on_finish_func)
            on_finish_func();
    }

    std::string toString() const;

    /// Add eviction info for another queue_id,
    /// Throws exception if eviction info with the same queue_id already exists.
    void add(EvictionInfoPtr && info);

    /// Whether actual eviction is needed to be done.
    bool requiresEviction() const { return size_to_evict || elements_to_evict; }
    /// Whether we "hold" some space.
    bool hasHoldSpace() const;
    /// Release hold space if still hold.
    void releaseHoldSpace(const CacheStateGuard::Lock & lock);

    /// Set on finish function,
    /// which will be called when this EvictionInfo object is destructed.
    void setOnFinishFunc(std::function<void()> func) { on_finish_func = func; }

    size_t size_to_evict = 0; /// Total size to evict among all eviction infos.
    size_t elements_to_evict = 0; /// Total elements to evict among all eviction infos.

private:
    void addImpl(const QueueID & queue_id, QueueEvictionInfo && info);

    std::function<void()> on_finish_func;
};

class EvictionCandidates : private boost::noncopyable
{
public:
    using AfterEvictWriteFunc = std::function<void(const CachePriorityGuard::WriteLock & lk)>;
    using AfterEvictStateFunc = std::function<void(const CacheStateGuard::Lock & lk)>;

    EvictionCandidates();
    ~EvictionCandidates();

    size_t size() const { return candidates_size; }
    size_t bytes() const { return candidates_bytes; }

    auto begin() const { return candidates.begin(); }
    auto end() const { return candidates.end(); }

    /// Add a new eviction candidate.
    void add(const FileSegmentMetadataPtr & candidate, LockedKey & locked_key);
    /// Set a callback to be executed after eviction is finished.
    void setAfterEvictWriteFunc(AfterEvictWriteFunc && func) { after_evict_write_func = func; }
    void setAfterEvictStateFunc(AfterEvictStateFunc && func) { after_evict_state_func = func; }

    /// Evict all candidates, which were added before via add().
    void evict();
    /// Finalize eviction state.
    void afterEvictWrite(const CachePriorityGuard::WriteLock & lock);
    void afterEvictState(const CacheStateGuard::Lock & lock);

    /// Used only for dynamic cache resize,
    /// allows to remove queue entries in advance.
    void removeQueueEntries(const CachePriorityGuard::WriteLock &);

    /// Whether finalize() is required.
    bool needFinalize() const;

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

    void invalidateQueueEntries(const CacheStateGuard::Lock &);
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
