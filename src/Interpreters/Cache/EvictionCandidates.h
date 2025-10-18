#pragma once
#include <Interpreters/Cache/QueryLimit.h>

namespace DB
{

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
