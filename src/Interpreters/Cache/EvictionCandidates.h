#pragma once
#include <Interpreters/Cache/QueryLimit.h>

namespace DB
{

class EvictionCandidates : private boost::noncopyable
{
public:
    using FinalizeEvictionFunc = std::function<void(const CachePriorityGuard::Lock & lk)>;

    EvictionCandidates();
    ~EvictionCandidates();

    /// Add a new eviction candidate.
    void add(
        const FileSegmentMetadataPtr & candidate,
        LockedKey & locked_key,
        const CachePriorityGuard::Lock &);

    /// Evict all candidates, which were added before via add().
    void evict();

    /// In case of dynamic cache resize we want to remove queue entries in advance.
    /// In ordinary eviction we would do this in finalize().
    void removeQueueEntries(const CachePriorityGuard::Lock &);

    /// Whether finalize() is required.
    bool needFinalize() const;

    /// Set a callback to be called on finalize().
    void onFinalize(FinalizeEvictionFunc && func) { on_finalize.emplace_back(std::move(func)); }

    /// Finalize eviction (remove invalidated queue entries).
    void finalize(
        FileCacheQueryLimit::QueryContext * query_context,
        const CachePriorityGuard::Lock &);

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

    size_t size() const { return candidates_size; }

    auto begin() const { return candidates.begin(); }

    auto end() const { return candidates.end(); }

    void setSpaceHolder(
        size_t size,
        size_t elements,
        IFileCachePriority & priority,
        const CachePriorityGuard::Lock &);

private:

    std::unordered_map<FileCacheKey, KeyCandidates> candidates;
    size_t candidates_size = 0;
    FailedCandidates failed_candidates;

    std::vector<FinalizeEvictionFunc> on_finalize;

    std::vector<IFileCachePriority::IteratorPtr> queue_entries_to_invalidate;
    bool removed_queue_entries = false;

    IFileCachePriority::HoldSpacePtr hold_space;

    LoggerPtr log;
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
