#pragma once
#include <Interpreters/Cache/QueryLimit.h>

namespace DB
{

class EvictionCandidates
{
public:
    EvictionCandidates() = default;
    EvictionCandidates(EvictionCandidates && other) noexcept
    {
        candidates = std::move(other.candidates);
        candidates_size = std::move(other.candidates_size);
        queue_entries_to_invalidate = std::move(other.queue_entries_to_invalidate);
        finalize_eviction_func = std::move(other.finalize_eviction_func);
    }
    using FinalizeEvictionFunc = std::function<void(const CachePriorityGuard::Lock & lk)>;

    ~EvictionCandidates();

    void add(
        const FileSegmentMetadataPtr & candidate,
        LockedKey & locked_key,
        const CachePriorityGuard::Lock &);

    void add(const EvictionCandidates & other, const CachePriorityGuard::Lock &) { candidates.insert(other.candidates.begin(), other.candidates.end()); }

    void evict();

    void onFinalize(FinalizeEvictionFunc && func) { on_finalize.emplace_back(std::move(func)); }

    void finalize(
        FileCacheQueryLimit::QueryContext * query_context,
        const CachePriorityGuard::Lock &);

    size_t size() const { return candidates_size; }

    auto begin() const { return candidates.begin(); }

    auto end() const { return candidates.end(); }

    void setSpaceHolder(
        size_t size,
        size_t elements,
        IFileCachePriority & priority,
        const CachePriorityGuard::Lock &);

private:
    struct KeyCandidates
    {
        KeyMetadataPtr key_metadata;
        std::vector<FileSegmentMetadataPtr> candidates;
    };

    std::unordered_map<FileCacheKey, KeyCandidates> candidates;
    size_t candidates_size = 0;

    std::vector<FinalizeEvictionFunc> on_finalize;
    std::vector<IFileCachePriority::IteratorPtr> queue_entries_to_invalidate;
    IFileCachePriority::HoldSpacePtr hold_space;
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
