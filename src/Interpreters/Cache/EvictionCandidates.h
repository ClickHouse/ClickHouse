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
        invalidated_queue_entries = std::move(other.invalidated_queue_entries);
        finalize_eviction_func = std::move(other.finalize_eviction_func);
    }
    ~EvictionCandidates();

    void add(LockedKey & locked_key, const FileSegmentMetadataPtr & candidate);

    void add(const EvictionCandidates & other, const CacheGuard::Lock &) { candidates.insert(other.candidates.begin(), other.candidates.end()); }

    void evict();

    void finalize(FileCacheQueryLimit::QueryContext * query_context, const CacheGuard::Lock & lock);

    size_t size() const { return candidates_size; }

    auto begin() const { return candidates.begin(); }

    auto end() const { return candidates.end(); }

    using FinalizeEvictionFunc = std::function<void(const CacheGuard::Lock & lk)>;
    void setFinalizeEvictionFunc(FinalizeEvictionFunc && func) { finalize_eviction_func = func; }

private:
    struct KeyCandidates
    {
        KeyMetadataPtr key_metadata;
        std::vector<FileSegmentMetadataPtr> candidates;
    };

    std::unordered_map<FileCacheKey, KeyCandidates> candidates;
    size_t candidates_size = 0;
    std::vector<IFileCachePriority::IteratorPtr> invalidated_queue_entries;
    FinalizeEvictionFunc finalize_eviction_func;
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
