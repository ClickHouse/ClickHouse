#pragma once
#include <Interpreters/Cache/QueryLimit.h>

namespace DB
{

class EvictionCandidates
{
public:
    ~EvictionCandidates();

    void add(LockedKey & locked_key, const FileSegmentMetadataPtr & candidate);

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
    FinalizeEvictionFunc finalize_eviction_func;
    std::vector<IFileCachePriority::IteratorPtr> queue_entries_to_invalidate;
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
