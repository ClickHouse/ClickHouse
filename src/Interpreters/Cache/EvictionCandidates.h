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
    }
    ~EvictionCandidates();

    void add(LockedKey & locked_key, const FileSegmentMetadataPtr & candidate);

    void insert(EvictionCandidates && other, const CachePriorityGuard::Lock &);

    void evict(FileCacheQueryLimit::QueryContext * query_context, const CachePriorityGuard::Lock &);

    std::vector<std::string> evictFromMemory(FileCacheQueryLimit::QueryContext * query_context, const CachePriorityGuard::Lock &);

    size_t size() const { return candidates_size; }

    auto begin() const { return candidates.begin(); }

    auto end() const { return candidates.end(); }

private:
    struct KeyCandidates
    {
        KeyMetadataPtr key_metadata;
        std::vector<FileSegmentMetadataPtr> candidates;
    };

    std::unordered_map<FileCacheKey, KeyCandidates> candidates;
    size_t candidates_size = 0;

    std::vector<IFileCachePriority::IteratorPtr> invalidated_queue_entries;

    std::vector<std::string> evictImpl(
        bool remove_only_metadata,
        FileCacheQueryLimit::QueryContext * query_context,
        const CachePriorityGuard::Lock & lock);
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
