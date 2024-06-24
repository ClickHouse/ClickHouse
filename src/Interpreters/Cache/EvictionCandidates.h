#pragma once
#include <Interpreters/Cache/QueryLimit.h>

namespace DB
{

class EvictionCandidates
{
public:
    ~EvictionCandidates();

    void add(LockedKey & locked_key, const FileSegmentMetadataPtr & candidate);

    void evict(FileCacheQueryLimit::QueryContext * query_context, const CachePriorityGuard::Lock &);

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
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
