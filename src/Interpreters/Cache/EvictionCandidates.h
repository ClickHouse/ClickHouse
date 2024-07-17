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

    void add(
        const FileSegmentMetadataPtr & candidate,
        LockedKey & locked_key,
        const CachePriorityGuard::Lock &);

    void evict();

    void removeQueueEntries(const CachePriorityGuard::Lock &);

    void onFinalize(FinalizeEvictionFunc && func) { on_finalize.emplace_back(std::move(func)); }

    void finalize(
        FileCacheQueryLimit::QueryContext * query_context,
        const CachePriorityGuard::Lock &);

    bool needFinalize() const;

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
    bool removed_queue_entries = false;

    IFileCachePriority::HoldSpacePtr hold_space;

    LoggerPtr log;
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
