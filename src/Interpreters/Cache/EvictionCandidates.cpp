#include <Interpreters/Cache/EvictionCandidates.h>
#include <Interpreters/Cache/Metadata.h>


namespace ProfileEvents
{
    extern const Event FilesystemCacheEvictMicroseconds;
    extern const Event FilesystemCacheEvictedBytes;
    extern const Event FilesystemCacheEvictedFileSegments;
}

namespace DB
{

EvictionCandidates::~EvictionCandidates()
{
    for (const auto & [key, key_candidates] : candidates)
    {
        for (const auto & candidate : key_candidates.candidates)
            candidate->removal_candidate = false;
    }
}

void EvictionCandidates::add(LockedKey & locked_key, const FileSegmentMetadataPtr & candidate)
{
    auto [it, inserted] = candidates.emplace(locked_key.getKey(), KeyCandidates{});
    if (inserted)
        it->second.key_metadata = locked_key.getKeyMetadata();
    it->second.candidates.push_back(candidate);

    candidate->removal_candidate = true;
    ++candidates_size;
}

void EvictionCandidates::evict(FileCacheQueryLimit::QueryContext * query_context, const CachePriorityGuard::Lock & lock)
{
    if (candidates.empty())
        return;

    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::FilesystemCacheEvictMicroseconds);

    for (auto & [key, key_candidates] : candidates)
    {
        auto locked_key = key_candidates.key_metadata->tryLock();
        if (!locked_key)
            continue; /// key could become invalid after we released the key lock above, just skip it.

        auto & to_evict = key_candidates.candidates;
        while (!to_evict.empty())
        {
            auto & candidate = to_evict.back();
            chassert(candidate->releasable());

            const auto segment = candidate->file_segment;
            auto queue_it = segment->getQueueIterator();
            chassert(queue_it);

            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedFileSegments);
            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedBytes, segment->range().size());

            locked_key->removeFileSegment(segment->offset(), segment->lock());
            queue_it->remove(lock);

            if (query_context)
                query_context->remove(segment->key(), segment->offset(), lock);

            to_evict.pop_back();
        }
    }
}

}
