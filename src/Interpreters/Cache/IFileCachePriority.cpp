#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/Metadata.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSizeLimit;
}

namespace ProfileEvents
{
    extern const Event FilesystemCacheEvictMicroseconds;
    extern const Event FilesystemCacheEvictedBytes;
    extern const Event FilesystemCacheEvictedFileSegments;
}

namespace DB
{

IFileCachePriority::IFileCachePriority(size_t max_size_, size_t max_elements_)
    : max_size(max_size_), max_elements(max_elements_)
{
    CurrentMetrics::set(CurrentMetrics::FilesystemCacheSizeLimit, max_size_);
}

IFileCachePriority::Entry::Entry(
    const Key & key_,
    size_t offset_,
    size_t size_,
    KeyMetadataPtr key_metadata_)
    : key(key_)
    , offset(offset_)
    , key_metadata(key_metadata_)
    , size(size_)
{
}

IFileCachePriority::Entry::Entry(const Entry & other)
    : key(other.key)
    , offset(other.offset)
    , key_metadata(other.key_metadata)
    , size(other.size.load())
    , hits(other.hits)
{
}

IFileCachePriority::EvictionCandidates::~EvictionCandidates()
{
    /// If failed to reserve space, we don't delete the candidates but drop the flag instead
    /// so the segments can be used again
    for (const auto & [key, key_candidates] : candidates)
    {
        for (const auto & candidate : key_candidates.candidates)
            candidate->removal_candidate = false;
    }
}

void IFileCachePriority::EvictionCandidates::add(const KeyMetadataPtr & key, const FileSegmentMetadataPtr & candidate)
{
    auto it = candidates.emplace(key->key, KeyCandidates{}).first;
    it->second.candidates.push_back(candidate);
    candidate->removal_candidate = true;
}

void IFileCachePriority::EvictionCandidates::evict(const CacheGuard::Lock & lock)
{
    if (candidates.empty())
        return;

    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::FilesystemCacheEvictMicroseconds);

    for (auto & [key, key_candidates] : candidates)
    {
        auto locked_key = key_candidates.key_metadata->tryLock();
        if (!locked_key)
            continue; /// key could become invalid after we released the key lock above, just skip it.

        /// delete from vector in reverse order just for efficiency
        auto & to_evict = key_candidates.candidates;
        while (!to_evict.empty())
        {
            auto & candidate = to_evict.back();
            chassert(candidate->releasable());

            const auto * segment = candidate->file_segment.get();
            auto queue_it = segment->getQueueIterator();
            chassert(queue_it);

            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedFileSegments);
            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedBytes, segment->range().size());

            locked_key->removeFileSegment(segment->offset(), segment->lock());
            queue_it->remove(lock);

            // if (query_context)
            //     query_context->remove(current_key, segment->offset(), cache_lock);

            to_evict.pop_back();
        }
    }
}

}
