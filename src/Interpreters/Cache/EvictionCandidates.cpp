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
    for (const auto & iterator : queue_entries_to_invalidate)
    {
        /// If there was an exception between evict and finalize phase
        /// for some eviction candidate, we need to reset its entry now.
        iterator->invalidate();
    }

    for (const auto & [key, key_candidates] : candidates)
    {
        for (const auto & candidate : key_candidates.candidates)
            candidate->setEvicting(false, nullptr, nullptr);
    }
}

void EvictionCandidates::add(const FileSegmentMetadataPtr & candidate, LockedKey & locked_key, const CachePriorityGuard::Lock & lock)
{
    auto [it, inserted] = candidates.emplace(locked_key.getKey(), KeyCandidates{});
    if (inserted)
        it->second.key_metadata = locked_key.getKeyMetadata();
    it->second.candidates.push_back(candidate);

    candidate->setEvicting(true, &locked_key, &lock);
    ++candidates_size;
}

void EvictionCandidates::evict(const CachePriorityGuard::Lock &)
{
    if (candidates.empty())
        return;

    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::FilesystemCacheEvictMicroseconds);
    queue_entries_to_invalidate.reserve(candidates_size);

    for (auto & [key, key_candidates] : candidates)
    {
        auto locked_key = key_candidates.key_metadata->tryLock();
        if (!locked_key)
            continue; /// key could become invalid after we released the key lock above, just skip it.

        while (!key_candidates.candidates.empty())
        {
            auto & candidate = key_candidates.candidates.back();
            chassert(candidate->releasable());

            const auto segment = candidate->file_segment;
            auto iterator = segment->getQueueIterator();
            chassert(iterator);

            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedFileSegments);
            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedBytes, segment->range().size());

            /// We remove file segment, but do not invalidate queue entry now,
            /// we will invalidate it after all eviction candidates are removed (in finalize() method),
            /// because invalidation of queue entries needs to be done under cache lock.
            /// Why?
            /// Firstly, as long as queue entry exists, the corresponding space in cache is considered to be hold,
            /// and once it is invalidated - the space is released.
            /// Secondly, after evict() and finalize() stages we will also add back "reserved size"
            /// (<= actually released size), but until we do this - we cannot allow other threads to think that
            /// this released space is free, as it is not - it is removed in favour of some reserver
            /// so we can make it visibly free only for that particular reserver.
            locked_key->removeFileSegment(
                segment->offset(), segment->lock(), /* can_be_broken */false, /* invalidate_queue_entry */false);

            queue_entries_to_invalidate.push_back(iterator);
            key_candidates.candidates.pop_back();
        }
    }
}

void EvictionCandidates::finalize(FileCacheQueryLimit::QueryContext * query_context, const CachePriorityGuard::Lock & lock)
{
    while (!queue_entries_to_invalidate.empty())
    {
        auto iterator = queue_entries_to_invalidate.back();
        iterator->invalidate();
        queue_entries_to_invalidate.pop_back();

        /// Remove entry from per query priority queue.
        if (query_context)
        {
            const auto & entry = iterator->getEntry();
            query_context->remove(entry->key, entry->offset, lock);
        }

        /// Remove entry from main priority queue.
        iterator->remove(lock);
    }

    if (finalize_eviction_func)
        finalize_eviction_func(lock);
}

}
