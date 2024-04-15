#include <Interpreters/Cache/EvictionCandidates.h>
#include <Interpreters/Cache/Metadata.h>
#include <Common/CurrentThread.h>


namespace ProfileEvents
{
    extern const Event FilesystemCacheEvictMicroseconds;
    extern const Event FilesystemCacheEvictedBytes;
    extern const Event FilesystemCacheEvictedFileSegments;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

EvictionCandidates::EvictionCandidates()
    : log(getLogger("EvictionCandidates"))
{
}

EvictionCandidates::~EvictionCandidates()
{
    /// Here `queue_entries_to_invalidate` contains queue entries
    /// for file segments which were successfully removed in evict().
    /// This set is non-empty in destructor only if there was
    /// an exception before we called finalize() or in the middle of finalize().
    for (const auto & iterator : queue_entries_to_invalidate)
    {
        /// In this case we need to finalize the state of queue entries
        /// which correspond to removed files segments to make sure
        /// consistent state of cache.
        iterator->invalidate();
    }

    /// We cannot reset evicting flag if we already removed queue entries.
    if (removed_queue_entries)
        return;

    /// Here `candidates` contain only those file segments
    /// which failed to be removed during evict()
    /// because there was some exception before evict()
    /// or in the middle of evict().
    for (const auto & [key, key_candidates] : candidates)
    {
        // Reset the evicting state
        // (as the corresponding file segments were not yet removed).
        for (const auto & candidate : key_candidates.candidates)
            candidate->resetEvictingFlag();
    }
}

void EvictionCandidates::add(
    const FileSegmentMetadataPtr & candidate,
    LockedKey & locked_key,
    const CachePriorityGuard::Lock & lock)
{
    auto [it, inserted] = candidates.emplace(locked_key.getKey(), KeyCandidates{});
    if (inserted)
        it->second.key_metadata = locked_key.getKeyMetadata();

    it->second.candidates.push_back(candidate);
    candidate->setEvictingFlag(locked_key, lock);
    ++candidates_size;
}

void EvictionCandidates::removeQueueEntries(const CachePriorityGuard::Lock & lock)
{
    /// Remove queue entries of eviction candidates.
    /// This will release space we consider to be hold for them.

    LOG_TEST(log, "Will remove {} eviction candidates", size());

    for (const auto & [key, key_candidates] : candidates)
    {
        for (const auto & candidate : key_candidates.candidates)
        {
            auto queue_iterator = candidate->getQueueIterator();
            queue_iterator->invalidate();

            candidate->file_segment->resetQueueIterator();
            queue_iterator->remove(lock);
        }
    }
    removed_queue_entries = true;
}

void EvictionCandidates::evict()
{
    if (candidates.empty())
        return;

    auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::FilesystemCacheEvictMicroseconds);

    /// If queue entries are already removed, then nothing to invalidate.
    if (!removed_queue_entries)
        queue_entries_to_invalidate.reserve(candidates_size);

    for (auto & [key, key_candidates] : candidates)
    {
        auto locked_key = key_candidates.key_metadata->tryLock();
        if (!locked_key)
        {
            /// key could become invalid after we released
            /// the key lock above, just skip it.
            continue;
        }

        while (!key_candidates.candidates.empty())
        {
            auto & candidate = key_candidates.candidates.back();
            chassert(candidate->releasable());
            const auto segment = candidate->file_segment;

            IFileCachePriority::IteratorPtr iterator;
            if (!removed_queue_entries)
            {
                iterator = segment->getQueueIterator();
                chassert(iterator);
            }

            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedFileSegments);
            ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedBytes, segment->range().size());

            locked_key->removeFileSegment(
                segment->offset(), segment->lock(),
                false/* can_be_broken */, false/* invalidate_queue_entry */);

            /// We set invalidate_queue_entry = false in removeFileSegment() above, because:
            ///   evict() is done without a cache priority lock while finalize() is done under the lock.
            ///   In evict() we:
            ///     - remove file segment from filesystem
            ///     - remove it from cache metadata
            ///   In finalize() we:
            ///     - remove corresponding queue entry from priority queue
            ///
            ///   We do not invalidate queue entry now in evict(),
            ///   because invalidation of queue entries needs to be done under cache lock.
            ///   Why? Firstly, as long as queue entry exists,
            ///   the corresponding space in cache is considered to be hold,
            ///   and once queue entry is removed/invalidated - the space is released.
            ///   Secondly, after evict() and finalize() stages we will also add back the
            ///   "reserved size" (<= actually released size),
            ///   but until we do this - we cannot allow other threads to think that
            ///   this released space is free to take, as it is not -
            ///   it was freed in favour of some reserver, so we can make it visibly
            ///   free only for that particular reserver.

            if (iterator)
                queue_entries_to_invalidate.push_back(iterator);

            key_candidates.candidates.pop_back();
        }
    }
}

void EvictionCandidates::finalize(
    FileCacheQueryLimit::QueryContext * query_context,
    const CachePriorityGuard::Lock & lock)
{
    chassert(lock.owns_lock());

    /// Release the hold space. It was hold only for the duration of evict() phase,
    /// now we can release. It might also be needed for on_finalize func,
    /// so release the space it firtst.
    if (hold_space)
        hold_space->release();

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

    for (auto & func : on_finalize)
        func(lock);

    /// Finalize functions might hold something (like HoldSpace object),
    /// so we need to clear them now.
    on_finalize.clear();
}

bool EvictionCandidates::needFinalize() const
{
    /// Do we need to call finalize()?
    return !on_finalize.empty() || !queue_entries_to_invalidate.empty();
}

void EvictionCandidates::setSpaceHolder(
    size_t size,
    size_t elements,
    IFileCachePriority & priority,
    const CachePriorityGuard::Lock & lock)
{
    if (hold_space)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Space hold is already set");
    else
        hold_space = std::make_unique<IFileCachePriority::HoldSpace>(size, elements, priority, lock);
}

}
