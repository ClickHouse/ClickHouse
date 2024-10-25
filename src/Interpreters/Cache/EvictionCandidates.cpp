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
        auto locked_key = key_candidates.key_metadata->lock();
        for (const auto & candidate : key_candidates.candidates)
        {
            auto queue_iterator = candidate->getQueueIterator();
            queue_iterator->invalidate();

            chassert(candidate->releasable());
            candidate->file_segment->resetQueueIterator();
            /// We need to set removed flag in file segment metadata,
            /// because in dynamic cache resize we first remove queue entries,
            /// then evict which also removes file segment metadata,
            /// but we need to make sure that this file segment is not requested from cache in the meantime.
            /// In ordinary eviction we use `evicting` flag for this purpose,
            /// but here we cannot, because `evicting` is a property of a queue entry,
            /// but at this point for dynamic cache resize we have already deleted all queue entries.
            candidate->setRemovedFlag(*locked_key, lock);

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
            if (!candidate->releasable())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Eviction candidate is not releasable: {} (evicting or removed flag: {})",
                                candidate->file_segment->getInfoForLog(), candidate->isEvictingOrRemoved(*locked_key));
            }

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

bool EvictionCandidates::needFinalize() const
{
    /// finalize() does the following:
    /// 1. Release space holder in case if exists.
    ///    (Space holder is created if some space needs to be hold
    ///    while were are doing eviction from filesystem without which is done without a lock)
    ///    Note: this step is not needed in case of dynamic cache resize,
    ///          because space holders are not used.
    /// 2. Delete queue entries from IFileCachePriority queue.
    ///    These queue entries were invalidated during lock-free eviction phase,
    ///    so on finalize() we just remove them (not to let the queue grow too much).
    ///    Note: this step can in fact be removed as we do this cleanup
    ///    (removal of invalidated queue entries)
    ///    when we iterate the queue and see such entries along the way.
    ///    Note: this step is not needed in case of dynamic cache resize,
    ///          because we remove queue entries in advance, before actual eviction.
    /// 3. Execute on_finalize functions.
    ///    These functions are set only for SLRU eviction policy,
    ///    where we need to do additional work after eviction.
    ///    Note: this step is not needed in case of dynamic cache resize even for SLRU.

    return !on_finalize.empty() || !queue_entries_to_invalidate.empty();
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

void EvictionCandidates::setSpaceHolder(
    size_t size,
    size_t elements,
    IFileCachePriority & priority,
    const CachePriorityGuard::Lock & lock)
{
    if (hold_space)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Space hold is already set");
    hold_space = std::make_unique<IFileCachePriority::HoldSpace>(size, elements, priority, lock);
}

}
