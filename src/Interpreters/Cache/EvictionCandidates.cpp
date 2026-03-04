#include <Interpreters/Cache/EvictionCandidates.h>
#include <Interpreters/Cache/Metadata.h>
#include <Common/logger_useful.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>


namespace ProfileEvents
{
    extern const Event FilesystemCacheEvictMicroseconds;
    extern const Event FilesystemCacheEvictedBytes;
    extern const Event FilesystemCacheEvictedFileSegments;
    extern const Event FilesystemCacheFailedEvictionCandidates;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char file_cache_dynamic_resize_fail_to_evict[];
}

void QueueEvictionInfo::releaseHoldSpace(const CacheStateGuard::Lock & lock)
{
    if (hold_space)
    {
        hold_space->release(lock);
        hold_space = {};
    }
}

std::string QueueEvictionInfo::toString() const
{
    WriteBufferFromOwnString wb;
    wb << "description: " << description;
    wb << ", " << "user: " << user_id;
    wb << ", " << "size to evict: " << size_to_evict;
    wb << ", " << "elements to evict: " << elements_to_evict;
    if (hold_space)
    {
        wb << ", " << "hold space size: " << hold_space->getSize();
        wb << ", " << "hold space elements: " << hold_space->getElements();
    }
    return wb.str();
}

void QueueEvictionInfo::merge(QueueEvictionInfoPtr other)
{
    size_to_evict += other->size_to_evict;
    elements_to_evict += other->elements_to_evict;
    if (other->hold_space)
    {
        if (hold_space)
            hold_space->merge(std::move(other->hold_space));
        else
            hold_space = std::move(other->hold_space);
    }
}

EvictionInfo::EvictionInfo(QueueID queue_id, QueueEvictionInfoPtr info)
{
    addImpl(queue_id, std::move(info), /* merge_if_exists */false);
}

std::string EvictionInfo::toString() const
{
    WriteBufferFromOwnString wb;
    for (auto it = begin(); it != end(); ++it)
    {
        if (it != begin())
            wb << ", ";
        wb << "[queue id " << it->first << ", " << it->second->toString() << "]";
    }
    return wb.str();
}

bool EvictionInfo::hasHoldSpace() const
{
    for (const auto & [_, elem] : *this)
        if (elem->hasHoldSpace())
            return true;
    return false;
}

void EvictionInfo::releaseHoldSpace(const CacheStateGuard::Lock & lock)
{
    for (auto & [_, elem] : *this)
        elem->releaseHoldSpace(lock);
}

void EvictionInfo::add(EvictionInfoPtr && info)
{
    for (auto && [queue_id, info_] : *info)
        addImpl(queue_id, std::move(info_), /* merge_if_exists */false);
}

void EvictionInfo::addOrUpdate(EvictionInfoPtr && info)
{
    for (auto && [queue_id, info_] : *info)
        addImpl(queue_id, std::move(info_), /* merge_if_exists */true);
}

void EvictionInfo::addImpl(
    const QueueID & queue_id,
    QueueEvictionInfoPtr info,
    bool merge_if_exists)
{
    size_to_evict += info->size_to_evict;
    elements_to_evict += info->elements_to_evict;
    auto [it, inserted] = emplace(queue_id, std::move(info));
    if (!inserted)
    {
        if (!merge_if_exists)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Queue with id {} already exists", queue_id);

        if (it->second)
            it->second->merge(std::move(info));
    }
}

const QueueEvictionInfo & EvictionInfo::get(const QueueID & queue_id) const
{
    if (auto it = find(queue_id); it != end())
    {
        return *it->second;
    }
    else
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Eviction info for queue  with id {} does not exist ({})",
            queue_id, toString());
    }
}

std::string EvictionCandidates::FailedCandidates::getFirstErrorMessage() const
{
    if (failed_candidates_per_key.empty())
        return "";

    const auto & first_failed = failed_candidates_per_key[0];
    if (!first_failed.error_messages.empty())
        return first_failed.error_messages[0];

    chassert(false);
    return "";
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
    LOG_TEST(log, "Will invalidate {} queue entries", queue_entries_to_invalidate.size());
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

void EvictionCandidates::add(const FileSegmentMetadataPtr & candidate, LockedKey & locked_key)
{
    auto [it, inserted] = candidates.emplace(locked_key.getKey(), KeyCandidates{});
    if (inserted)
        it->second.key_metadata = locked_key.getKeyMetadata();

    it->second.candidates.push_back(candidate);
    candidate->setEvictingFlag(locked_key);
    ++candidates_size;
    candidates_bytes += candidate->size();
}

void EvictionCandidates::removeQueueEntries(const CachePriorityGuard::WriteLock & lock)
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
            candidate->file_segment->markDelayedRemovalAndResetQueueIterator();

            /// We need to set removed flag in file segment metadata,
            /// because in dynamic cache resize we first remove queue entries,
            /// then evict which also removes file segment metadata,
            /// but we need to make sure that this file segment is not requested from cache in the meantime.
            /// In ordinary eviction we use `evicting` flag for this purpose,
            /// but here we cannot, because `evicting` is a property of a queue entry,
            /// but at this point for dynamic cache resize we have already deleted all queue entries.
            candidate->setRemovedFlag(*locked_key);

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
            /// key could become invalid (meaning all cache by this key was dropped)
            /// after we released the key lock above, just skip it.
            continue;
        }

        KeyCandidates failed_key_candidates;
        failed_key_candidates.key_metadata = key_candidates.key_metadata;

        while (!key_candidates.candidates.empty())
        {
            auto & candidate = key_candidates.candidates.back();
            try
            {
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

                fiu_do_on(FailPoints::file_cache_dynamic_resize_fail_to_evict, {
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Failed to evict file segment");
                });

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

                ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedFileSegments);
                ProfileEvents::increment(ProfileEvents::FilesystemCacheEvictedBytes, segment->range().size());

                if (iterator)
                    queue_entries_to_invalidate.push_back(iterator);
            }
            catch (...)
            {
                failed_candidates.total_cache_size += candidate->file_segment->getDownloadedSize();
                failed_candidates.total_cache_elements += 1;
                failed_key_candidates.candidates.push_back(candidate);

                const auto error_message = getCurrentExceptionMessage(true);
                failed_key_candidates.error_messages.push_back(error_message);

                ProfileEvents::increment(ProfileEvents::FilesystemCacheFailedEvictionCandidates);

                LOG_ERROR(log, "Failed to evict file segment ({}): {}",
                          candidate->file_segment->getInfoForLog(), error_message);
            }

            key_candidates.candidates.pop_back();
        }

        if (!failed_key_candidates.candidates.empty())
            failed_candidates.failed_candidates_per_key.push_back(failed_key_candidates);
    }
}

void EvictionCandidates::afterEvictWrite(const CachePriorityGuard::WriteLock & lock)
{
    if (after_evict_write_func)
    {
        after_evict_write_func(lock);
        after_evict_write_func = {};
    }
}

void EvictionCandidates::afterEvictState(const CacheStateGuard::Lock & lock)
{
    /// We invalidate queue entries under state lock,
    /// because this space will be replaced by reserver,
    /// so we need to make sure this is done atomically.
    ///
    /// Note: this step is not needed in case of dynamic cache resize,
    ///       because we remove queue entries in advance, before actual eviction.
    while (!queue_entries_to_invalidate.empty())
    {
        auto iterator = queue_entries_to_invalidate.back();
        iterator->invalidate();
        iterator->check(lock);
        queue_entries_to_invalidate.pop_back();
    }

    if (after_evict_state_func)
    {
        after_evict_state_func(lock);
        after_evict_state_func = {};
    }
}

}
