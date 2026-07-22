#pragma once

#include <Storages/Streaming/IStreamSubscription.h>

#include <Common/WakeupFd.h>

#include <base/defines.h>
#include <base/types.h>

#include <map>
#include <mutex>

namespace DB
{

/// Per-coordinator subscription holding the per-partition table cursor.
class MergeTreeBoundsSubscription : public IStreamSubscription
{
public:
    MergeTreeBoundsSubscription(size_t query_subscriptions_count_, size_t current_subscription_index_, bool bounded_ = false);

    /// Promote the partition's `safe_block_number` to `new_cursor`.
    void advance(const String & partition_id, Int64 new_cursor);
    std::map<String, Int64> snapshot() const;

    bool isDisabled() const;
    void disable();

    /// Whether this subscription backs a bounded stream (read the first snapshot, then finish).
    bool isBounded() const { return bounded; }

    /// Mark the start of an enrichment round: the safe segment stops being determined while the round
    /// advances safe_block_numbers, and onEnrichmentRound republishes it once pending is known.
    void beginEnrichmentRound();

    /// Record an enrichment round; `pending` means a block is still in flight in some partition's
    /// gap, so the safe segment is not fully determined. A resolved round (`pending == false`) also
    /// wakes a bounded source so it can finish even when nothing was advanced (e.g. an empty table).
    void onEnrichmentRound(bool pending);

    /// Whether the safe segment is fully determined: an enrichment round has completed and left no
    /// partition blocked by an in-flight block in the gap. A bounded stream may finish once this holds.
    bool safeSegmentDetermined() const;

    /// Read end of the wakeup pipe;
    int fd() const { return wake.fd(); }
    void drain() { wake.drain(); }

    const size_t query_subscriptions_count;
    const size_t current_subscription_index;

private:
    const bool bounded;

    mutable std::mutex mutex;
    std::map<String, Int64> safe_block_numbers TSA_GUARDED_BY(mutex);
    bool is_disabled TSA_GUARDED_BY(mutex) = false;
    /// Set by the latest enrichment round: true once the safe segment is fully determined
    /// (a round completed with no partition still blocked by an in-flight block in the gap).
    bool safe_segment_determined TSA_GUARDED_BY(mutex) = false;

    WakeupFd wake;
};

using MergeTreeBoundsSubscriptionPtr = std::shared_ptr<MergeTreeBoundsSubscription>;

}
