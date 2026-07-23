#pragma once

#include <Storages/Streaming/IStreamSubscription.h>

#include <Common/WakeupFd.h>

#include <base/defines.h>
#include <base/types.h>

#include <map>
#include <mutex>
#include <optional>

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

    /// Mark the start of a round: clears "determined" while the map is advanced (onEnrichmentRound republishes it).
    void beginEnrichmentRound();

    /// Record a round; `pending` = a block still in flight (not determined), and a resolved round wakes a bounded source.
    void onEnrichmentRound(bool pending);

    /// Whether the safe segment is fully determined (a round completed with no partition still blocked).
    bool safeSegmentDetermined() const;

    /// Atomically returns the safe-block-number map iff the segment is determined, else nullopt.
    std::optional<std::map<String, Int64>> snapshotIfDetermined() const;

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
    /// True once the latest round fully determined the safe segment (no partition still blocked).
    bool safe_segment_determined TSA_GUARDED_BY(mutex) = false;

    WakeupFd wake;
};

using MergeTreeBoundsSubscriptionPtr = std::shared_ptr<MergeTreeBoundsSubscription>;

}
