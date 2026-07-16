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

    /// Record that an enrichment round has completed for this subscription. For bounded
    /// subscriptions this also wakes the pipeline so it can finish even when the round
    /// advanced nothing (e.g. an empty table).
    void onEnrichmentRound();

    /// Whether at least one enrichment round has completed for this subscription.
    bool enrichmentDone() const;

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
    bool enrichment_done TSA_GUARDED_BY(mutex) = false;

    WakeupFd wake;
};

using MergeTreeBoundsSubscriptionPtr = std::shared_ptr<MergeTreeBoundsSubscription>;

}
