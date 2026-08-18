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
    MergeTreeBoundsSubscription(size_t query_subscriptions_count_, size_t current_subscription_index_);

    /// Promote the partition's `safe_block_number` to `new_cursor`.
    void advance(const String & partition_id, Int64 new_cursor);
    std::map<String, Int64> snapshot() const;

    bool isDisabled() const;
    void disable();

    /// Record that one enrichment round finished (an empty round counts too) and wake readers.
    void onEnrichmentRound();

    /// How many enrichment rounds have been applied to this subscription (a bounded source compares this with 0).
    size_t updatesCount() const;

    /// Read end of the wakeup pipe;
    int fd() const { return wake.fd(); }
    void drain() { wake.drain(); }

    const size_t query_subscriptions_count;
    const size_t current_subscription_index;

private:
    mutable std::mutex mutex;
    std::map<String, Int64> safe_block_numbers TSA_GUARDED_BY(mutex);
    bool is_disabled TSA_GUARDED_BY(mutex) = false;
    size_t updates_count TSA_GUARDED_BY(mutex) = 0;

    WakeupFd wake;
};

using MergeTreeBoundsSubscriptionPtr = std::shared_ptr<MergeTreeBoundsSubscription>;

}
