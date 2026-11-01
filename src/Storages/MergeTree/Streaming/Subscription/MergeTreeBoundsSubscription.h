#pragma once

#include <Storages/Streaming/IStreamSubscription.h>

#include <Common/WakeupFd.h>

#include <base/defines.h>
#include <base/types.h>

#include <map>
#include <set>
#include <mutex>

namespace DB
{

/// Per-coordinator subscription holding the per-partition table cursor.
struct MergeTreeBoundsSubscription : public IStreamSubscription
{
    struct Snapshot
    {
        std::map<std::string, Int64> safe_block_numbers;
        bool was_updated = false;
    };

public:
    MergeTreeBoundsSubscription(size_t query_subscriptions_count_, size_t current_subscription_index_);

    bool update(std::map<std::string, int64_t> promoted_partitions, std::set<std::string> removed_partitions);
    Snapshot snapshot() const;

    /// Disabled subscription will not be updated anymore.
    void disable();
    bool isDisabled() const;

    /// Read end of the wakeup pipe;
    int fd() const { return wake.fd(); }
    void drain() { wake.drain(); }

    const size_t query_subscriptions_count;
    const size_t current_subscription_index;

private:
    mutable std::mutex mutex;

    /// Local parts information.
    std::map<std::string, Int64> safe_block_numbers TSA_GUARDED_BY(mutex);

    /// Runtime information.
    bool is_disabled TSA_GUARDED_BY(mutex) = false;
    bool was_updated TSA_GUARDED_BY(mutex) = false;

    /// Changes notification.
    WakeupFd wake;
};

using MergeTreeBoundsSubscriptionPtr = std::shared_ptr<MergeTreeBoundsSubscription>;

}
