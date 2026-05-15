#pragma once

#include <Storages/Streaming/IStreamSubscription.h>

#include <base/defines.h>
#include <base/types.h>

#if defined(OS_LINUX)
#    include <Common/EventFD.h>
#else
#    include <condition_variable>
#endif

#include <map>
#include <mutex>
#include <optional>

namespace DB
{

/// Per-coordinator subscription holding the per-partition table cursor
class MergeTreeBoundsSubscription : public IStreamSubscription
{
public:
    MergeTreeBoundsSubscription(size_t query_subscriptions_count_, size_t current_subscription_index_);

    /// Promote the partition's `safe_block_number` to `new_cursor`.
    void advance(const String & partition_id, Int64 new_cursor);
    std::map<String, Int64> snapshot() const;

    bool isDisabled() const;
    void disable();

    /// Linux: eventfd handle. Other platforms: nullptr
    EventFD * fd();
    void wait();

    const size_t query_subscriptions_count;
    const size_t current_subscription_index;

private:
    mutable std::mutex mutex;
    std::map<String, Int64> safe_block_numbers TSA_GUARDED_BY(mutex);
    bool is_disabled TSA_GUARDED_BY(mutex) = false;

#if defined(OS_LINUX)
    EventFD wake{/*non_blocking=*/true};
#else
    mutable std::condition_variable wake;
#endif
};

using MergeTreeBoundsSubscriptionPtr = std::shared_ptr<MergeTreeBoundsSubscription>;

}
