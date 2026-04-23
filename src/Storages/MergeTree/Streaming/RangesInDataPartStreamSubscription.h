#pragma once

#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/Streaming/QueueStreamSubscription.h>

namespace DB
{

/// Per-coordinator subscription. One subscription covers all partitions whose
/// `hash(pid) % query_subscriptions_count == current_subscription_index`.
class RangesInDataPartStreamSubscription : public QueueStreamSubscription<RangesInDataPart>
{
public:
    RangesInDataPartStreamSubscription(size_t query_subscriptions_count_, size_t current_subscription_index_);
    String dumpStructure() const;

    /// Per-partition cursor: highest `max_block` already pushed for the partition.
    std::map<String, Int64> max_block_numbers;
    const size_t query_subscriptions_count;
    const size_t current_subscription_index;
};

using RangesInDataPartStreamSubscriptionPtr = std::shared_ptr<RangesInDataPartStreamSubscription>;

}
