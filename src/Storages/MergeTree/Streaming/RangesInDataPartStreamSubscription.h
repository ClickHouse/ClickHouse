#pragma once

#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/Streaming/QueueStreamSubscription.h>

namespace DB
{

class RangesInDataPartStreamSubscription : public QueueStreamSubscription<RangesInDataPart>
{
public:
    String dumpStructure() const;

    std::map<String, Int64> max_block_numbers;
    size_t query_subscriptions_count;
    size_t current_subscription_index;
};

}
