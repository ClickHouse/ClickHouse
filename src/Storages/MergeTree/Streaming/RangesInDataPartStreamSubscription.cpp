#include <Storages/MergeTree/Streaming/RangesInDataPartStreamSubscription.h>

#include <boost/algorithm/string/join.hpp>

namespace DB
{

RangesInDataPartStreamSubscription::RangesInDataPartStreamSubscription(
    size_t query_subscriptions_count_, size_t current_subscription_index_)
    : query_subscriptions_count(query_subscriptions_count_)
    , current_subscription_index(current_subscription_index_)
{
}

String RangesInDataPartStreamSubscription::dumpStructure() const
{
    std::vector<String> block_numbers_dump;
    block_numbers_dump.reserve(max_block_numbers.size());

    for (const auto & [partition_id, block_number] : max_block_numbers)
        block_numbers_dump.push_back(fmt::format("{{{} : {}}}", partition_id, block_number));

    return fmt::format("{}/{} | {{{}}}", current_subscription_index, query_subscriptions_count, boost::join(block_numbers_dump, ", "));
}

}
