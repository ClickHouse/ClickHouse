#include <vector>

#include <fmt/core.h>

#include <boost/algorithm/string/join.hpp>

#include <Storages/MergeTree/Streaming/RangesInDataPartStreamSubscription.h>

namespace DB
{

String RangesInDataPartStreamSubscription::dumpStructure() const
{
    std::vector<String> block_numbers_dump;
    block_numbers_dump.reserve(max_block_numbers.size());

    for (const auto & [partition_id, block_number] : max_block_numbers)
        block_numbers_dump.push_back(fmt::format("{{{} : {}}}", partition_id, block_number));

    return fmt::format("{}/{} | {{{}}}", current_subscription_index, query_subscriptions_count, boost::join(block_numbers_dump, ", "));
}

}
