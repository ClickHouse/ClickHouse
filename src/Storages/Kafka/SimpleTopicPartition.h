#pragma once

#include <base/types.h>
#include <cppkafka/cppkafka.h>
#include <cppkafka/topic_partition.h>
#include <cppkafka/topic_partition_list.h>

namespace DB
{
    struct SimpleTopicPartition
    {
        String topic;
        int32_t partition_id;

        bool operator==(const SimpleTopicPartition &) const = default;
        bool operator<(const SimpleTopicPartition & other) const;
    };

    using SimpleTopicPartitions = std::vector<SimpleTopicPartition>;

    struct SimpleTopicPartitionHash
    {
        std::size_t operator()(const SimpleTopicPartition & tp) const;
    };

    struct SimpleTopicPartitionEquality
    {
        bool operator()(const SimpleTopicPartition & lhs, const SimpleTopicPartition & rhs) const
        {
            return lhs.topic == rhs.topic && lhs.partition_id == rhs.partition_id;
        }
    };

}
