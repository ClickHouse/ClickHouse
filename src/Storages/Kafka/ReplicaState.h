#pragma once

#include <base/types.h>
#include <Storages/Kafka/KafkaConsumer2.h>

#include <vector>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

struct ReplicaStateData 
{
    void writeText(WriteBuffer & out) const;
    void readText(ReadBuffer & in);
    String toString() const;

    size_t topic_partitions;
    KafkaConsumer2::TopicPartitions topics_assigned;
};


struct ReplicaState : public ReplicaStateData, std::enable_shared_from_this<ReplicaState>
{
    using Ptr = std::shared_ptr<ReplicaState>;

    static Ptr parse(const String & s);
};

using ReplicaStatePtr = std::shared_ptr<ReplicaState>;



}
