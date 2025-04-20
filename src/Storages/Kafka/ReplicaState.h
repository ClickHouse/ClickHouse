#pragma once

#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Storages/Kafka/KafkaConsumer2.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

struct ReplicaState
{
    KafkaConsumer2::TopicPartitions permanent_topics_assigned;
    KafkaConsumer2::TopicPartitions tmp_topics_assigned;

    ReplicaState() = default;
    explicit ReplicaState(const String & str)
    {
        fromString(str);
    }

    void writeText(WriteBuffer & out) const
    {
        out << "permanent topic partitions: " << permanent_topics_assigned.size() << "\n";
        for (const auto & partition : permanent_topics_assigned)
            out << partition.topic << "\t" << partition.partition_id << "\n";

        out << "temporary topic partitions: " << tmp_topics_assigned.size() << "\n";
        for (const auto & partition : tmp_topics_assigned)
            out << partition.topic << "\t" << partition.partition_id << "\n";
    }

    void readText(ReadBuffer & in)
    {
        size_t permanent_topic_partitions;
        in >> "permanent topic partitions: " >> permanent_topic_partitions >> "\n";
        permanent_topics_assigned.resize(permanent_topic_partitions);
        for (size_t i = 0; i < permanent_topic_partitions; ++i)
            in >> permanent_topics_assigned[i].topic >> "\t" >> permanent_topics_assigned[i].partition_id >> "\n";

        size_t tmp_topic_partitions;
        in >> "temporary topic partitions: " >> tmp_topic_partitions >> "\n";
        tmp_topics_assigned.resize(tmp_topic_partitions);
        for (size_t i = 0; i < tmp_topic_partitions; ++i)
            in >> tmp_topics_assigned[i].topic >> "\t" >> tmp_topics_assigned[i].partition_id >> "\n";
    }

    String toString() const
    {
        WriteBufferFromOwnString out;
        writeText(out);
        return out.str();
    }

    void fromString(const String & str)
    {
        ReadBufferFromString in(str);
        readText(in);
    }
};

}
