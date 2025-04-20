#include <Storages/Kafka/ReplicaState.h>
#include <IO/Operators.h>
#include "IO/ReadBufferFromString.h"
#include <IO/WriteBufferFromString.h>

namespace DB
{
    void ReplicaStateData::writeText(WriteBuffer & out) const
    {
        out << "permanent topic partitions: " << permanent_topic_partitions << "\n";
        for (const auto & partition : permanent_topics_assigned)
        {
            out << "topic: " << escape << partition.topic << "\n";
            out << "partition: " << escape << partition.partition_id << "\n";
        }

        out << "temporary topic partitions: " << tmp_topic_partitions << "\n";
        for (const auto & partition : tmp_topics_assigned)
        {
            out << "topic: " << escape << partition.topic << "\n";
            out << "partition: " << escape << partition.partition_id << "\n";
        }

    }

    void ReplicaStateData::readText(ReadBuffer & in)
    {
        in >> "permanent topic partitions: " >> permanent_topic_partitions >> "\n";
        permanent_topics_assigned.resize(permanent_topic_partitions);
        for (size_t i = 0; i < permanent_topic_partitions; ++i)
        {
            in >> "topic: " >> escape >> permanent_topics_assigned[i].topic >> "\n";
            in >> "partition: " >> escape >> permanent_topics_assigned[i].partition_id >> "\n";
        }

        in >> "temporary topic partitions: " >> tmp_topic_partitions >> "\n";
        tmp_topics_assigned.resize(tmp_topic_partitions);
        for (size_t i = 0; i < tmp_topic_partitions; ++i)
        {
            in >> "topic: " >> escape >> tmp_topics_assigned[i].topic >> "\n";
            in >> "partition: " >> escape >> tmp_topics_assigned[i].partition_id >> "\n";
        }
    }

    String ReplicaStateData::toString() const
    {
        WriteBufferFromOwnString out;
        writeText(out);
        return out.str();
    }

    ReplicaState::Ptr ReplicaState::parse(const String & s)
    {
        ReadBufferFromString in(s);
        Ptr res = std::make_shared<ReplicaState>();
        res->readText(in);
        assertEOF(in);
        return res;
    }
}
