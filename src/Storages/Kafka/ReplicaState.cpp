#include <Storages/Kafka/ReplicaState.h>
#include <IO/Operators.h>
#include "IO/ReadBufferFromString.h"
#include <IO/WriteBufferFromString.h>

namespace DB
{
    void ReplicaStateData::writeText(WriteBuffer & out) const
    {
        out << "topic partitions: " << topic_partitions << "\n";
        for (const auto& partition : topics_assigned) {
            out << "topic: " << escape << partition.topic << "\n";
            out << "partition: " << escape << partition.partition_id << "\n";
        }
    }

    void ReplicaStateData::readText(ReadBuffer & in)
    {
        in >> "topic partitions: " >> topic_partitions >> "\n";
        topics_assigned.resize(topic_partitions);
        for (size_t i = 0; i < topic_partitions; ++i) {
            in >> "topic: " >> escape >> topics_assigned[i].topic >> "\n";
            in >> "partition: " >> escape >> topics_assigned[i].partition_id >> "\n";
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
