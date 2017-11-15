#pragma once

#include <set>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/ZooKeeper/ZooKeeper.h>


namespace DB
{

/** To implement the functionality of the "quorum write".
  * Information about which replicas the inserted part of data appeared on,
  *  and on how many replicas it should be.
  */
struct ReplicatedMergeTreeQuorumEntry
{
    String part_name;
    size_t required_number_of_replicas;
    std::set<String> replicas;

    ReplicatedMergeTreeQuorumEntry() {}
    ReplicatedMergeTreeQuorumEntry(const String & str)
    {
        fromString(str);
    }

    void writeText(WriteBuffer & out) const
    {
        out << "version: 1\n"
            << "part_name: " << part_name << "\n"
            << "required_number_of_replicas: " << required_number_of_replicas << "\n"
            << "actual_number_of_replicas: " << replicas.size() << "\n"
            << "replicas:\n";

        for (const auto & replica : replicas)
            out << escape << replica << "\n";
    }

    void readText(ReadBuffer & in)
    {
        size_t actual_number_of_replicas = 0;

        in >> "version: 1\n"
            >> "part_name: " >> part_name >> "\n"
            >> "required_number_of_replicas: " >> required_number_of_replicas >> "\n"
            >> "actual_number_of_replicas: " >> actual_number_of_replicas >> "\n"
            >> "replicas:\n";

        for (size_t i = 0; i < actual_number_of_replicas; ++i)
        {
            String replica;
            in >> escape >> replica >> "\n";
            replicas.insert(replica);
        }
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
