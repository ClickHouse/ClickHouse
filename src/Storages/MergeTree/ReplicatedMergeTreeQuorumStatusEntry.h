#pragma once

#include <set>
#include <common/types.h>
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
struct ReplicatedMergeTreeQuorumStatusEntry
{
    size_t required_number_of_replicas{};
    std::set<String> replicas;

    ReplicatedMergeTreeQuorumStatusEntry() {}
    ReplicatedMergeTreeQuorumStatusEntry(const String & str)
    {
        fromString(str);
    }

    void writeText(WriteBuffer & out) const
    {
        out << "required_number_of_replicas: " << required_number_of_replicas << "\n"
            << "actual_number_of_replicas: " << replicas.size() << "\n"
            << "replicas:\n";

        for (const auto & replica : replicas)
            out << escape << replica << "\n";
    }

    void readText(ReadBuffer & in)
    {
        size_t actual_number_of_replicas = 0;

        in >> "required_number_of_replicas: " >> required_number_of_replicas >> "\n"
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

	bool isQuorumReached()
	{
		return required_number_of_replicas && required_number_of_replicas <= replicas.size();
	}
};

}
