#pragma once

#include <set>
#include <common/types.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumStatusEntry.h>


namespace DB
{

/** To implement the functionality of the "quorum write".
  * Information about which replicas the inserted part of data appeared on,
  *  and on how many replicas it should be.
  */
struct ReplicatedMergeTreeBlockEntry
{
	String part_name;
	std::optional<ReplicatedMergeTreeQuorumStatusEntry> quorum_status;

    ReplicatedMergeTreeBlockEntry() {}
    ReplicatedMergeTreeBlockEntry(const String & str)
    {
        fromString(str);
    }

    void writeText(WriteBuffer & out) const
    {
        out << part_name << "\n";

		if (quorum_status)
			quorum_status->writeText(out);
    }

    void readText(ReadBuffer & in)
    {
		in >> part_name;

		if (!in.eof())
		{
			in >> "\n";
			quorum_status = ReplicatedMergeTreeQuorumStatusEntry();
			quorum_status->readText(in);
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
