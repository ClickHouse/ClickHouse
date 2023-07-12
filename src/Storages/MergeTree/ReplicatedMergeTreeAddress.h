#pragma once
#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/// Lets you know where to send requests to get to the replica.

struct ReplicatedMergeTreeAddress
{
    String host;
    UInt16 replication_port = 0;
    /// TODO(cluster): port for secure connections
    UInt16 queries_port = 0;
    String database;
    String table;
    String scheme;

    ReplicatedMergeTreeAddress() = default;
    explicit ReplicatedMergeTreeAddress(const String & str)
    {
        fromString(str);
    }

    void writeText(WriteBuffer & out) const;

    void readText(ReadBuffer & in);

    String toString() const;

    void fromString(const String & str);
};

}
