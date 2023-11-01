#pragma once
#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/MergeTree/PartMetadataJSON.h>


namespace DB
{

/// Lets you know where to send requests to get to the replica.

struct ReplicatedMergeTreeAddress
{
    String host;
    UInt16 replication_port;
    UInt16 queries_port;
    String database;
    String table;
    String scheme;
    // If the version wasn't explicitly set, it's probably coming from an old version of CH
    PartMetadataFormatVersion desired_part_metadata_format_version = PART_METADATA_FORMAT_VERSION_OLD;

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
