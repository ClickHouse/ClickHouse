#pragma once

/// ListOffsets (api_key 2), supported range: v5-v7.
///
/// v6 is the first flexible version; v5 is the non-flexible form Kafka 2.4
/// speaks (same fields, classic encodings). v7 only added the MAX_TIMESTAMP
/// semantic for timestamp = -3 (KIP-734).

#include <Storages/Kafka/Clients/Protocol/ApiKey.h>
#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <vector>

namespace Kafka::Protocol
{

/// Sentinel timestamps defined by the protocol.
inline constexpr Int64 list_offsets_earliest = -2;
inline constexpr Int64 list_offsets_latest = -1;

struct ListOffsetsPartition
{
    Int32 partition_index = 0;
    Int32 current_leader_epoch = -1;
    Int64 timestamp = 0;

    bool operator==(const ListOffsetsPartition &) const = default;
};

struct ListOffsetsTopic
{
    String name;
    std::vector<ListOffsetsPartition> partitions;

    bool operator==(const ListOffsetsTopic &) const = default;
};

struct ListOffsetsPartitionResponse
{
    Int32 partition_index = 0;
    Int16 error_code = 0;
    Int64 timestamp = -1;
    Int64 offset = -1;
    Int32 leader_epoch = -1;

    bool operator==(const ListOffsetsPartitionResponse &) const = default;
};

struct ListOffsetsTopicResponse
{
    String name;
    std::vector<ListOffsetsPartitionResponse> partitions;

    bool operator==(const ListOffsetsTopicResponse &) const = default;
};

struct ListOffsetsResponse
{
    Int32 throttle_time_ms = 0;
    std::vector<ListOffsetsTopicResponse> topics;

    bool operator==(const ListOffsetsResponse &) const = default;

    static ListOffsetsResponse decode(Reader & r, Int16 version);
    void encode(Writer & w, Int16 version) const;
};

struct ListOffsetsRequest
{
    using Response = ListOffsetsResponse;
    static constexpr ApiKey api_key = ApiKey::ListOffsets;
    static constexpr Int16 min_version = 5;
    static constexpr Int16 max_version = 7;
    static constexpr bool flexible(Int16 version) { return version >= 6; }
    static constexpr bool responseHeaderFlexible(Int16 version) { return version >= 6; }

    Int32 replica_id = -1;
    Int8 isolation_level = 0;
    std::vector<ListOffsetsTopic> topics;

    bool operator==(const ListOffsetsRequest &) const = default;

    void encode(Writer & w, Int16 version) const;
    static ListOffsetsRequest decode(Reader & r, Int16 version);
};

}
