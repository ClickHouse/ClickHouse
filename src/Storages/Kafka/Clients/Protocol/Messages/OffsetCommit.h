#pragma once

/// OffsetCommit (api_key 8), supported range: v8-v9.
///
/// v8 is the first flexible version. v9 only renamed generation_id to
/// generation_id_or_member_epoch (KIP-848) — the wire layout is identical,
/// which is why the range spans both. Group-less commits (ClickHouse-managed
/// assignment) send generation -1 and an empty member id.

#include <Storages/Kafka/Clients/Protocol/ApiKey.h>
#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <optional>
#include <vector>

namespace Kafka::Protocol
{

struct OffsetCommitPartition
{
    Int32 partition_index = 0;
    Int64 committed_offset = 0;
    Int32 committed_leader_epoch = -1;
    std::optional<String> committed_metadata;

    bool operator==(const OffsetCommitPartition &) const = default;
};

struct OffsetCommitTopic
{
    String name;
    std::vector<OffsetCommitPartition> partitions;

    bool operator==(const OffsetCommitTopic &) const = default;
};

struct OffsetCommitPartitionResponse
{
    Int32 partition_index = 0;
    Int16 error_code = 0;

    bool operator==(const OffsetCommitPartitionResponse &) const = default;
};

struct OffsetCommitTopicResponse
{
    String name;
    std::vector<OffsetCommitPartitionResponse> partitions;

    bool operator==(const OffsetCommitTopicResponse &) const = default;
};

struct OffsetCommitResponse
{
    Int32 throttle_time_ms = 0;
    std::vector<OffsetCommitTopicResponse> topics;

    bool operator==(const OffsetCommitResponse &) const = default;

    static OffsetCommitResponse decode(Reader & r, Int16 version);
    void encode(Writer & w, Int16 version) const;
};

struct OffsetCommitRequest
{
    using Response = OffsetCommitResponse;
    static constexpr ApiKey api_key = ApiKey::OffsetCommit;
    static constexpr Int16 min_version = 8;
    static constexpr Int16 max_version = 9;
    static constexpr bool flexible(Int16) { return true; }
    static constexpr bool responseHeaderFlexible(Int16) { return true; }

    String group_id;
    /// -1 for group-less commits (no membership).
    Int32 generation_id_or_member_epoch = -1;
    String member_id;
    std::optional<String> group_instance_id;
    std::vector<OffsetCommitTopic> topics;

    bool operator==(const OffsetCommitRequest &) const = default;

    void encode(Writer & w, Int16 version) const;
    static OffsetCommitRequest decode(Reader & r, Int16 version);
};

}
