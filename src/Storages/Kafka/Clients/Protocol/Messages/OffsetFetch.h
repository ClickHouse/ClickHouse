#pragma once

/// OffsetFetch (api_key 9), supported range: v6-v8.
///
/// v8 is the batched-groups form; v6-v7 (first flexible versions, Kafka
/// 2.4-2.8) carry a single group at the top level — the codec maps them onto
/// groups[0] (require_stable exists from v7). v9 added MemberId/MemberEpoch
/// (KIP-848); v8 omits them, which is exactly right for group-less usage.

#include <Storages/Kafka/Clients/Protocol/ApiKey.h>
#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <optional>
#include <vector>

namespace Kafka::Protocol
{

struct OffsetFetchTopic
{
    String name;
    std::vector<Int32> partition_indexes;

    bool operator==(const OffsetFetchTopic &) const = default;
};

struct OffsetFetchGroup
{
    String group_id;
    /// nullopt means "all topics this group has committed offsets for".
    std::optional<std::vector<OffsetFetchTopic>> topics;

    bool operator==(const OffsetFetchGroup &) const = default;
};

struct OffsetFetchPartitionResponse
{
    Int32 partition_index = 0;
    Int64 committed_offset = -1;
    Int32 committed_leader_epoch = -1;
    std::optional<String> metadata;
    Int16 error_code = 0;

    bool operator==(const OffsetFetchPartitionResponse &) const = default;
};

struct OffsetFetchTopicResponse
{
    String name;
    std::vector<OffsetFetchPartitionResponse> partitions;

    bool operator==(const OffsetFetchTopicResponse &) const = default;
};

struct OffsetFetchGroupResponse
{
    String group_id;
    std::vector<OffsetFetchTopicResponse> topics;
    Int16 error_code = 0;

    bool operator==(const OffsetFetchGroupResponse &) const = default;
};

struct OffsetFetchResponse
{
    Int32 throttle_time_ms = 0;
    std::vector<OffsetFetchGroupResponse> groups;

    bool operator==(const OffsetFetchResponse &) const = default;

    static OffsetFetchResponse decode(Reader & r, Int16 version);
    void encode(Writer & w, Int16 version) const;
};

struct OffsetFetchRequest
{
    using Response = OffsetFetchResponse;
    static constexpr ApiKey api_key = ApiKey::OffsetFetch;
    static constexpr Int16 min_version = 6;
    static constexpr Int16 max_version = 8;
    static constexpr bool flexible(Int16) { return true; }
    static constexpr bool responseHeaderFlexible(Int16) { return true; }

    std::vector<OffsetFetchGroup> groups;
    bool require_stable = false;

    bool operator==(const OffsetFetchRequest &) const = default;

    void encode(Writer & w, Int16 version) const;
    static OffsetFetchRequest decode(Reader & r, Int16 version);
};

}
