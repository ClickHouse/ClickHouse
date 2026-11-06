#pragma once

/// Metadata (api_key 3), supported range: v9-v12 (flexible since v9).
///
/// Topic UUIDs exist from v10 (Fetch v13 needs them; older brokers fetch by
/// name). v9-v10 carry include/cluster_authorized_operations extras; the
/// request topic name is non-nullable below v10 and the response topic name
/// non-nullable below v12.

#include <Storages/Kafka/Clients/Protocol/ApiKey.h>
#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <optional>
#include <vector>

namespace Kafka::Protocol
{

struct MetadataRequestTopic
{
    Uuid topic_id; /// zero UUID = look up by name
    std::optional<String> name;

    bool operator==(const MetadataRequestTopic &) const = default;
};

struct MetadataBroker
{
    Int32 node_id = 0;
    String host;
    Int32 port = 0;
    std::optional<String> rack;

    bool operator==(const MetadataBroker &) const = default;
};

struct MetadataPartition
{
    Int16 error_code = 0;
    Int32 partition_index = 0;
    Int32 leader_id = 0;
    Int32 leader_epoch = 0;
    std::vector<Int32> replica_nodes;
    std::vector<Int32> isr_nodes;
    std::vector<Int32> offline_replicas;

    bool operator==(const MetadataPartition &) const = default;
};

struct MetadataTopic
{
    Int16 error_code = 0;
    std::optional<String> name;
    Uuid topic_id;
    bool is_internal = false;
    std::vector<MetadataPartition> partitions;
    Int32 topic_authorized_operations = 0;

    bool operator==(const MetadataTopic &) const = default;
};

struct MetadataResponse
{
    Int32 throttle_time_ms = 0;
    std::vector<MetadataBroker> brokers;
    std::optional<String> cluster_id;
    Int32 controller_id = 0;
    std::vector<MetadataTopic> topics;

    bool operator==(const MetadataResponse &) const = default;

    static MetadataResponse decode(Reader & r, Int16 version);
    void encode(Writer & w, Int16 version) const;
};

struct MetadataRequest
{
    using Response = MetadataResponse;
    static constexpr ApiKey api_key = ApiKey::Metadata;
    static constexpr Int16 min_version = 9;
    static constexpr Int16 max_version = 12;
    static constexpr bool flexible(Int16) { return true; }
    static constexpr bool responseHeaderFlexible(Int16) { return true; }

    /// nullopt requests metadata for every topic the client can access.
    std::optional<std::vector<MetadataRequestTopic>> topics;
    bool allow_auto_topic_creation = false;
    bool include_topic_authorized_operations = false;

    bool operator==(const MetadataRequest &) const = default;

    void encode(Writer & w, Int16 version) const;
    static MetadataRequest decode(Reader & r, Int16 version);
};

}
