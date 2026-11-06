#pragma once

/// Produce (api_key 0), supported range: v7-v9.
///
/// v9 is the first flexible version (Kafka 4.0 baseline). v7-v8 are the
/// non-flexible forms that redpanda 26.x (max v7) and Kafka 2.4 (max v8)
/// speak; the response gains record_errors/error_message at v8.

#include <Storages/Kafka/Clients/Protocol/ApiKey.h>
#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <optional>
#include <string_view>
#include <vector>

namespace Kafka::Protocol
{

struct ProducePartitionData
{
    Int32 index = 0;
    /// Encoded record-batch blob — zero-copy view; the caller keeps the blob
    /// alive for the duration of the call. nullopt = null bytes.
    std::optional<std::string_view> records;

    bool operator==(const ProducePartitionData &) const = default;
};

struct ProduceTopicData
{
    String name;
    std::vector<ProducePartitionData> partition_data;

    bool operator==(const ProduceTopicData &) const = default;
};

struct RecordError
{
    Int32 batch_index = 0;
    std::optional<String> batch_index_error_message;

    bool operator==(const RecordError &) const = default;
};

struct ProducePartitionResponse
{
    Int32 index = 0;
    Int16 error_code = 0;
    Int64 base_offset = -1;
    Int64 log_append_time_ms = -1;
    Int64 log_start_offset = -1;
    std::vector<RecordError> record_errors;
    std::optional<String> error_message;

    bool operator==(const ProducePartitionResponse &) const = default;
};

struct ProduceTopicResponse
{
    String name;
    std::vector<ProducePartitionResponse> partition_responses;

    bool operator==(const ProduceTopicResponse &) const = default;
};

struct ProduceResponse
{
    std::vector<ProduceTopicResponse> responses;
    Int32 throttle_time_ms = 0;

    bool operator==(const ProduceResponse &) const = default;

    static ProduceResponse decode(Reader & r, Int16 version);
    void encode(Writer & w, Int16 version) const;
};

struct ProduceRequest
{
    using Response = ProduceResponse;
    static constexpr ApiKey api_key = ApiKey::Produce;
    static constexpr Int16 min_version = 7;
    static constexpr Int16 max_version = 9;
    static constexpr bool flexible(Int16 version) { return version >= 9; }
    static constexpr bool responseHeaderFlexible(Int16 version) { return version >= 9; }

    std::optional<String> transactional_id;
    /// -1 = all in-sync replicas, 1 = leader only, 0 = fire-and-forget.
    Int16 acks = -1;
    Int32 timeout_ms = 30'000;
    std::vector<ProduceTopicData> topic_data;

    bool operator==(const ProduceRequest &) const = default;

    void encode(Writer & w, Int16 version) const;
    static ProduceRequest decode(Reader & r, Int16 version);
};

}
