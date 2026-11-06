#pragma once

/// Fetch (api_key 1), supported range: v11-v13.
///
/// v13 addresses topics by topic_id (UUID, from Metadata v10+); v11-v12 use
/// topic names. v11 is non-flexible (the boundary is v12) and is what Kafka
/// 2.4-3.0 brokers speak. The request carries replica_id = -1 for ordinary
/// consumers. last_fetched_epoch exists from v12 on.

#include <Storages/Kafka/Clients/Protocol/ApiKey.h>
#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <optional>
#include <string_view>
#include <vector>

namespace Kafka::Protocol
{

struct FetchPartition
{
    Int32 partition = 0;
    Int32 current_leader_epoch = -1;
    Int64 fetch_offset = 0;
    Int32 last_fetched_epoch = -1;
    Int64 log_start_offset = -1;
    Int32 partition_max_bytes = 1 << 20;

    bool operator==(const FetchPartition &) const = default;
};

struct FetchTopic
{
    /// Used by v13+; fill both name and id so the codec can emit either.
    Uuid topic_id;
    /// Used by v11-v12.
    String name;
    std::vector<FetchPartition> partitions;

    bool operator==(const FetchTopic &) const = default;
};

struct ForgottenTopic
{
    Uuid topic_id;
    String name;
    std::vector<Int32> partitions;

    bool operator==(const ForgottenTopic &) const = default;
};

struct AbortedTransaction
{
    Int64 producer_id = 0;
    Int64 first_offset = 0;

    bool operator==(const AbortedTransaction &) const = default;
};

struct FetchPartitionResponse
{
    Int32 partition_index = 0;
    Int16 error_code = 0;
    Int64 high_watermark = -1;
    Int64 last_stable_offset = -1;
    Int64 log_start_offset = -1;
    std::optional<std::vector<AbortedTransaction>> aborted_transactions;
    Int32 preferred_read_replica = -1;
    /// Encoded record-batch blob — zero-copy: a view into the buffer the
    /// response was decoded from (the connection's receive buffer), valid
    /// until the next request on that connection. nullopt = null bytes.
    std::optional<std::string_view> records;

    bool operator==(const FetchPartitionResponse &) const = default;
};

struct FetchTopicResponse
{
    Uuid topic_id; /// v13+
    String name; /// v11-v12
    std::vector<FetchPartitionResponse> partitions;

    bool operator==(const FetchTopicResponse &) const = default;
};

struct FetchResponse
{
    Int32 throttle_time_ms = 0;
    Int16 error_code = 0;
    Int32 session_id = 0;
    std::vector<FetchTopicResponse> responses;

    bool operator==(const FetchResponse &) const = default;

    static FetchResponse decode(Reader & r, Int16 version);
    void encode(Writer & w, Int16 version) const;
};

struct FetchRequest
{
    using Response = FetchResponse;
    static constexpr ApiKey api_key = ApiKey::Fetch;
    static constexpr Int16 min_version = 11;
    static constexpr Int16 max_version = 13;
    static constexpr bool flexible(Int16 version) { return version >= 12; }
    static constexpr bool responseHeaderFlexible(Int16 version) { return version >= 12; }
    /// True when this version routes by topic UUID instead of topic name.
    static constexpr bool usesTopicIds(Int16 version) { return version >= 13; }

    Int32 replica_id = -1;
    Int32 max_wait_ms = 500;
    Int32 min_bytes = 1;
    Int32 max_bytes = 50 << 20;
    Int8 isolation_level = 0; /// 0 = read_uncommitted
    Int32 session_id = 0;
    Int32 session_epoch = -1; /// -1 = sessionless (KIP-227 not used)
    std::vector<FetchTopic> topics;
    std::vector<ForgottenTopic> forgotten_topics_data;
    String rack_id;

    bool operator==(const FetchRequest &) const = default;

    void encode(Writer & w, Int16 version) const;
    static FetchRequest decode(Reader & r, Int16 version);
};

}
