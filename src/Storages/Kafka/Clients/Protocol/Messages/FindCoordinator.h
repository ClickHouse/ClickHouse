#pragma once

/// FindCoordinator (api_key 10), supported range: v3-v4.
///
/// v4 batches coordinator keys; v3 (first flexible version, Kafka 2.4 max)
/// carries a single key and a flat single-coordinator response. The codec maps
/// v3 onto the same structs: coordinator_keys[0] on the way out, a
/// single-element coordinators vector (with the key left empty — v3 responses
/// do not echo it) on the way in.

#include <Storages/Kafka/Clients/Protocol/ApiKey.h>
#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <optional>
#include <vector>

namespace Kafka::Protocol
{

/// key_type values defined by the protocol.
inline constexpr Int8 coordinator_type_group = 0;
inline constexpr Int8 coordinator_type_transaction = 1;

struct Coordinator
{
    String key;
    Int32 node_id = -1;
    String host;
    Int32 port = -1;
    Int16 error_code = 0;
    std::optional<String> error_message;

    bool operator==(const Coordinator &) const = default;
};

struct FindCoordinatorResponse
{
    Int32 throttle_time_ms = 0;
    std::vector<Coordinator> coordinators;

    bool operator==(const FindCoordinatorResponse &) const = default;

    static FindCoordinatorResponse decode(Reader & r, Int16 version);
    void encode(Writer & w, Int16 version) const;
};

struct FindCoordinatorRequest
{
    using Response = FindCoordinatorResponse;
    static constexpr ApiKey api_key = ApiKey::FindCoordinator;
    static constexpr Int16 min_version = 3;
    static constexpr Int16 max_version = 4;
    static constexpr bool flexible(Int16) { return true; }
    static constexpr bool responseHeaderFlexible(Int16) { return true; }

    Int8 key_type = coordinator_type_group;
    std::vector<String> coordinator_keys;

    bool operator==(const FindCoordinatorRequest &) const = default;

    void encode(Writer & w, Int16 version) const;
    static FindCoordinatorRequest decode(Reader & r, Int16 version);
};

}
