#pragma once

/// Kafka broker error codes as named constants.
///
/// The protocol returns errors as i16 codes inside response structs. This
/// names the codes the client acts on; everything else stays a raw Int16.
/// Names match the Kafka source's Errors.java.

#include <base/types.h>

namespace Kafka::BrokerError
{

inline constexpr Int16 NONE = 0;
inline constexpr Int16 OFFSET_OUT_OF_RANGE = 1;
inline constexpr Int16 CORRUPT_MESSAGE = 2;
inline constexpr Int16 UNKNOWN_TOPIC_OR_PARTITION = 3;
inline constexpr Int16 INVALID_FETCH_SIZE = 4;
inline constexpr Int16 LEADER_NOT_AVAILABLE = 5;
inline constexpr Int16 NOT_LEADER_OR_FOLLOWER = 6;
inline constexpr Int16 REQUEST_TIMED_OUT = 7;
inline constexpr Int16 BROKER_NOT_AVAILABLE = 8;
inline constexpr Int16 REPLICA_NOT_AVAILABLE = 9;
inline constexpr Int16 MESSAGE_TOO_LARGE = 10;
inline constexpr Int16 STALE_CONTROLLER_EPOCH = 11;
inline constexpr Int16 OFFSET_METADATA_TOO_LARGE = 12;
inline constexpr Int16 NETWORK_EXCEPTION = 13;
inline constexpr Int16 COORDINATOR_LOAD_IN_PROGRESS = 14;
inline constexpr Int16 COORDINATOR_NOT_AVAILABLE = 15;
inline constexpr Int16 NOT_COORDINATOR = 16;
inline constexpr Int16 INVALID_TOPIC_EXCEPTION = 17;
inline constexpr Int16 RECORD_LIST_TOO_LARGE = 18;
inline constexpr Int16 NOT_ENOUGH_REPLICAS = 19;
inline constexpr Int16 NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20;
inline constexpr Int16 INVALID_REQUIRED_ACKS = 21;
inline constexpr Int16 ILLEGAL_GENERATION = 22;
inline constexpr Int16 INCONSISTENT_GROUP_PROTOCOL = 23;
inline constexpr Int16 INVALID_GROUP_ID = 24;
inline constexpr Int16 UNKNOWN_MEMBER_ID = 25;
inline constexpr Int16 INVALID_SESSION_TIMEOUT = 26;
inline constexpr Int16 REBALANCE_IN_PROGRESS = 27;
inline constexpr Int16 INVALID_COMMIT_OFFSET_SIZE = 28;
inline constexpr Int16 TOPIC_AUTHORIZATION_FAILED = 29;
inline constexpr Int16 GROUP_AUTHORIZATION_FAILED = 30;
inline constexpr Int16 CLUSTER_AUTHORIZATION_FAILED = 31;
inline constexpr Int16 INVALID_TIMESTAMP = 32;
inline constexpr Int16 UNSUPPORTED_SASL_MECHANISM = 33;
inline constexpr Int16 ILLEGAL_SASL_STATE = 34;
inline constexpr Int16 UNSUPPORTED_VERSION = 35;
inline constexpr Int16 TOPIC_ALREADY_EXISTS = 36;
inline constexpr Int16 INVALID_PARTITIONS = 37;
inline constexpr Int16 INVALID_REPLICATION_FACTOR = 38;
inline constexpr Int16 INVALID_REPLICA_ASSIGNMENT = 39;
inline constexpr Int16 INVALID_CONFIG = 40;
inline constexpr Int16 NOT_CONTROLLER = 41;
inline constexpr Int16 SASL_AUTHENTICATION_FAILED = 58;
inline constexpr Int16 FENCED_LEADER_EPOCH = 74;
inline constexpr Int16 UNKNOWN_LEADER_EPOCH = 75;
inline constexpr Int16 UNSUPPORTED_COMPRESSION_TYPE = 76;
inline constexpr Int16 FENCED_INSTANCE_ID = 82;
inline constexpr Int16 UNKNOWN_TOPIC_ID = 100;

/// True for the two coordinator-state errors handled by invalidating the
/// cached coordinator id and re-resolving via FindCoordinator.
constexpr bool isCoordinatorStale(Int16 code)
{
    return code == COORDINATOR_NOT_AVAILABLE || code == NOT_COORDINATOR;
}

/// True for errors handled by refreshing cached metadata (leadership moved,
/// topic id unknown to this broker, epoch fenced).
constexpr bool isMetadataStale(Int16 code)
{
    return code == NOT_LEADER_OR_FOLLOWER || code == LEADER_NOT_AVAILABLE || code == UNKNOWN_TOPIC_OR_PARTITION
        || code == UNKNOWN_TOPIC_ID || code == FENCED_LEADER_EPOCH || code == UNKNOWN_LEADER_EPOCH;
}

}
