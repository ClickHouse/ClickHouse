#include <Storages/Kafka/Clients/Error.h>

#include <Storages/Kafka/Clients/Protocol/BrokerError.h>

namespace DB::ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int KAFKA_ERROR;
    extern const int NETWORK_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
}

namespace Kafka
{

const char * brokerErrorName(Int16 code) noexcept
{
    using namespace BrokerError;
    switch (code)
    {
        case NONE: return "NONE";
        case OFFSET_OUT_OF_RANGE: return "OFFSET_OUT_OF_RANGE";
        case CORRUPT_MESSAGE: return "CORRUPT_MESSAGE";
        case UNKNOWN_TOPIC_OR_PARTITION: return "UNKNOWN_TOPIC_OR_PARTITION";
        case INVALID_FETCH_SIZE: return "INVALID_FETCH_SIZE";
        case LEADER_NOT_AVAILABLE: return "LEADER_NOT_AVAILABLE";
        case NOT_LEADER_OR_FOLLOWER: return "NOT_LEADER_OR_FOLLOWER";
        case REQUEST_TIMED_OUT: return "REQUEST_TIMED_OUT";
        case BROKER_NOT_AVAILABLE: return "BROKER_NOT_AVAILABLE";
        case REPLICA_NOT_AVAILABLE: return "REPLICA_NOT_AVAILABLE";
        case MESSAGE_TOO_LARGE: return "MESSAGE_TOO_LARGE";
        case STALE_CONTROLLER_EPOCH: return "STALE_CONTROLLER_EPOCH";
        case OFFSET_METADATA_TOO_LARGE: return "OFFSET_METADATA_TOO_LARGE";
        case NETWORK_EXCEPTION: return "NETWORK_EXCEPTION";
        case COORDINATOR_LOAD_IN_PROGRESS: return "COORDINATOR_LOAD_IN_PROGRESS";
        case COORDINATOR_NOT_AVAILABLE: return "COORDINATOR_NOT_AVAILABLE";
        case NOT_COORDINATOR: return "NOT_COORDINATOR";
        case INVALID_TOPIC_EXCEPTION: return "INVALID_TOPIC_EXCEPTION";
        case RECORD_LIST_TOO_LARGE: return "RECORD_LIST_TOO_LARGE";
        case NOT_ENOUGH_REPLICAS: return "NOT_ENOUGH_REPLICAS";
        case NOT_ENOUGH_REPLICAS_AFTER_APPEND: return "NOT_ENOUGH_REPLICAS_AFTER_APPEND";
        case INVALID_REQUIRED_ACKS: return "INVALID_REQUIRED_ACKS";
        case ILLEGAL_GENERATION: return "ILLEGAL_GENERATION";
        case INCONSISTENT_GROUP_PROTOCOL: return "INCONSISTENT_GROUP_PROTOCOL";
        case INVALID_GROUP_ID: return "INVALID_GROUP_ID";
        case UNKNOWN_MEMBER_ID: return "UNKNOWN_MEMBER_ID";
        case INVALID_SESSION_TIMEOUT: return "INVALID_SESSION_TIMEOUT";
        case REBALANCE_IN_PROGRESS: return "REBALANCE_IN_PROGRESS";
        case INVALID_COMMIT_OFFSET_SIZE: return "INVALID_COMMIT_OFFSET_SIZE";
        case TOPIC_AUTHORIZATION_FAILED: return "TOPIC_AUTHORIZATION_FAILED";
        case GROUP_AUTHORIZATION_FAILED: return "GROUP_AUTHORIZATION_FAILED";
        case CLUSTER_AUTHORIZATION_FAILED: return "CLUSTER_AUTHORIZATION_FAILED";
        case INVALID_TIMESTAMP: return "INVALID_TIMESTAMP";
        case UNSUPPORTED_SASL_MECHANISM: return "UNSUPPORTED_SASL_MECHANISM";
        case ILLEGAL_SASL_STATE: return "ILLEGAL_SASL_STATE";
        case UNSUPPORTED_VERSION: return "UNSUPPORTED_VERSION";
        case TOPIC_ALREADY_EXISTS: return "TOPIC_ALREADY_EXISTS";
        case INVALID_PARTITIONS: return "INVALID_PARTITIONS";
        case INVALID_REPLICATION_FACTOR: return "INVALID_REPLICATION_FACTOR";
        case INVALID_REPLICA_ASSIGNMENT: return "INVALID_REPLICA_ASSIGNMENT";
        case INVALID_CONFIG: return "INVALID_CONFIG";
        case NOT_CONTROLLER: return "NOT_CONTROLLER";
        case SASL_AUTHENTICATION_FAILED: return "SASL_AUTHENTICATION_FAILED";
        case FENCED_LEADER_EPOCH: return "FENCED_LEADER_EPOCH";
        case UNKNOWN_LEADER_EPOCH: return "UNKNOWN_LEADER_EPOCH";
        case UNSUPPORTED_COMPRESSION_TYPE: return "UNSUPPORTED_COMPRESSION_TYPE";
        case FENCED_INSTANCE_ID: return "FENCED_INSTANCE_ID";
        case UNKNOWN_TOPIC_ID: return "UNKNOWN_TOPIC_ID";
        default: return "UNKNOWN";
    }
}

/// Mirrors Errors.java's RetriableException subtree (for the codes above).
bool isBrokerErrorRetriable(Int16 code) noexcept
{
    using namespace BrokerError;
    switch (code)
    {
        case CORRUPT_MESSAGE:
        case UNKNOWN_TOPIC_OR_PARTITION:
        case LEADER_NOT_AVAILABLE:
        case NOT_LEADER_OR_FOLLOWER:
        case REQUEST_TIMED_OUT:
        case BROKER_NOT_AVAILABLE:
        case REPLICA_NOT_AVAILABLE:
        case NETWORK_EXCEPTION:
        case COORDINATOR_LOAD_IN_PROGRESS:
        case COORDINATOR_NOT_AVAILABLE:
        case NOT_COORDINATOR:
        case NOT_ENOUGH_REPLICAS:
        case NOT_ENOUGH_REPLICAS_AFTER_APPEND:
        case REBALANCE_IN_PROGRESS:
        case NOT_CONTROLLER:
        case FENCED_LEADER_EPOCH:
        case UNKNOWN_LEADER_EPOCH:
        case UNKNOWN_TOPIC_ID:
            return true;
        default:
            return false;
    }
}

const char * errorKindName(ErrorKind kind) noexcept
{
    switch (kind)
    {
        case ErrorKind::Config: return "Config";
        case ErrorKind::Protocol: return "Protocol";
        case ErrorKind::Connection: return "Connection";
        case ErrorKind::Timeout: return "Timeout";
        case ErrorKind::Wakeup: return "Wakeup";
        case ErrorKind::Broker: return "Broker";
        case ErrorKind::NotSupported: return "NotSupported";
    }
    return "Unknown";
}

int clickHouseErrorCode(ErrorKind kind) noexcept
{
    switch (kind)
    {
        case ErrorKind::Config: return DB::ErrorCodes::BAD_ARGUMENTS;
        case ErrorKind::Protocol: return DB::ErrorCodes::INCORRECT_DATA;
        case ErrorKind::Connection: return DB::ErrorCodes::NETWORK_ERROR;
        case ErrorKind::Timeout: return DB::ErrorCodes::TIMEOUT_EXCEEDED;
        case ErrorKind::Wakeup: return DB::ErrorCodes::ABORTED;
        case ErrorKind::Broker: return DB::ErrorCodes::KAFKA_ERROR;
        case ErrorKind::NotSupported: return DB::ErrorCodes::NOT_IMPLEMENTED;
    }
    return DB::ErrorCodes::KAFKA_ERROR;
}

void Exception::appendBrokerError()
{
    if (kind == ErrorKind::Broker)
        addMessage("Broker error: {} ({})", brokerErrorName(broker_code), broker_code);
}

bool Exception::isRetriable() const noexcept
{
    switch (kind)
    {
        case ErrorKind::Broker:
            return isBrokerErrorRetriable(broker_code);
        case ErrorKind::Connection:
            return true;
        default:
            return false;
    }
}

}
