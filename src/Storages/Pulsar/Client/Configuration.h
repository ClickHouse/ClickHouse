#pragma once

#include <functional>
#include <string>

namespace DB::Pulsar
{

/// Where a brand-new subscription starts reading. Ignored by the broker if the subscription already
/// exists. The names match the Apache client so the engine can keep using the same identifiers.
enum InitialPosition
{
    InitialPositionLatest = 0,
    InitialPositionEarliest = 1,
};

/// The subscription type. Only `ConsumerShared` is used by the engine; the others are listed so the
/// enumerators keep their Apache-client meaning if ever needed.
enum ConsumerType
{
    ConsumerExclusive = 0,
    ConsumerShared = 1,
    ConsumerFailover = 2,
    ConsumerKeyShared = 3,
};

/// Called before the client opens a TCP connection to a broker address (`host:port`), both for the
/// service-URL brokers and for every address learned from a lookup response. Returning false refuses
/// the connection. Used by the engine to apply `remote_url_allow_hosts` to lookup results.
using ConnectionValidator = std::function<bool(const std::string &)>;

struct ClientConfiguration
{
    /// Verify that a `pulsar+ssl://` broker certificate matches the host name. Kept for API parity;
    /// this client does not implement TLS yet and refuses `pulsar+ssl://` service URLs.
    bool validate_hostname = false;
    ConnectionValidator connection_validator;
    /// How long to wait for a single broker operation (connect, lookup, subscribe, producer create).
    int operation_timeout_ms = 30000;

    ClientConfiguration & setValidateHostName(bool value)
    {
        validate_hostname = value;
        return *this;
    }

    ClientConfiguration & setConnectionValidator(ConnectionValidator validator)
    {
        connection_validator = std::move(validator);
        return *this;
    }

    ClientConfiguration & setOperationTimeout(int milliseconds)
    {
        operation_timeout_ms = milliseconds;
        return *this;
    }
};

struct ProducerConfiguration
{
    /// How long a `send` waits for the broker to acknowledge before failing. Must be positive; the
    /// engine validates this and rejects a zero or oversized value.
    int send_timeout_ms = 30000;
    /// Kept for API parity. This client sends synchronously (one in-flight message at a time), so the
    /// queue is never "full"; the flag has no effect.
    bool block_if_queue_full = false;

    ProducerConfiguration & setSendTimeout(int milliseconds)
    {
        send_timeout_ms = milliseconds;
        return *this;
    }

    ProducerConfiguration & setBlockIfQueueFull(bool value)
    {
        block_if_queue_full = value;
        return *this;
    }
};

/// Controls a single `batchReceive`: at most `max_messages` messages, waiting at most `timeout_ms`.
/// `max_bytes` is accepted for API parity but not enforced (the engine passes 0).
struct BatchReceivePolicy
{
    BatchReceivePolicy() = default;
    BatchReceivePolicy(int max_messages_, long max_bytes_, long timeout_ms_)
        : max_messages(max_messages_), max_bytes(max_bytes_), timeout_ms(timeout_ms_)
    {
    }

    int max_messages = 100;
    long max_bytes = 0;
    long timeout_ms = 1000;
};

struct ConsumerConfiguration
{
    ConsumerType consumer_type = ConsumerShared;
    InitialPosition subscription_initial_position = InitialPositionLatest;
    BatchReceivePolicy batch_receive_policy;

    ConsumerConfiguration & setConsumerType(ConsumerType type)
    {
        consumer_type = type;
        return *this;
    }

    ConsumerConfiguration & setSubscriptionInitialPosition(InitialPosition position)
    {
        subscription_initial_position = position;
        return *this;
    }

    ConsumerConfiguration & setBatchReceivePolicy(BatchReceivePolicy policy)
    {
        batch_receive_policy = policy;
        return *this;
    }
};

}
