#pragma once

#include <Storages/RabbitMQ/RabbitMQConnection.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/Names.h>

#include <map>
#include <memory>
#include <amqpcpp.h>

namespace DB
{

/// A connection with one confirm-mode channel to publish messages to an exchange.
/// The channel callbacks capture the publisher, so it owns all per-channel state (delivery tags and
/// unconfirmed messages). This allows `RabbitMQPublisherPool` to hand the same connection and channel
/// to subsequent `INSERT` queries. A publisher is used by one thread at a time.
class RabbitMQPublisher
{
public:
    struct Payload
    {
        String message;
        UInt64 id{};
    };

    using Payloads = ConcurrentBoundedQueue<Payload>;

    /// Number of messages published before the event loop is run.
    static constexpr size_t BATCH = 1000;

    RabbitMQPublisher(
        const RabbitMQConfiguration & configuration_,
        const Names & routing_keys_,
        const String & exchange_name_,
        AMQP::ExchangeType exchange_type_,
        size_t channel_id_base_,
        bool persistent_,
        LoggerPtr log_);

    /// Connects and waits until the channel is ready. Throws `CANNOT_CONNECT_RABBITMQ`.
    void connect();

    /// Publishes a portion of messages (republished ones first), runs the event loop
    /// and restores the connection and the channel if they are lost.
    void publishStep(Payloads & payloads);

    /// Whether some messages are not confirmed by the server yet or have to be republished.
    bool hasPendingDeliveries() const { return !returned.empty() || !delivery_record.empty(); }

    /// Runs the event loop once to process what the server sent in the meantime (e.g. closed the connection),
    /// then checks that the connection and the channel are usable and nothing is pending.
    bool isReusable();

    int iterateEventLoop();

    /// Closes the channel and waits for the server confirmation with a timeout.
    void closeChannel();

    void disconnect(bool immediately = false);

    /// Heartbeat interval in seconds negotiated with the server, 0 if heartbeats are disabled.
    uint16_t getHeartbeatInterval() { return connection.getHandler().getHeartbeatInterval(); }

private:
    void setupChannel();
    void removeRecord(UInt64 received_delivery_tag, bool multiple, bool republish);
    void publish(Payloads & messages, bool republishing);

    RabbitMQConnection connection;

    const Names routing_keys;
    const String exchange_name;
    AMQP::ExchangeType exchange_type;
    const String channel_id_base; /// Serial number of the publisher
    const bool persistent;
    LoggerPtr log;

    AMQP::Table key_arguments;

    std::unique_ptr<AMQP::TcpChannel> channel;
    bool channel_ready = false;

    /// Channel errors lead to channel closure, need to count number of recreated channels to update channel id
    UInt64 channel_id_counter = 0;

    /// channel id which contains id of the publisher and serial number of recreated channel in this publisher
    String channel_id;

    /* returned.queue:
     *      - payloads are pushed to queue:
     *           1) inside channel->onError() callback if channel becomes unusable and the record of pending acknowledgements from server
     *              is non-empty.
     *           2) inside removeRecord() if received nack() - negative acknowledgement from the server that message failed to be written
     *              to disk or it was unable to reach the queue.
     *      - payloads are popped from the queue once republished
     */
    Payloads returned;

    /* Counter of current delivery on a current channel. Delivery tags are scoped per channel. The server attaches a delivery tag for each
     * published message - a serial number of delivery on current channel. Delivery tag is a way of server to notify publisher if it was
     * able or unable to process delivery, i.e. it sends back a response with a corresponding delivery tag.
     */
    UInt64 delivery_tag = 0;

    /// Record of pending acknowledgements from the server; its size never exceeds size of returned.queue
    std::map<UInt64, Payload> delivery_record;
};

using RabbitMQPublisherPtr = std::unique_ptr<RabbitMQPublisher>;

}
