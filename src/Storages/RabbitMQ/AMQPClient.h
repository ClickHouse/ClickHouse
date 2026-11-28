#pragma once

#include <base/types.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/Logger.h>
#include <Common/SecretString.h>
#include <Common/StringWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#include <chrono>
#include <memory>
#include <optional>


namespace DB
{

/// A minimal synchronous client of AMQP 0-9-1 with the RabbitMQ extensions, enough for the `RabbitMQ` table engine:
/// https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf
///
/// A connection has exactly one channel. Every method sends a request and, if the protocol has a reply for it,
/// waits for the reply. There is no event loop and no background thread: deliveries and publisher confirms,
/// which the broker sends on its own, are read only inside the calls of the connection, and buffered if they
/// arrive while waiting for a reply. AMQP heartbeats are disabled, as nothing would send them while the connection
/// is not used, e.g. while a materialized view processes a block; TCP keepalive detects a lost peer instead.
namespace AMQP
{

/// `amqp://user:password@host:port/vhost` or `amqps://...`.
struct Address
{
    String host;
    UInt16 port = 5672;
    String user;
    SecretString password;
    String vhost = "/";
    bool secure = false;

    /// The login is everything up to the first '@' after the scheme (`guest:guest` if there is no '@'),
    /// the vhost is everything after the first '/' following the host, up to an optional '?'.
    static Address parse(std::string_view uri);
};

enum class ExchangeType : uint8_t
{
    Fanout,
    Direct,
    Topic,
    Headers,
    ConsistentHash,
};

/// The name of the type in `exchange.declare`.
std::string_view exchangeTypeName(ExchangeType type);

/// An encoded AMQP field table. Only the value types the engine sends are supported.
class FieldTable
{
public:
    void addString(std::string_view name, std::string_view value);
    void addInteger(std::string_view name, Int64 value);
    void addBool(std::string_view name, bool value);
    void addTable(std::string_view name, const FieldTable & value);

    [[nodiscard]] const String & data() const { return encoded; }
    [[nodiscard]] bool empty() const { return encoded.empty(); }

private:
    void addName(std::string_view name);
    String encoded;
};

struct Delivery
{
    String body;
    String message_id;
    UInt64 timestamp = 0;
    UInt64 delivery_tag = 0;
    bool redelivered = false;
};

/// A publisher confirm: the broker has processed (`ack`) or failed to process (`nack`) the messages
/// up to `delivery_tag`, or only that message if `multiple` is false.
struct Confirm
{
    UInt64 delivery_tag = 0;
    bool multiple = false;
    bool ack = false;
};

struct MessageProperties
{
    String message_id;
    /// 2 makes the message persistent, 0 omits the property (the broker uses 1, transient).
    UInt8 delivery_mode = 0;
    FieldTable headers;
};

struct ConnectionSettings
{
    Address address;
    Poco::Timespan connect_timeout{5, 0};
    /// Bounds waiting for a reply and every single read or write on the socket.
    Poco::Timespan request_timeout{30, 0};
};

class Connection
{
public:
    /// Connects, authenticates and opens the channel. Throws `CANNOT_CONNECT_RABBITMQ` on any failure.
    Connection(const ConnectionSettings & settings, LoggerPtr log);

    /// Closes the socket without the closing handshake.
    ~Connection();

    /// The closing handshake (waiting for its reply at most `timeout`) and then closing the socket.
    /// Does not throw: it is used when the connection is not needed anymore, including after an error.
    void close(Poco::Timespan timeout);

    /// Whether the connection and its channel can still be used: nothing has failed and the broker did not close them.
    /// Detects a connection that the broker has closed in the meantime.
    [[nodiscard]] bool isUsable();

    /// The broker has closed the channel, but not the connection. Then this is the reason, otherwise empty.
    [[nodiscard]] const String & getChannelCloseReason() const { return channel_close_reason; }

    /// Shuts the socket down, so that a call blocked in another thread returns with an error. Thread-safe with the
    /// other methods, which is the only exception: it is how a stopped consumer interrupts a source that reads.
    void interrupt();

    void declareExchange(const String & name, ExchangeType type, bool durable, bool auto_delete, const FieldTable & arguments = {});
    void deleteExchange(const String & name);
    /// The RabbitMQ extension `exchange.bind`: messages from `source` are routed to `destination`.
    void bindExchange(const String & source, const String & destination, const String & routing_key, const FieldTable & arguments = {});

    struct QueueInfo
    {
        String name;
        UInt32 message_count = 0;
        UInt32 consumer_count = 0;
    };
    QueueInfo declareQueue(const String & name, bool durable, const FieldTable & arguments);
    void bindQueue(const String & queue, const String & exchange, const String & routing_key);
    /// Returns the number of messages deleted with the queue.
    UInt32 deleteQueue(const String & name, bool if_unused);

    /// Subscribes to the queue. Returns the consumer tag.
    String consume(const String & queue);
    void ack(UInt64 delivery_tag, bool multiple);
    void nack(UInt64 delivery_tag, bool multiple, bool requeue);
    void reject(UInt64 delivery_tag, bool requeue);

    /// Waits for the next delivery at most `timeout`. Returns `std::nullopt` if there is none.
    std::optional<Delivery> receiveDelivery(Poco::Timespan timeout);

    /// Reads, without waiting, everything that is already there, so that the closing of the connection or the channel
    /// by the broker is noticed even if it comes after many deliveries. Deliveries are buffered, at most `max_deliveries`.
    void receiveAvailable(size_t max_deliveries);

    /// Puts the channel into the confirm mode.
    void confirmSelect();
    /// Buffers the message. It is sent by `flush`, or once the buffer is full.
    void publish(const String & exchange, const String & routing_key, std::string_view body, const MessageProperties & properties);
    void flush();
    /// Waits for publisher confirms at most `timeout` and returns all that arrived.
    VectorWithMemoryTracking<Confirm> receiveConfirms(Poco::Timespan timeout);

private:
    struct Frame
    {
        UInt8 type = 0;
        UInt16 channel = 0;
        StringWithMemoryTracking payload;
    };

    struct Method
    {
        UInt16 class_id = 0;
        UInt16 method_id = 0;
        /// The arguments, without the class and method ids.
        StringWithMemoryTracking arguments;
    };

    void handshake();
    void openChannel();

    /// The socket is non-blocking: every read and write waits with `poll` against a deadline, also with TLS,
    /// where a blocking read would wait inside the TLS library for the rest of a record as long as bytes trickle in.
    void writeRaw(const char * data, size_t size);
    void flushOutput();
    /// Waits until some input is available (received bytes, or decrypted data in the TLS library). Returns false on timeout.
    bool waitForInput(std::chrono::steady_clock::time_point deadline);

    void sendFrame(UInt8 type, UInt16 channel, std::string_view payload);
    void sendMethod(UInt16 channel, UInt16 class_id, UInt16 method_id, std::string_view arguments);

    using Clock = std::chrono::steady_clock;

    /// Reads a frame if one starts arriving within `timeout`. Reading the rest of it is bounded by the request timeout.
    std::optional<Frame> receiveFrame(Poco::Timespan timeout);
    /// Reads a frame, which must be read completely before `deadline`. A broker can not stall the reading by sending slowly.
    Frame receiveFrameOrThrow(Clock::time_point deadline);
    Frame receiveFrameOrThrow();
    void readExact(char * to, size_t size, Clock::time_point deadline);

    /// Processes a frame that is not a reply to a request: deliveries, confirms, heartbeats,
    /// the closing of the channel or of the connection by the broker (throws then).
    /// Returns the method if it is a reply on the channel, which the caller is waiting for.
    std::optional<Method> processFrame(Frame frame);
    void readDeliveryContent(Delivery & delivery);

    /// Sends a method on the channel and waits for the reply with the given method id (in the same class).
    Method call(UInt16 class_id, UInt16 method_id, std::string_view arguments, UInt16 reply_method_id);
    Method waitForReply(UInt16 channel, UInt16 class_id, UInt16 reply_method_id);

    /// Reads frames until one of them is processed or `timeout` passes. Returns false on timeout.
    bool processIncoming(Poco::Timespan timeout);

    void checkUsable() const;
    [[noreturn]] void fail(const String & message);

    const ConnectionSettings settings;
    LoggerPtr log;
    String address_for_log;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    String input;
    size_t input_pos = 0;
    String output;

    /// Negotiated in `connection.tune`. Before that, the default of RabbitMQ, so that `connection.start` fits.
    UInt32 frame_max = 131072;

    /// Set when anything fails or the broker closes the connection: the connection can not be used anymore.
    String error;
    String channel_close_reason;

    DequeWithMemoryTracking<Delivery> deliveries;
    VectorWithMemoryTracking<Confirm> confirms;
};

}

}
