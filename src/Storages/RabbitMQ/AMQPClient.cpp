#include <Storages/RabbitMQ/AMQPClient.h>

#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <base/unaligned.h>

#include "config.h"

#if USE_SSL
#include <Poco/Net/Context.h>
#include <Poco/Net/SecureStreamSocket.h>
#endif

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CONNECT_RABBITMQ;
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace AMQP
{

namespace
{

constexpr UInt8 FRAME_METHOD = 1;
constexpr UInt8 FRAME_HEADER = 2;
constexpr UInt8 FRAME_BODY = 3;
constexpr UInt8 FRAME_HEARTBEAT = 8;
constexpr UInt8 FRAME_END = 0xCE;

/// The type, the channel, and the size of the payload.
constexpr size_t FRAME_HEADER_SIZE = 7;
/// The header and the end octet.
constexpr size_t FRAME_OVERHEAD = FRAME_HEADER_SIZE + 1;

/// The maximum frame size proposed by the broker is clamped to this range. The lower bound is the minimum of the protocol.
/// The upper bound limits the memory for one frame: a broker could propose up to 4 GiB.
constexpr UInt32 MIN_FRAME_SIZE = 4096;
constexpr UInt32 MAX_FRAME_SIZE = 128 * 1024 * 1024;

/// RabbitMQ does not accept messages larger than 512 MiB.
constexpr UInt64 MAX_MESSAGE_SIZE = 512 * 1024 * 1024;

/// The only channel of a connection.
constexpr UInt16 CHANNEL = 1;

namespace Class
{
    constexpr UInt16 CONNECTION = 10;
    constexpr UInt16 CHANNEL = 20;
    constexpr UInt16 EXCHANGE = 40;
    constexpr UInt16 QUEUE = 50;
    constexpr UInt16 BASIC = 60;
    constexpr UInt16 CONFIRM = 85;
}

namespace ConnectionMethod
{
    constexpr UInt16 START = 10;
    constexpr UInt16 START_OK = 11;
    constexpr UInt16 TUNE = 30;
    constexpr UInt16 TUNE_OK = 31;
    constexpr UInt16 OPEN = 40;
    constexpr UInt16 OPEN_OK = 41;
    constexpr UInt16 CLOSE = 50;
    constexpr UInt16 CLOSE_OK = 51;
    constexpr UInt16 BLOCKED = 60;
    constexpr UInt16 UNBLOCKED = 61;
}

namespace ChannelMethod
{
    constexpr UInt16 OPEN = 10;
    constexpr UInt16 OPEN_OK = 11;
    constexpr UInt16 FLOW = 20;
    constexpr UInt16 FLOW_OK = 21;
    constexpr UInt16 CLOSE = 40;
    constexpr UInt16 CLOSE_OK = 41;
}

namespace ExchangeMethod
{
    constexpr UInt16 DECLARE = 10;
    constexpr UInt16 DECLARE_OK = 11;
    constexpr UInt16 DELETE = 20;
    constexpr UInt16 DELETE_OK = 21;
    constexpr UInt16 BIND = 30;
    constexpr UInt16 BIND_OK = 31;
}

namespace QueueMethod
{
    constexpr UInt16 DECLARE = 10;
    constexpr UInt16 DECLARE_OK = 11;
    constexpr UInt16 BIND = 20;
    constexpr UInt16 BIND_OK = 21;
    constexpr UInt16 DELETE = 40;
    constexpr UInt16 DELETE_OK = 41;
}

namespace BasicMethod
{
    constexpr UInt16 CONSUME = 20;
    constexpr UInt16 CONSUME_OK = 21;
    constexpr UInt16 CANCEL = 30;
    constexpr UInt16 PUBLISH = 40;
    constexpr UInt16 RETURN = 50;
    constexpr UInt16 DELIVER = 60;
    constexpr UInt16 ACK = 80;
    constexpr UInt16 REJECT = 90;
    constexpr UInt16 NACK = 120;
}

namespace ConfirmMethod
{
    constexpr UInt16 SELECT = 10;
    constexpr UInt16 SELECT_OK = 11;
}

/// The flags of the basic properties in the content header, from the most significant bit.
namespace Property
{
    constexpr UInt16 CONTENT_TYPE = 1 << 15;
    constexpr UInt16 CONTENT_ENCODING = 1 << 14;
    constexpr UInt16 HEADERS = 1 << 13;
    constexpr UInt16 DELIVERY_MODE = 1 << 12;
    constexpr UInt16 PRIORITY = 1 << 11;
    constexpr UInt16 CORRELATION_ID = 1 << 10;
    constexpr UInt16 REPLY_TO = 1 << 9;
    constexpr UInt16 EXPIRATION = 1 << 8;
    constexpr UInt16 MESSAGE_ID = 1 << 7;
    constexpr UInt16 TIMESTAMP = 1 << 6;
    /// The rest (type, user id, app id, cluster id) are not used.
    /// Another word of flags follows.
    constexpr UInt16 CONTINUATION = 1;
}

void writeOctet(String & out, UInt8 value)
{
    out.push_back(static_cast<char>(value));
}

void writeShort(String & out, UInt16 value)
{
    char buf[sizeof(value)];
    unalignedStoreBigEndian<UInt16>(buf, value);
    out.append(buf, sizeof(buf));
}

void writeLong(String & out, UInt32 value)
{
    char buf[sizeof(value)];
    unalignedStoreBigEndian<UInt32>(buf, value);
    out.append(buf, sizeof(buf));
}

void writeLongLong(String & out, UInt64 value)
{
    char buf[sizeof(value)];
    unalignedStoreBigEndian<UInt64>(buf, value);
    out.append(buf, sizeof(buf));
}

void writeShortString(String & out, std::string_view value)
{
    if (value.size() > 255)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The AMQP name '{}' is longer than 255 bytes", value);
    writeOctet(out, static_cast<UInt8>(value.size()));
    out.append(value);
}

void writeLongString(String & out, std::string_view value)
{
    if (value.size() > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The AMQP string of size {} is too long", value.size());
    writeLong(out, static_cast<UInt32>(value.size()));
    out.append(value);
}

void writeTable(String & out, const FieldTable & table)
{
    writeLongString(out, table.data());
}

/// Reads the arguments of a method or the content header. Every read is checked against the end of the data.
class Reader
{
public:
    explicit Reader(std::string_view data_) : data(data_) {}

    UInt8 readOctet() { return static_cast<UInt8>(take(1)[0]); }
    UInt16 readShort() { return unalignedLoadBigEndian<UInt16>(take(2).data()); }
    UInt32 readLong() { return unalignedLoadBigEndian<UInt32>(take(4).data()); }
    UInt64 readLongLong() { return unalignedLoadBigEndian<UInt64>(take(8).data()); }
    String readShortString() { return String(take(readOctet())); }
    String readLongString() { return String(take(readLong())); }
    void skipShortString() { take(readOctet()); }
    void skipLongString() { take(readLong()); }

private:
    std::string_view take(size_t size)
    {
        if (data.size() - pos < size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Truncated AMQP frame from RabbitMQ: {} more bytes expected, {} left", size, data.size() - pos);
        auto result = data.substr(pos, size);
        pos += size;
        return result;
    }

    std::string_view data;
    size_t pos = 0;
};

#if USE_SSL
/// The certificate of the broker is not verified, as it was not with the library used before.
Poco::Net::Context::Ptr getTLSContext()
{
    static Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::CLIENT_USE, "", Poco::Net::Context::VERIFY_NONE);
    return context;
}
#endif

String formatReply(Reader & reader)
{
    const UInt16 code = reader.readShort();
    const String text = reader.readShortString();
    const UInt16 class_id = reader.readShort();
    const UInt16 method_id = reader.readShort();
    if (class_id || method_id)
        return fmt::format("{} {} (in reply to method {}.{})", code, text, class_id, method_id);
    return fmt::format("{} {}", code, text);
}

}


Address Address::parse(std::string_view uri)
{
    Address address;
    if (uri.starts_with("amqps://"))
    {
        address.secure = true;
        address.port = 5671;
        uri.remove_prefix(8);
    }
    else if (uri.starts_with("amqp://"))
    {
        uri.remove_prefix(7);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The RabbitMQ address must start with `amqp://` or `amqps://`");

    if (auto at = uri.find('@'); at != std::string_view::npos)
    {
        const std::string_view login = uri.substr(0, at);
        const auto colon = login.find(':');
        address.user = String(login.substr(0, colon));
        if (colon != std::string_view::npos)
            address.password = SecretString(String(login.substr(colon + 1)));
        uri.remove_prefix(at + 1);
    }

    std::string_view host_port = uri;
    if (auto slash = uri.find('/'); slash != std::string_view::npos)
    {
        host_port = uri.substr(0, slash);
        std::string_view vhost = uri.substr(slash + 1);
        vhost = vhost.substr(0, vhost.find('?'));
        if (!vhost.empty())
            address.vhost = String(vhost);
    }
    else
    {
        host_port = host_port.substr(0, host_port.find('?'));
    }

    if (auto colon = host_port.find(':'); colon != std::string_view::npos)
    {
        const std::string_view port = host_port.substr(colon + 1);
        UInt64 value = 0;
        if (port.empty() || port.size() > 5 || !std::all_of(port.begin(), port.end(), isNumericASCII))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid port '{}' in the RabbitMQ address", port);
        for (char c : port)
            value = value * 10 + (c - '0');
        if (value == 0 || value > std::numeric_limits<UInt16>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid port '{}' in the RabbitMQ address", port);
        address.port = static_cast<UInt16>(value);
        host_port = host_port.substr(0, colon);
    }

    if (host_port.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No host in the RabbitMQ address");
    address.host = String(host_port);
    return address;
}


std::string_view exchangeTypeName(ExchangeType type)
{
    switch (type)
    {
        case ExchangeType::Fanout: return "fanout";
        case ExchangeType::Direct: return "direct";
        case ExchangeType::Topic: return "topic";
        case ExchangeType::Headers: return "headers";
        case ExchangeType::ConsistentHash: return "x-consistent-hash";
    }
    UNREACHABLE();
}


void FieldTable::addName(std::string_view name)
{
    writeShortString(encoded, name);
}

void FieldTable::addString(std::string_view name, std::string_view value)
{
    addName(name);
    writeOctet(encoded, 'S');
    writeLongString(encoded, value);
}

void FieldTable::addInteger(std::string_view name, Int64 value)
{
    addName(name);
    writeOctet(encoded, 'l');
    writeLongLong(encoded, static_cast<UInt64>(value));
}

void FieldTable::addBool(std::string_view name, bool value)
{
    addName(name);
    writeOctet(encoded, 't');
    writeOctet(encoded, value ? 1 : 0);
}

void FieldTable::addTable(std::string_view name, const FieldTable & value)
{
    addName(name);
    writeOctet(encoded, 'F');
    writeTable(encoded, value);
}


Connection::Connection(const ConnectionSettings & settings_, LoggerPtr log_)
    : settings(settings_)
    , log(std::move(log_))
    , address_for_log(fmt::format("{}:{}", settings.address.host, settings.address.port))
{
    try
    {
        if (settings.address.secure)
        {
#if USE_SSL
            auto secure_socket = std::make_unique<Poco::Net::SecureStreamSocket>(getTLSContext());
            secure_socket->setPeerHostName(settings.address.host);
            /// The handshake is postponed until the first read or write, which are bounded by the request timeout.
            secure_socket->setLazyHandshake(true);
            socket = std::move(secure_socket);
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "TLS connections to RabbitMQ are disabled, because ClickHouse was built without SSL support");
#endif
        }
        else
        {
            socket = std::make_unique<Poco::Net::StreamSocket>();
        }

        const auto addresses = DNSResolver::instance().resolveAddressList(settings.address.host, settings.address.port);
        if (addresses.empty())
            throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot resolve the host {}", settings.address.host);
        for (size_t i = 0; i < addresses.size(); ++i)
        {
            try
            {
                socket->connect(addresses[i], settings.connect_timeout);
                break;
            }
            catch (...)
            {
                if (i + 1 == addresses.size())
                    throw;
            }
        }

        socket->setReceiveTimeout(settings.request_timeout);
        socket->setSendTimeout(settings.request_timeout);
        socket->setNoDelay(true);

        in = std::make_unique<ReadBufferFromPocoSocket>(*socket);
        out = std::make_unique<WriteBufferFromPocoSocket>(*socket);

        handshake();
        openChannel();
    }
    catch (...)
    {
        if (out)
            out->cancel();
        throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot connect to RabbitMQ {}: {}", address_for_log, getCurrentExceptionMessage(false));
    }

    LOG_TRACE(log, "Connected to RabbitMQ {} (heartbeat interval {} s, maximum frame size {})", address_for_log, heartbeat_interval, frame_max);
}

Connection::~Connection()
{
    /// Unsent data is not needed anymore: the connection is not usable or was closed already.
    if (out)
        out->cancel();
}

void Connection::fail(const String & message)
{
    if (error.empty())
        error = message;
    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER, "RabbitMQ {}: {}", address_for_log, message);
}

void Connection::checkUsable() const
{
    if (!error.empty())
        throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "The connection to RabbitMQ {} is not usable: {}", address_for_log, error);
}

bool Connection::isUsable()
{
    if (!error.empty())
        return false;

    /// Reads what is already there: the closing of the connection by the broker or the end of the stream.
    try
    {
        processIncoming(Poco::Timespan(0));
    }
    catch (...)
    {
        if (error.empty())
            error = getCurrentExceptionMessage(false);
    }
    return error.empty();
}

void Connection::sendFrame(UInt8 type, UInt16 channel, std::string_view payload)
{
    char header[FRAME_HEADER_SIZE];
    header[0] = static_cast<char>(type);
    unalignedStoreBigEndian<UInt16>(header + 1, channel);
    unalignedStoreBigEndian<UInt32>(header + 3, static_cast<UInt32>(payload.size()));

    out->write(header, FRAME_HEADER_SIZE);
    out->write(payload.data(), payload.size());
    out->write(static_cast<char>(FRAME_END));
    last_sent = std::chrono::steady_clock::now();
}

void Connection::sendMethod(UInt16 channel, UInt16 class_id, UInt16 method_id, std::string_view arguments)
{
    String payload;
    payload.reserve(4 + arguments.size());
    writeShort(payload, class_id);
    writeShort(payload, method_id);
    payload.append(arguments);
    sendFrame(FRAME_METHOD, channel, payload);
}

void Connection::sendHeartbeatIfNeeded()
{
    if (!heartbeat_interval || !error.empty())
        return;

    if (std::chrono::steady_clock::now() - last_sent < std::chrono::seconds(heartbeat_interval) / 2)
        return;

    try
    {
        sendFrame(FRAME_HEARTBEAT, 0, {});
        out->next();
    }
    catch (...)
    {
        if (error.empty())
            error = getCurrentExceptionMessage(false);
        throw;
    }
}

std::optional<Connection::Frame> Connection::receiveFrame(Poco::Timespan timeout)
{
    if (!in->poll(static_cast<size_t>(std::max<Int64>(timeout.totalMicroseconds(), 0))))
        return {};
    return receiveFrameOrThrow();
}

Connection::Frame Connection::receiveFrameOrThrow()
{
    char header[FRAME_HEADER_SIZE];
    in->readStrict(header, FRAME_HEADER_SIZE);

    Frame frame;
    frame.type = static_cast<UInt8>(header[0]);
    frame.channel = unalignedLoadBigEndian<UInt16>(header + 1);
    const UInt32 size = unalignedLoadBigEndian<UInt32>(header + 3);

    if (size > frame_max - FRAME_OVERHEAD)
        fail(fmt::format("frame size exceeded: the frame has a payload of {} bytes, the maximum frame size is {}", size, frame_max));

    frame.payload.resize(size);
    in->readStrict(frame.payload.data(), size);

    char end = 0;
    in->readStrict(&end, 1);
    if (static_cast<UInt8>(end) != FRAME_END)
        fail(fmt::format("invalid frame end octet 0x{:02x}", static_cast<UInt32>(static_cast<UInt8>(end))));

    last_received = std::chrono::steady_clock::now();
    return frame;
}

void Connection::readDeliveryContent(Delivery & delivery)
{
    /// The content header and the body frames follow the method. Heartbeats may come in between.
    Frame header;
    do
        header = receiveFrameOrThrow();
    while (header.type == FRAME_HEARTBEAT);

    if (header.type != FRAME_HEADER || header.channel != CHANNEL)
        fail(fmt::format("expected a content header, got a frame of type {} on channel {}", static_cast<UInt32>(header.type), header.channel));

    Reader reader(header.payload);
    reader.readShort(); /// class id
    reader.readShort(); /// weight
    const UInt64 body_size = reader.readLongLong();
    if (body_size > MAX_MESSAGE_SIZE)
        fail(fmt::format("the message of size {} is larger than the maximum {}", body_size, MAX_MESSAGE_SIZE));

    const UInt16 flags = reader.readShort();
    /// No properties are defined in the following words of flags.
    for (UInt16 more = flags; more & Property::CONTINUATION;)
        more = reader.readShort();

    if (flags & Property::CONTENT_TYPE)
        reader.skipShortString();
    if (flags & Property::CONTENT_ENCODING)
        reader.skipShortString();
    if (flags & Property::HEADERS)
        reader.skipLongString();
    if (flags & Property::DELIVERY_MODE)
        reader.readOctet();
    if (flags & Property::PRIORITY)
        reader.readOctet();
    if (flags & Property::CORRELATION_ID)
        reader.skipShortString();
    if (flags & Property::REPLY_TO)
        reader.skipShortString();
    if (flags & Property::EXPIRATION)
        reader.skipShortString();
    if (flags & Property::MESSAGE_ID)
        delivery.message_id = reader.readShortString();
    if (flags & Property::TIMESTAMP)
        delivery.timestamp = reader.readLongLong();
    /// The remaining properties (type, user id, app id, cluster id) are not used.

    /// The size is checked above, but the data is reserved only as it arrives.
    delivery.body.reserve(std::min<UInt64>(body_size, frame_max));
    while (delivery.body.size() < body_size)
    {
        Frame body = receiveFrameOrThrow();
        if (body.type == FRAME_HEARTBEAT)
            continue;
        if (body.type != FRAME_BODY || body.channel != CHANNEL)
            fail(fmt::format("expected a content body, got a frame of type {} on channel {}", static_cast<UInt32>(body.type), body.channel));
        if (body.payload.size() > body_size - delivery.body.size())
            fail("the content body is larger than declared in the content header");
        delivery.body.append(body.payload);
    }
}

std::optional<Connection::Method> Connection::processFrame(Frame frame)
{
    if (frame.type == FRAME_HEARTBEAT)
        return {};

    if (frame.type != FRAME_METHOD)
        fail(fmt::format("unexpected frame of type {} on channel {}", static_cast<UInt32>(frame.type), frame.channel));

    Reader reader(frame.payload);
    Method method;
    method.class_id = reader.readShort();
    method.method_id = reader.readShort();
    frame.payload.erase(0, 4);
    method.arguments = std::move(frame.payload);
    Reader args(method.arguments);

    if (frame.channel == 0)
    {
        if (method.class_id == Class::CONNECTION && method.method_id == ConnectionMethod::CLOSE)
        {
            const String reason = formatReply(args);
            sendMethod(0, Class::CONNECTION, ConnectionMethod::CLOSE_OK, {});
            out->next();
            fail(fmt::format("the broker closed the connection: {}", reason));
        }
        if (method.class_id == Class::CONNECTION
            && (method.method_id == ConnectionMethod::BLOCKED || method.method_id == ConnectionMethod::UNBLOCKED))
            return {};
        return method;
    }

    if (frame.channel != CHANNEL)
        fail(fmt::format("a frame on the unknown channel {}", frame.channel));

    if (method.class_id == Class::BASIC)
    {
        switch (method.method_id)
        {
            case BasicMethod::DELIVER:
            {
                Delivery delivery;
                args.skipShortString(); /// consumer tag
                delivery.delivery_tag = args.readLongLong();
                delivery.redelivered = args.readOctet() & 1;
                readDeliveryContent(delivery);
                deliveries.push_back(std::move(delivery));
                return {};
            }
            case BasicMethod::ACK:
            case BasicMethod::NACK:
            {
                Confirm confirm;
                confirm.delivery_tag = args.readLongLong();
                confirm.multiple = args.readOctet() & 1;
                confirm.ack = method.method_id == BasicMethod::ACK;
                confirms.push_back(confirm);
                return {};
            }
            case BasicMethod::RETURN:
            {
                /// Not expected, as messages are published without the `mandatory` flag. The content is skipped.
                Delivery returned;
                readDeliveryContent(returned);
                return {};
            }
            case BasicMethod::CANCEL:
                fail("the broker cancelled the consumer, probably the queue was deleted");
            default:
                break;
        }
    }

    if (method.class_id == Class::CHANNEL && method.method_id == ChannelMethod::CLOSE)
    {
        channel_close_reason = formatReply(args);
        sendMethod(CHANNEL, Class::CHANNEL, ChannelMethod::CLOSE_OK, {});
        out->next();
        fail(fmt::format("the broker closed the channel: {}", channel_close_reason));
    }

    if (method.class_id == Class::CHANNEL && method.method_id == ChannelMethod::FLOW)
    {
        String reply;
        writeOctet(reply, args.readOctet());
        sendMethod(CHANNEL, Class::CHANNEL, ChannelMethod::FLOW_OK, reply);
        out->next();
        return {};
    }

    return method;
}

bool Connection::processIncoming(Poco::Timespan timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(timeout.totalMicroseconds());
    while (true)
    {
        sendHeartbeatIfNeeded();

        auto now = std::chrono::steady_clock::now();
        auto wait = std::max(deadline - now, std::chrono::steady_clock::duration::zero());
        if (heartbeat_interval)
        {
            /// Wake up in time to send the next heartbeat.
            const auto heartbeat_due = last_sent + std::chrono::seconds(heartbeat_interval) / 2;
            wait = std::clamp(heartbeat_due - now, std::chrono::steady_clock::duration::zero(), wait);
        }

        if (auto frame = receiveFrame(Poco::Timespan(std::chrono::duration_cast<std::chrono::microseconds>(wait).count())))
        {
            if (auto method = processFrame(std::move(*frame)))
                fail(fmt::format("unexpected method {}.{}", method->class_id, method->method_id));
            if (!deliveries.empty() || !confirms.empty())
                return true;
            continue;
        }

        /// The broker sends heartbeats too. Its silence for two intervals means that the connection is lost.
        /// Checked only when nothing is there to read, as a connection that was not used for a while has unread heartbeats.
        now = std::chrono::steady_clock::now();
        if (heartbeat_interval && now - last_received > 2 * std::chrono::seconds(heartbeat_interval))
            fail(fmt::format("no data was received for {} seconds", 2 * heartbeat_interval));

        if (now >= deadline)
            return false;
    }
}

Connection::Method Connection::waitForReply(UInt16 channel, UInt16 class_id, UInt16 reply_method_id)
{
    out->next();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(settings.request_timeout.totalMicroseconds());
    while (true)
    {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
        {
            if (error.empty())
                error = "timeout";
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded while waiting for a reply from RabbitMQ {}", address_for_log);
        }

        sendHeartbeatIfNeeded();
        auto wait = deadline - now;
        if (heartbeat_interval)
            wait = std::clamp(last_sent + std::chrono::seconds(heartbeat_interval) / 2 - now, std::chrono::steady_clock::duration::zero(), wait);

        auto frame = receiveFrame(Poco::Timespan(std::chrono::duration_cast<std::chrono::microseconds>(wait).count()));
        if (!frame)
            continue;

        const UInt16 frame_channel = frame->channel;
        auto method = processFrame(std::move(*frame));
        if (!method)
            continue;

        if (frame_channel != channel || method->class_id != class_id || method->method_id != reply_method_id)
            fail(fmt::format("unexpected method {}.{} on channel {}, expected {}.{}",
                method->class_id, method->method_id, frame_channel, class_id, reply_method_id));
        return std::move(*method);
    }
}

Connection::Method Connection::call(UInt16 class_id, UInt16 method_id, std::string_view arguments, UInt16 reply_method_id)
{
    checkUsable();
    try
    {
        sendMethod(CHANNEL, class_id, method_id, arguments);
        return waitForReply(CHANNEL, class_id, reply_method_id);
    }
    catch (...)
    {
        if (error.empty())
            error = getCurrentExceptionMessage(false);
        throw;
    }
}

void Connection::handshake()
{
    out->write("AMQP\x00\x00\x09\x01", 8);

    {
        auto start = waitForReply(0, Class::CONNECTION, ConnectionMethod::START);
        Reader reader(start.arguments);
        const UInt8 major = reader.readOctet();
        const UInt8 minor = reader.readOctet();
        if (major != 0 || minor != 9)
            fail(fmt::format("unsupported protocol version {}.{}", static_cast<UInt32>(major), static_cast<UInt32>(minor)));
        reader.skipLongString(); /// server properties
        const String mechanisms = reader.readLongString();
        if (mechanisms.find("PLAIN") == String::npos)
            fail(fmt::format("the broker does not support the PLAIN authentication, its mechanisms are: {}", mechanisms));
    }

    {
        FieldTable capabilities;
        capabilities.addBool("publisher_confirms", true);
        capabilities.addBool("basic.nack", true);
        capabilities.addBool("consumer_cancel_notify", true);
        /// The broker closes the connection with an explanation instead of just dropping it on wrong credentials.
        capabilities.addBool("authentication_failure_close", true);

        FieldTable client_properties;
        client_properties.addString("product", "ClickHouse");
        client_properties.addTable("capabilities", capabilities);

        String response;
        response.push_back('\0');
        response.append(settings.address.user);
        response.push_back('\0');
        response.append(std::string_view(settings.address.password));

        String arguments;
        writeTable(arguments, client_properties);
        writeShortString(arguments, "PLAIN");
        writeLongString(arguments, response);
        writeShortString(arguments, "en_US");
        sendMethod(0, Class::CONNECTION, ConnectionMethod::START_OK, arguments);
    }

    {
        auto tune = waitForReply(0, Class::CONNECTION, ConnectionMethod::TUNE);
        Reader reader(tune.arguments);
        reader.readShort(); /// channel max
        const UInt32 proposed_frame_max = reader.readLong();
        heartbeat_interval = reader.readShort();

        /// 0 means no limit, but a frame must fit into memory.
        frame_max = std::clamp(proposed_frame_max, MIN_FRAME_SIZE, MAX_FRAME_SIZE);

        String arguments;
        writeShort(arguments, 1); /// channel max: one channel is used.
        writeLong(arguments, frame_max);
        writeShort(arguments, heartbeat_interval);
        sendMethod(0, Class::CONNECTION, ConnectionMethod::TUNE_OK, arguments);
    }

    {
        String arguments;
        writeShortString(arguments, settings.address.vhost);
        writeShortString(arguments, ""); /// reserved
        writeOctet(arguments, 0); /// reserved
        sendMethod(0, Class::CONNECTION, ConnectionMethod::OPEN, arguments);
        waitForReply(0, Class::CONNECTION, ConnectionMethod::OPEN_OK);
    }
}

void Connection::openChannel()
{
    String arguments;
    writeShortString(arguments, ""); /// reserved
    sendMethod(CHANNEL, Class::CHANNEL, ChannelMethod::OPEN, arguments);
    waitForReply(CHANNEL, Class::CHANNEL, ChannelMethod::OPEN_OK);
}

void Connection::close(Poco::Timespan timeout)
{
    if (error.empty())
    {
        try
        {
            String arguments;
            writeShort(arguments, 200);
            writeShortString(arguments, "");
            writeShort(arguments, 0);
            writeShort(arguments, 0);
            sendMethod(0, Class::CONNECTION, ConnectionMethod::CLOSE, arguments);
            out->next();

            /// Everything else that the broker sends before the reply is not needed anymore.
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(timeout.totalMicroseconds());
            while (true)
            {
                const auto remaining = deadline - std::chrono::steady_clock::now();
                if (remaining <= std::chrono::steady_clock::duration::zero())
                {
                    LOG_DEBUG(log, "Timeout exceeded while closing the connection to RabbitMQ {}", address_for_log);
                    break;
                }

                auto frame = receiveFrame(Poco::Timespan(std::chrono::duration_cast<std::chrono::microseconds>(remaining).count()));
                if (!frame || frame->type != FRAME_METHOD || frame->channel != 0 || frame->payload.size() < 4)
                    continue;

                const UInt16 class_id = unalignedLoadBigEndian<UInt16>(frame->payload.data());
                const UInt16 method_id = unalignedLoadBigEndian<UInt16>(frame->payload.data() + 2);
                if (class_id == Class::CONNECTION && method_id == ConnectionMethod::CLOSE_OK)
                    break;
                if (class_id == Class::CONNECTION && method_id == ConnectionMethod::CLOSE)
                {
                    /// The broker is closing the connection at the same time.
                    sendMethod(0, Class::CONNECTION, ConnectionMethod::CLOSE_OK, {});
                    out->next();
                    break;
                }
            }
        }
        catch (...)
        {
            LOG_DEBUG(log, "Error while closing the connection to RabbitMQ {}: {}", address_for_log, getCurrentExceptionMessage(false));
        }
        error = "the connection is closed";
    }

    out->cancel();
    try
    {
        socket->close();
    }
    catch (...)
    {
        LOG_DEBUG(log, "Error while closing the socket of RabbitMQ {}: {}", address_for_log, getCurrentExceptionMessage(false));
    }
}

void Connection::declareExchange(const String & name, ExchangeType type, bool durable, bool auto_delete, const FieldTable & arguments)
{
    String args;
    writeShort(args, 0); /// reserved
    writeShortString(args, name);
    writeShortString(args, exchangeTypeName(type));
    /// passive, durable, auto-delete, internal, no-wait
    writeOctet(args, static_cast<UInt8>((durable ? 2 : 0) | (auto_delete ? 4 : 0)));
    writeTable(args, arguments);
    call(Class::EXCHANGE, ExchangeMethod::DECLARE, args, ExchangeMethod::DECLARE_OK);
}

void Connection::deleteExchange(const String & name)
{
    String args;
    writeShort(args, 0); /// reserved
    writeShortString(args, name);
    writeOctet(args, 0); /// if-unused, no-wait
    call(Class::EXCHANGE, ExchangeMethod::DELETE, args, ExchangeMethod::DELETE_OK);
}

void Connection::bindExchange(const String & source, const String & destination, const String & routing_key, const FieldTable & arguments)
{
    String args;
    writeShort(args, 0); /// reserved
    writeShortString(args, destination);
    writeShortString(args, source);
    writeShortString(args, routing_key);
    writeOctet(args, 0); /// no-wait
    writeTable(args, arguments);
    call(Class::EXCHANGE, ExchangeMethod::BIND, args, ExchangeMethod::BIND_OK);
}

Connection::QueueInfo Connection::declareQueue(const String & name, bool durable, const FieldTable & arguments)
{
    String args;
    writeShort(args, 0); /// reserved
    writeShortString(args, name);
    /// passive, durable, exclusive, auto-delete, no-wait
    writeOctet(args, durable ? 2 : 0);
    writeTable(args, arguments);
    auto reply = call(Class::QUEUE, QueueMethod::DECLARE, args, QueueMethod::DECLARE_OK);

    Reader reader(reply.arguments);
    QueueInfo info;
    info.name = reader.readShortString();
    info.message_count = reader.readLong();
    info.consumer_count = reader.readLong();
    return info;
}

void Connection::bindQueue(const String & queue, const String & exchange, const String & routing_key)
{
    String args;
    writeShort(args, 0); /// reserved
    writeShortString(args, queue);
    writeShortString(args, exchange);
    writeShortString(args, routing_key);
    writeOctet(args, 0); /// no-wait
    writeTable(args, {});
    call(Class::QUEUE, QueueMethod::BIND, args, QueueMethod::BIND_OK);
}

UInt32 Connection::deleteQueue(const String & name, bool if_unused)
{
    String args;
    writeShort(args, 0); /// reserved
    writeShortString(args, name);
    /// if-unused, if-empty, no-wait
    writeOctet(args, if_unused ? 1 : 0);
    auto reply = call(Class::QUEUE, QueueMethod::DELETE, args, QueueMethod::DELETE_OK);
    return Reader(reply.arguments).readLong();
}

String Connection::consume(const String & queue)
{
    String args;
    writeShort(args, 0); /// reserved
    writeShortString(args, queue);
    writeShortString(args, ""); /// consumer tag, generated by the broker
    writeOctet(args, 0); /// no-local, no-ack, exclusive, no-wait
    writeTable(args, {});
    auto reply = call(Class::BASIC, BasicMethod::CONSUME, args, BasicMethod::CONSUME_OK);
    return Reader(reply.arguments).readShortString();
}

void Connection::ack(UInt64 delivery_tag, bool multiple)
{
    checkUsable();
    try
    {
        String args;
        writeLongLong(args, delivery_tag);
        writeOctet(args, multiple ? 1 : 0);
        sendMethod(CHANNEL, Class::BASIC, BasicMethod::ACK, args);
        out->next();
    }
    catch (...)
    {
        if (error.empty())
            error = getCurrentExceptionMessage(false);
        throw;
    }
}

void Connection::nack(UInt64 delivery_tag, bool multiple, bool requeue)
{
    checkUsable();
    try
    {
        String args;
        writeLongLong(args, delivery_tag);
        writeOctet(args, static_cast<UInt8>((multiple ? 1 : 0) | (requeue ? 2 : 0)));
        sendMethod(CHANNEL, Class::BASIC, BasicMethod::NACK, args);
        out->next();
    }
    catch (...)
    {
        if (error.empty())
            error = getCurrentExceptionMessage(false);
        throw;
    }
}

void Connection::reject(UInt64 delivery_tag, bool requeue)
{
    checkUsable();
    try
    {
        String args;
        writeLongLong(args, delivery_tag);
        writeOctet(args, requeue ? 1 : 0);
        sendMethod(CHANNEL, Class::BASIC, BasicMethod::REJECT, args);
        out->next();
    }
    catch (...)
    {
        if (error.empty())
            error = getCurrentExceptionMessage(false);
        throw;
    }
}

std::optional<Delivery> Connection::receiveDelivery(Poco::Timespan timeout)
{
    if (deliveries.empty())
    {
        checkUsable();
        try
        {
            processIncoming(timeout);
        }
        catch (...)
        {
            if (error.empty())
                error = getCurrentExceptionMessage(false);
            throw;
        }
    }

    if (deliveries.empty())
        return {};

    auto delivery = std::move(deliveries.front());
    deliveries.pop_front();
    return delivery;
}

void Connection::confirmSelect()
{
    String args;
    writeOctet(args, 0); /// no-wait
    call(Class::CONFIRM, ConfirmMethod::SELECT, args, ConfirmMethod::SELECT_OK);
}

void Connection::publish(const String & exchange, const String & routing_key, std::string_view body, const MessageProperties & properties)
{
    checkUsable();
    try
    {
        String args;
        writeShort(args, 0); /// reserved
        writeShortString(args, exchange);
        writeShortString(args, routing_key);
        writeOctet(args, 0); /// mandatory, immediate
        sendMethod(CHANNEL, Class::BASIC, BasicMethod::PUBLISH, args);

        UInt16 flags = 0;
        if (!properties.headers.empty())
            flags |= Property::HEADERS;
        if (properties.delivery_mode)
            flags |= Property::DELIVERY_MODE;
        if (!properties.message_id.empty())
            flags |= Property::MESSAGE_ID;

        String header;
        writeShort(header, Class::BASIC);
        writeShort(header, 0); /// weight
        writeLongLong(header, body.size());
        writeShort(header, flags);
        /// In the order of the flags.
        if (flags & Property::HEADERS)
            writeTable(header, properties.headers);
        if (flags & Property::DELIVERY_MODE)
            writeOctet(header, properties.delivery_mode);
        if (flags & Property::MESSAGE_ID)
            writeShortString(header, properties.message_id);
        sendFrame(FRAME_HEADER, CHANNEL, header);

        const size_t max_body_frame = frame_max - FRAME_OVERHEAD;
        for (size_t offset = 0; offset < body.size(); offset += max_body_frame)
            sendFrame(FRAME_BODY, CHANNEL, body.substr(offset, max_body_frame));
    }
    catch (...)
    {
        if (error.empty())
            error = getCurrentExceptionMessage(false);
        throw;
    }
}

void Connection::flush()
{
    checkUsable();
    try
    {
        out->next();
    }
    catch (...)
    {
        if (error.empty())
            error = getCurrentExceptionMessage(false);
        throw;
    }
}

VectorWithMemoryTracking<Confirm> Connection::receiveConfirms(Poco::Timespan timeout)
{
    if (confirms.empty())
    {
        checkUsable();
        try
        {
            processIncoming(timeout);
        }
        catch (...)
        {
            if (error.empty())
                error = getCurrentExceptionMessage(false);
            throw;
        }
    }

    return std::exchange(confirms, {});
}

}

}
