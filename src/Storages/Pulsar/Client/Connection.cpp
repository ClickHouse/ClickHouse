#include <Storages/Pulsar/Client/Connection.h>

#include <Storages/Pulsar/Client/Checksum.h>
#include <Common/logger_useful.h>

#include <chrono>
#include <cstring>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace DB::Pulsar
{

namespace
{

/// Magic numbers that may precede the message metadata inside a frame (see the Pulsar binary protocol).
constexpr uint16_t MAGIC_CRC32C = 0x0e01;
constexpr uint16_t MAGIC_BROKER_ENTRY_METADATA = 0x0e02;

/// A hard ceiling on a single frame, independent of the negotiated message size, so a corrupt length
/// prefix cannot make the client allocate an unbounded buffer.
constexpr uint32_t MAX_FRAME_SIZE = 128 * 1024 * 1024;

int64_t nowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

void putBE16(std::string & out, uint16_t value)
{
    out.push_back(static_cast<char>((value >> 8) & 0xFF));
    out.push_back(static_cast<char>(value & 0xFF));
}

void putBE32(std::string & out, uint32_t value)
{
    out.push_back(static_cast<char>((value >> 24) & 0xFF));
    out.push_back(static_cast<char>((value >> 16) & 0xFF));
    out.push_back(static_cast<char>((value >> 8) & 0xFF));
    out.push_back(static_cast<char>(value & 0xFF));
}

uint16_t getBE16(const char * p)
{
    return static_cast<uint16_t>((static_cast<unsigned char>(p[0]) << 8) | static_cast<unsigned char>(p[1]));
}

uint32_t getBE32(const char * p)
{
    return (static_cast<uint32_t>(static_cast<unsigned char>(p[0])) << 24)
        | (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 8)
        | static_cast<uint32_t>(static_cast<unsigned char>(p[3]));
}

/// A bounds-checked cursor over a received buffer. Every read verifies there are enough bytes left, so
/// a malformed frame can never read out of bounds; `ok` turns false on the first short read.
struct Cursor
{
    const char * data;
    size_t size;
    size_t offset = 0;
    bool ok = true;

    bool has(size_t n) const { return offset + n <= size; }

    uint16_t u16()
    {
        if (!has(2)) { ok = false; return 0; }
        uint16_t v = getBE16(data + offset);
        offset += 2;
        return v;
    }

    uint32_t u32()
    {
        if (!has(4)) { ok = false; return 0; }
        uint32_t v = getBE32(data + offset);
        offset += 4;
        return v;
    }

    std::string_view view(size_t n)
    {
        if (!has(n)) { ok = false; return {}; }
        std::string_view v(data + offset, n);
        offset += n;
        return v;
    }

    std::string_view rest()
    {
        std::string_view v(data + offset, size - offset);
        offset = size;
        return v;
    }
};

/// The `request_id` of a response command, or -1 if the command type does not carry one.
int64_t responseRequestId(const pulsar::proto::BaseCommand & command)
{
    using BaseCommand = pulsar::proto::BaseCommand;
    switch (command.type())
    {
        case BaseCommand::SUCCESS:
            return static_cast<int64_t>(command.success().request_id());
        case BaseCommand::ERROR:
            return static_cast<int64_t>(command.error().request_id());
        case BaseCommand::PRODUCER_SUCCESS:
            return static_cast<int64_t>(command.producer_success().request_id());
        case BaseCommand::LOOKUP_RESPONSE:
            return static_cast<int64_t>(command.lookuptopicresponse().request_id());
        case BaseCommand::PARTITIONED_METADATA_RESPONSE:
            return static_cast<int64_t>(command.partitionmetadataresponse().request_id());
        default:
            return -1;
    }
}

}

Connection::Connection(int fd_, const ClientConfiguration & config_, LoggerPtr log_)
    : fd(fd_), config(config_), log(std::move(log_))
{
}

Connection::~Connection()
{
    close();
    if (fd >= 0)
        ::close(fd);
}

std::unique_ptr<Connection>
Connection::open(const std::string & host, uint16_t port, const ClientConfiguration & config, const LoggerPtr & log, Result & result)
{
    const std::string physical_url = "pulsar://" + host + ":" + std::to_string(port);
    if (config.connection_validator && !config.connection_validator(physical_url))
    {
        LOG_ERROR(log, "Connection to {} rejected by the connection validator", physical_url);
        result = ResultConnectError;
        return nullptr;
    }

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo * addresses = nullptr;
    if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &addresses) != 0 || !addresses)
    {
        LOG_ERROR(log, "Cannot resolve Pulsar broker address {}", physical_url);
        result = ResultConnectError;
        return nullptr;
    }

    int fd = -1;
    const int64_t deadline = nowMs() + config.operation_timeout_ms;
    for (addrinfo * ai = addresses; ai != nullptr; ai = ai->ai_next)
    {
        fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);
        if (fd < 0)
            continue;

        /// Connect with a timeout: switch to non-blocking, start the connect, then wait for writability.
        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        int rc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (rc != 0 && errno == EINPROGRESS)
        {
            pollfd pfd{fd, POLLOUT, 0};
            const int64_t remaining = deadline - nowMs();
            rc = ::poll(&pfd, 1, remaining > 0 ? static_cast<int>(remaining) : 0);
            if (rc == 1)
            {
                int error = 0;
                socklen_t len = sizeof(error);
                ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len);
                rc = error == 0 ? 0 : -1;
            }
            else
                rc = -1;
        }

        if (rc == 0)
        {
            ::fcntl(fd, F_SETFL, flags); /// back to blocking
            int one = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            break;
        }

        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(addresses);

    if (fd < 0)
    {
        LOG_ERROR(log, "Cannot connect to Pulsar broker {}", physical_url);
        result = ResultConnectError;
        return nullptr;
    }

    std::unique_ptr<Connection> connection(new Connection(fd, config, log));

    /// Handshake: CONNECT -> CONNECTED.
    pulsar::proto::BaseCommand connect_command;
    connect_command.set_type(pulsar::proto::BaseCommand::CONNECT);
    auto * connect = connect_command.mutable_connect();
    connect->set_client_version("ClickHouse");
    connect->set_protocol_version(21);
    if (Result send_result = connection->send(connect_command); send_result != ResultOk)
    {
        result = send_result;
        return nullptr;
    }

    Incoming incoming;
    if (Result receive_result = connection->receive(incoming, config.operation_timeout_ms); receive_result != ResultOk)
    {
        LOG_ERROR(log, "No CONNECTED response from Pulsar broker {}", physical_url);
        result = receive_result;
        return nullptr;
    }
    if (incoming.command.type() != pulsar::proto::BaseCommand::CONNECTED)
    {
        LOG_ERROR(log, "Unexpected handshake response from Pulsar broker {}: {}", physical_url, static_cast<int>(incoming.command.type()));
        result = ResultConnectError;
        return nullptr;
    }
    if (incoming.command.connected().has_max_message_size())
        connection->max_message_size = incoming.command.connected().max_message_size();

    LOG_TRACE(log, "Connected to Pulsar broker {}", physical_url);
    result = ResultOk;
    return connection;
}

void Connection::close()
{
    if (!closed.exchange(true, std::memory_order_acq_rel))
    {
        /// Unblock a read in progress on another thread without closing the descriptor, which the
        /// destructor does once no one uses it any more.
        if (fd >= 0)
            ::shutdown(fd, SHUT_RDWR);
    }
}

Result Connection::writeAll(const char * data, size_t size)
{
    size_t written = 0;
    while (written < size)
    {
        if (isClosed())
            return ResultAlreadyClosed;
        ssize_t n = ::send(fd, data + written, size - written, MSG_NOSIGNAL);
        if (n > 0)
        {
            written += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK))
            continue;
        return ResultConnectError;
    }
    return ResultOk;
}

Result Connection::readAll(char * buffer, size_t size, int64_t deadline_ms)
{
    size_t read_bytes = 0;
    while (read_bytes < size)
    {
        if (isClosed())
            return ResultAlreadyClosed;

        const int64_t remaining = deadline_ms - nowMs();
        if (remaining <= 0)
            return read_bytes == 0 ? ResultTimeout : ResultConnectError;

        pollfd pfd{fd, POLLIN, 0};
        const int slice = static_cast<int>(std::min<int64_t>(remaining, 1000));
        int rc = ::poll(&pfd, 1, slice);
        if (rc == 0)
            continue; /// nothing yet; re-check the deadline and the closed flag
        if (rc < 0)
        {
            if (errno == EINTR)
                continue;
            return ResultConnectError;
        }

        ssize_t n = ::recv(fd, buffer + read_bytes, size - read_bytes, 0);
        if (n > 0)
        {
            read_bytes += static_cast<size_t>(n);
            continue;
        }
        if (n == 0)
            return ResultAlreadyClosed; /// peer closed
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
            continue;
        return ResultConnectError;
    }
    return ResultOk;
}

Result Connection::readFrame(std::string & frame, int64_t deadline_ms)
{
    char length_prefix[4];
    if (Result r = readAll(length_prefix, 4, deadline_ms); r != ResultOk)
        return r;

    const uint32_t total_size = getBE32(length_prefix);
    if (total_size < 4 || total_size > MAX_FRAME_SIZE)
    {
        LOG_ERROR(log, "Received a Pulsar frame of invalid size {}", total_size);
        return ResultError;
    }

    frame.resize(total_size);
    /// Once a frame has started the rest should follow promptly; allow a bounded grace period.
    const int64_t body_deadline = std::max<int64_t>(deadline_ms, nowMs() + config.operation_timeout_ms);
    if (Result r = readAll(frame.data(), total_size, body_deadline); r != ResultOk)
        return r == ResultTimeout ? ResultConnectError : r;

    return ResultOk;
}

Result Connection::receive(Incoming & incoming, int timeout_ms)
{
    const int64_t deadline = nowMs() + timeout_ms;
    while (true)
    {
        std::string frame;
        if (Result r = readFrame(frame, deadline); r != ResultOk)
            return r;

        Cursor cursor{frame.data(), frame.size()};
        const uint32_t command_size = cursor.u32();
        std::string_view command_bytes = cursor.view(command_size);
        if (!cursor.ok || !incoming.command.ParseFromArray(command_bytes.data(), static_cast<int>(command_bytes.size())))
        {
            LOG_ERROR(log, "Cannot parse a Pulsar command from a frame of {} bytes", frame.size());
            return ResultError;
        }

        const auto type = incoming.command.type();

        if (type == pulsar::proto::BaseCommand::PING)
        {
            pulsar::proto::BaseCommand pong;
            pong.set_type(pulsar::proto::BaseCommand::PONG);
            pong.mutable_pong();
            send(pong);
            continue;
        }
        if (type == pulsar::proto::BaseCommand::PONG)
            continue;

        incoming.is_message = type == pulsar::proto::BaseCommand::MESSAGE;
        incoming.metadata.clear();
        incoming.payload.clear();

        if (incoming.is_message)
        {
            /// Layout after the command: [optional broker-entry-metadata][optional CRC32C][metadata][payload].
            if (cursor.has(2) && getBE16(cursor.data + cursor.offset) == MAGIC_BROKER_ENTRY_METADATA)
            {
                cursor.u16();
                const uint32_t broker_meta_size = cursor.u32();
                cursor.view(broker_meta_size); /// skip it
            }

            bool verify_crc = false;
            uint32_t expected_crc = 0;
            if (cursor.has(2) && getBE16(cursor.data + cursor.offset) == MAGIC_CRC32C)
            {
                cursor.u16();
                expected_crc = cursor.u32();
                verify_crc = true;
            }

            const size_t checksummed_offset = cursor.offset;
            const uint32_t metadata_size = cursor.u32();
            std::string_view metadata = cursor.view(metadata_size);
            std::string_view payload = cursor.rest();

            if (!cursor.ok)
            {
                LOG_ERROR(log, "Received a malformed Pulsar message frame");
                return ResultError;
            }

            if (verify_crc)
            {
                const uint32_t actual_crc = crc32c(frame.data() + checksummed_offset, frame.size() - checksummed_offset);
                if (actual_crc != expected_crc)
                {
                    LOG_WARNING(log, "Dropping a Pulsar message with a bad checksum");
                    continue; /// leave it unacknowledged; the broker will redeliver it
                }
            }

            incoming.metadata.assign(metadata);
            incoming.payload.assign(payload);
        }

        return ResultOk;
    }
}

Result Connection::send(const pulsar::proto::BaseCommand & command)
{
    const std::string command_bytes = command.SerializeAsString();

    std::string frame;
    frame.reserve(8 + command_bytes.size());
    putBE32(frame, static_cast<uint32_t>(4 + command_bytes.size())); /// total size
    putBE32(frame, static_cast<uint32_t>(command_bytes.size())); /// command size
    frame += command_bytes;

    return writeAll(frame.data(), frame.size());
}

Result Connection::sendMessage(const pulsar::proto::BaseCommand & command, const std::string & metadata, const std::string & payload)
{
    const std::string command_bytes = command.SerializeAsString();

    /// The region protected by the checksum: [metadataSize][metadata][payload].
    std::string checksummed;
    checksummed.reserve(4 + metadata.size() + payload.size());
    putBE32(checksummed, static_cast<uint32_t>(metadata.size()));
    checksummed += metadata;
    checksummed += payload;

    const uint32_t checksum = crc32c(checksummed.data(), checksummed.size());

    std::string body;
    body.reserve(4 + command_bytes.size() + 2 + 4 + checksummed.size());
    putBE32(body, static_cast<uint32_t>(command_bytes.size()));
    body += command_bytes;
    putBE16(body, MAGIC_CRC32C);
    putBE32(body, checksum);
    body += checksummed;

    std::string frame;
    frame.reserve(4 + body.size());
    putBE32(frame, static_cast<uint32_t>(body.size()));
    frame += body;

    return writeAll(frame.data(), frame.size());
}

Result Connection::request(const pulsar::proto::BaseCommand & command, uint64_t request_id, pulsar::proto::BaseCommand & response)
{
    if (Result r = send(command); r != ResultOk)
        return r;

    const int64_t deadline = nowMs() + config.operation_timeout_ms;
    while (true)
    {
        const int64_t remaining = deadline - nowMs();
        if (remaining <= 0)
            return ResultTimeout;

        Incoming incoming;
        if (Result r = receive(incoming, static_cast<int>(remaining)); r != ResultOk)
            return r;

        if (responseRequestId(incoming.command) == static_cast<int64_t>(request_id))
        {
            response = incoming.command;
            return ResultOk;
        }
        /// A dedicated connection never multiplexes requests, so anything else is unexpected; ignore it.
    }
}

}
