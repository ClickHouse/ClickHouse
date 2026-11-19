#include <Server/StatelessWorker/StatelessWorkerControlProtocol.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Poco/Net/StreamSocket.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LIMIT_EXCEEDED;
    extern const int PROTOCOL_VERSION_MISMATCH;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

namespace StatelessWorkerControlProtocol
{

namespace
{

constexpr UInt64 MAX_HELLO_BODY_BYTES = 64 * 1024;
constexpr UInt64 MAX_MESSAGE_BODY_BYTES = 64 * 1024;
constexpr UInt64 MAX_TASK_IDS_BODY_BYTES = 16 * 1024 * 1024;
constexpr UInt64 MAX_TASKS_BODY_BYTES = 1024 * 1024 * 1024;
constexpr UInt64 MAX_STATUSES_BODY_BYTES = 1024 * 1024 * 1024;
constexpr UInt64 MAX_REQUEST_ID_BODY_BYTES = 64;

/// Longer messages are cut, so that a reply carrying one stays within its cap.
constexpr size_t MAX_MESSAGE_BYTES = 16 * 1024;

constexpr auto LINGER_TIMEOUT = std::chrono::seconds(1);

Sender senderOf(PacketType type)
{
    switch (type)
    {
        case PacketType::ControlHello:
        case PacketType::StartTasks:
        case PacketType::GetStatuses:
        case PacketType::CancelTasks:
        case PacketType::ForgetTasks:
        case PacketType::Ping:
            return Sender::Initiator;
        case PacketType::ControlHelloReply:
        case PacketType::StartTasksReply:
        case PacketType::GetStatusesReply:
        case PacketType::CancelTasksReply:
        case PacketType::ForgetTasksReply:
        case PacketType::Pong:
        case PacketType::Error:
            return Sender::Worker;
    }
    UNREACHABLE();
}

String senderName(Sender sender)
{
    return sender == Sender::Initiator ? "initiator" : "worker";
}

int unknownPacketCode(Sender sender)
{
    return sender == Sender::Initiator ? ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT : ErrorCodes::UNKNOWN_PACKET_FROM_SERVER;
}

int unexpectedPacketCode(Sender sender)
{
    return sender == Sender::Initiator ? ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT : ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER;
}

UInt64 readVersion(ReadBuffer & in, Sender sender)
{
    UInt64 version = 0;
    readVarUInt(version, in);
    if (version != PROTOCOL_VERSION)
        throw Exception(ErrorCodes::PROTOCOL_VERSION_MISMATCH,
            "Stateless worker control protocol version mismatch: the {} speaks version {}, this node speaks version {}",
            senderName(sender), version, PROTOCOL_VERSION);
    return version;
}

void writeMessage(const String & message, WriteBuffer & out)
{
    writeStringBinary(std::string_view(message).substr(0, MAX_MESSAGE_BYTES), out);
}

}

bool isControlPacketType(UInt64 packet_type)
{
    return packet_type >= static_cast<UInt64>(PacketType::ControlHello) && packet_type <= static_cast<UInt64>(PacketType::Error);
}

String packetTypeName(UInt64 packet_type)
{
    if (!isControlPacketType(packet_type))
        return fmt::format("0x{:x}", packet_type);
    switch (static_cast<PacketType>(packet_type))
    {
        case PacketType::ControlHello: return "ControlHello";
        case PacketType::ControlHelloReply: return "ControlHelloReply";
        case PacketType::StartTasks: return "StartTasks";
        case PacketType::StartTasksReply: return "StartTasksReply";
        case PacketType::GetStatuses: return "GetStatuses";
        case PacketType::GetStatusesReply: return "GetStatusesReply";
        case PacketType::CancelTasks: return "CancelTasks";
        case PacketType::CancelTasksReply: return "CancelTasksReply";
        case PacketType::ForgetTasks: return "ForgetTasks";
        case PacketType::ForgetTasksReply: return "ForgetTasksReply";
        case PacketType::Ping: return "Ping";
        case PacketType::Pong: return "Pong";
        case PacketType::Error: return "Error";
    }
    UNREACHABLE();
}

UInt64 maxBodyBytes(PacketType type)
{
    switch (type)
    {
        case PacketType::ControlHello:
        case PacketType::ControlHelloReply:
            return MAX_HELLO_BODY_BYTES;
        case PacketType::StartTasks:
            return MAX_TASKS_BODY_BYTES;
        case PacketType::GetStatusesReply:
            return MAX_STATUSES_BODY_BYTES;
        case PacketType::GetStatuses:
        case PacketType::CancelTasks:
        case PacketType::ForgetTasks:
            return MAX_TASK_IDS_BODY_BYTES;
        case PacketType::StartTasksReply:
        case PacketType::Error:
            return MAX_MESSAGE_BODY_BYTES;
        case PacketType::CancelTasksReply:
        case PacketType::ForgetTasksReply:
        case PacketType::Ping:
        case PacketType::Pong:
            return MAX_REQUEST_ID_BODY_BYTES;
    }
    UNREACHABLE();
}

void checkHeader(const PacketHeader & header, Sender sender)
{
    if (!isControlPacketType(header.packet_type))
        throw Exception(unknownPacketCode(sender), "Unknown stateless worker control packet type {} from the {}",
            packetTypeName(header.packet_type), senderName(sender));

    const auto type = static_cast<PacketType>(header.packet_type);
    if (senderOf(type) != sender)
        throw Exception(unexpectedPacketCode(sender), "Unexpected stateless worker control packet {} from the {}",
            packetTypeName(header.packet_type), senderName(sender));

    if (header.bytes_size > maxBodyBytes(type))
        throw Exception(ErrorCodes::INCORRECT_DATA, "The body of stateless worker control packet {} from the {} has {} bytes, over the limit of {}",
            packetTypeName(header.packet_type), senderName(sender), header.bytes_size, maxBodyBytes(type));
}

PacketHeader readHeader(ReadBuffer & in, Sender sender)
{
    PacketHeader header{};
    in.readStrict(reinterpret_cast<char *>(&header), sizeof(header));
    checkHeader(header, sender);
    return header;
}

PacketHeader makeHeader(PacketType type, UInt64 body_size)
{
    if (body_size > maxBodyBytes(type))
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "The body of stateless worker control packet {} has {} bytes, over the limit of {}",
            packetTypeName(static_cast<UInt64>(type)), body_size, maxBodyBytes(type));
    return PacketHeader{.packet_type = static_cast<UInt64>(type), .bytes_size = body_size};
}

void readBody(const PacketHeader & header, PacketType expected_type, ReadBuffer & in, const std::function<void(ReadBuffer &)> & read)
{
    if (header.packet_type != static_cast<UInt64>(expected_type))
        throw Exception(unexpectedPacketCode(senderOf(expected_type)), "Expected stateless worker control packet {}, got {}",
            packetTypeName(static_cast<UInt64>(expected_type)), packetTypeName(header.packet_type));

    LimitReadBuffer body(in, {.read_no_less = header.bytes_size, .read_no_more = header.bytes_size});
    read(body);
    if (!body.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected trailing data in stateless worker control packet {}",
            packetTypeName(header.packet_type));
}

void parseBody(const PacketHeader & header, PacketType expected_type, std::string_view body, const std::function<void(ReadBuffer &)> & read)
{
    if (body.size() != header.bytes_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The body of stateless worker control packet {} has {} bytes, its header says {}",
            packetTypeName(header.packet_type), body.size(), header.bytes_size);
    ReadBufferFromMemory in(body.data(), body.size());
    readBody(header, expected_type, in, read);
}

void writeFrame(PacketType type, std::string_view body, WriteBuffer & out)
{
    const PacketHeader header = makeHeader(type, body.size());
    out.write(reinterpret_cast<const char *>(&header), sizeof(header));
    out.write(body.data(), body.size());
}

void lingeringClose(Poco::Net::StreamSocket & socket)
{
    try
    {
        socket.shutdownSend();
        const auto deadline = std::chrono::steady_clock::now() + LINGER_TIMEOUT;
        char discarded[4096];
        while (true)
        {
            const auto left = std::chrono::duration_cast<std::chrono::microseconds>(deadline - std::chrono::steady_clock::now());
            if (left.count() <= 0 || !socket.poll(Poco::Timespan(left.count()), Poco::Net::Socket::SELECT_READ))
                break;
            if (socket.receiveBytes(discarded, sizeof(discarded)) <= 0)
                break;
        }
    }
    catch (const Poco::Exception &) // NOLINT(bugprone-empty-catch)
    {
        /// The peer reset the connection, so there is nothing left to wait for.
    }
    socket.close();
}

void ControlHello::write(WriteBuffer & out) const
{
    writeVarUInt(version, out);
    writeVarUInt(static_cast<UInt64>(purpose), out);
    writeStringBinary(unique_query_id, out);
    writeStringBinary(auth_token, out);
    writeStringBinary(user, out);
    writeStringBinary(password, out);
    writeStringBinary(initiator, out);
}

void ControlHello::read(ReadBuffer & in)
{
    version = readVersion(in, Sender::Initiator);
    UInt64 purpose_value = 0;
    readVarUInt(purpose_value, in);
    if (purpose_value != static_cast<UInt64>(Purpose::Query) && purpose_value != static_cast<UInt64>(Purpose::Probe))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown purpose {} of a stateless worker control connection", purpose_value);
    purpose = static_cast<Purpose>(purpose_value);
    readStringBinary(unique_query_id, in);
    readStringBinary(auth_token, in);
    readStringBinary(user, in);
    readStringBinary(password, in);
    readStringBinary(initiator, in);
}

void ControlHelloReply::write(WriteBuffer & out) const
{
    writeVarUInt(version, out);
    writeIntBinary(code, out);
    writeMessage(message, out);
    writeVarUInt(task_serialization_version, out);
    writeVarUInt(plan_serialization_version, out);
}

void ControlHelloReply::read(ReadBuffer & in)
{
    version = readVersion(in, Sender::Worker);
    readIntBinary(code, in);
    readStringBinary(message, in);
    readVarUInt(task_serialization_version, in);
    readVarUInt(plan_serialization_version, in);
}

namespace
{

void writeStartTasksFields(const StartTasks & request, WriteBuffer & out)
{
    writeVarUInt(request.request_id, out);
    writeStringBinary(request.temp_path, out);
    writeStringBinary(request.collectors.toString(), out);
    request.trace_context.serialize(out);
}

}

void StartTasks::write(WriteBuffer & out) const
{
    writeStartTasksFields(*this, out);
    serializeTaskBatch(batch, out);
}

void StartTasks::write(WriteBuffer & out, const TaskBatchPlanWriter & write_plan) const
{
    writeStartTasksFields(*this, out);
    serializeTaskBatch(batch, out, write_plan);
}

void StartTasks::read(ReadBuffer & in)
{
    readVarUInt(request_id, in);
    readStringBinary(temp_path, in);
    String collectors_value;
    readStringBinary(collectors_value, in);
    collectors = TaskCollectors::parse(collectors_value);
    trace_context.deserialize(in);
    deserializeTaskBatch(batch, in);
}

void StartTasksReply::write(WriteBuffer & out) const
{
    writeVarUInt(request_id, out);
    writeIntBinary(code, out);
    writeMessage(message, out);
}

void StartTasksReply::read(ReadBuffer & in)
{
    readVarUInt(request_id, in);
    readIntBinary(code, in);
    readStringBinary(message, in);
}

void GetStatuses::write(WriteBuffer & out) const
{
    writeVarUInt(request_id, out);
    writeVarUInt(wait_milliseconds, out);
    writeTaskIds(task_ids, out);
}

void GetStatuses::read(ReadBuffer & in)
{
    readVarUInt(request_id, in);
    readVarUInt(wait_milliseconds, in);
    task_ids = readTaskIds(in);
}

void GetStatusesReply::write(WriteBuffer & out) const
{
    writeVarUInt(request_id, out);
    writeTaskStatuses(entries, out);
}

void GetStatusesReply::read(ReadBuffer & in)
{
    readVarUInt(request_id, in);
    entries = readTaskStatuses(in);
}

void Error::write(WriteBuffer & out) const
{
    writeVarUInt(request_id, out);
    writeIntBinary(code, out);
    writeMessage(message, out);
}

void Error::read(ReadBuffer & in)
{
    readVarUInt(request_id, in);
    readIntBinary(code, in);
    readStringBinary(message, in);
}

}

}
