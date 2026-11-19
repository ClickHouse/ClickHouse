#pragma once

#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <Server/StatelessWorker/StatelessWorkerTaskSerialization.h>
#include <Common/OpenTelemetryTracingContext.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <base/types.h>

#include <functional>
#include <string_view>
#include <vector>

namespace DB
{

/// The control of the tasks of one distributed query on one stateless worker, on the worker's streaming-exchange port.
///
/// A frame is a `StreamingExchangeProtocol::PacketHeader` (type and body size, host byte order), then the body; each
/// type caps its body size. The initiator opens with `ControlHello`, which binds the connection to one query, and sends
/// nothing else until the `ControlHelloReply`. Every later body starts with a `varUInt` request id, increasing per
/// connection; a reply carries the id of its request, and id 0 stands for the connection.
///
///   ControlHello       I->W  varUInt version, varUInt purpose, unique_query_id, auth_token, user, password, initiator
///   ControlHelloReply  W->I  varUInt version, Int32 code (0: accepted), message, varUInt task and plan serialization versions
///   StartTasks         I->W  id, temp_path, `TaskCollectors::toString`, `TracingContext::serialize`, the `serializeTaskBatch` bytes
///   StartTasksReply    W->I  id, Int32 code (0: started), message
///   GetStatuses        I->W  id, varUInt wait_milliseconds, `writeTaskIds`
///   GetStatusesReply   W->I  id, `writeTaskStatuses`
///   CancelTasks        I->W  id, `writeTaskIds`; answered by CancelTasksReply: id
///   ForgetTasks        I->W  id, `writeTaskIds`; answered by ForgetTasksReply: id
///   Ping, Pong         I->W, W->I  id
///   Error              W->I  id, Int32 code, message; with id 0 the worker closes the connection
///
/// Strings are written by `writeStringBinary`. The versions must match exactly: on a mismatch the worker still replies
/// with its version, and reading either hello throws naming both.
namespace StatelessWorkerControlProtocol
{

static constexpr UInt64 PROTOCOL_VERSION = 1;

/// Apart from every `StreamingExchangeProtocol::PacketType`, so an exchange peer and a control peer reject each other's
/// first frame.
enum class PacketType : UInt64
{
    ControlHello = 0x5c000001,
    ControlHelloReply = 0x5c000002,
    StartTasks = 0x5c000003,
    StartTasksReply = 0x5c000004,
    GetStatuses = 0x5c000005,
    GetStatusesReply = 0x5c000006,
    CancelTasks = 0x5c000007,
    CancelTasksReply = 0x5c000008,
    ForgetTasks = 0x5c000009,
    ForgetTasksReply = 0x5c00000a,
    Ping = 0x5c00000b,
    Pong = 0x5c00000c,
    Error = 0x5c00000d,
};

enum class Sender : uint8_t
{
    Initiator,
    Worker,
};

using PacketHeader = StreamingExchangeProtocol::PacketHeader;

bool isControlPacketType(UInt64 packet_type);
/// Returns the name of a control packet type, the number for any other.
String packetTypeName(UInt64 packet_type);
UInt64 maxBodyBytes(PacketType type);

/// Throws for a type `sender` does not send or a body over the cap of its type.
void checkHeader(const PacketHeader & header, Sender sender);
PacketHeader readHeader(ReadBuffer & in, Sender sender);
/// Throws `LIMIT_EXCEEDED` for a body over the cap of `type`.
PacketHeader makeHeader(PacketType type, UInt64 body_size);

/// Reads the body that follows `header` with `read`, which must consume it exactly.
void readBody(const PacketHeader & header, PacketType expected_type, ReadBuffer & in, const std::function<void(ReadBuffer &)> & read);
/// Parses a body received completely after `header`.
void parseBody(const PacketHeader & header, PacketType expected_type, std::string_view body, const std::function<void(ReadBuffer &)> & read);
/// Writes the header and the body; the caller flushes `out`.
void writeFrame(PacketType type, std::string_view body, WriteBuffer & out);

template <typename Message>
void readBody(const PacketHeader & header, ReadBuffer & in, Message & message)
{
    readBody(header, Message::type, in, [&message](ReadBuffer & body) { message.read(body); });
}

template <typename Message>
Message parseBody(const PacketHeader & header, std::string_view body)
{
    Message message;
    parseBody(header, Message::type, body, [&message](ReadBuffer & in) { message.read(in); });
    return message;
}

template <typename Message>
void writeFrame(const Message & message, WriteBuffer & out)
{
    WriteBufferFromOwnString body;
    message.write(body);
    body.finalize();
    writeFrame(Message::type, body.stringView(), out);
}

enum class Purpose : UInt64
{
    Query = 0,
    Probe = 1,
};

struct ControlHello
{
    static constexpr PacketType type = PacketType::ControlHello;

    UInt64 version = PROTOCOL_VERSION;
    Purpose purpose = Purpose::Query;
    String unique_query_id;
    String auth_token;
    String user;
    String password;
    /// The initiator's address, for messages.
    String initiator;

    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);
};

struct ControlHelloReply
{
    static constexpr PacketType type = PacketType::ControlHelloReply;

    UInt64 version = PROTOCOL_VERSION;
    Int32 code = 0;
    String message;
    UInt64 task_serialization_version = 0;
    UInt64 plan_serialization_version = 0;

    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);
};

struct StartTasks
{
    static constexpr PacketType type = PacketType::StartTasks;

    UInt64 request_id = 0;
    String temp_path;
    TaskCollectors collectors;
    OpenTelemetry::TracingContext trace_context;
    DistributedQueryTaskBatch batch;

    void write(WriteBuffer & out) const;
    /// Writes the same bytes, except that the stage plans go to `write_plan`, as in `serializeTaskBatch`.
    void write(WriteBuffer & out, const TaskBatchPlanWriter & write_plan) const;
    void read(ReadBuffer & in);
};

struct StartTasksReply
{
    static constexpr PacketType type = PacketType::StartTasksReply;

    UInt64 request_id = 0;
    Int32 code = 0;
    String message;

    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);
};

struct GetStatuses
{
    static constexpr PacketType type = PacketType::GetStatuses;

    UInt64 request_id = 0;
    /// How long the worker may hold the reply until one of the tasks ends or reports its failure.
    UInt64 wait_milliseconds = 0;
    Strings task_ids;

    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);
};

struct GetStatusesReply
{
    static constexpr PacketType type = PacketType::GetStatusesReply;

    UInt64 request_id = 0;
    std::vector<DistributedQueryTaskStatusEntry> entries;

    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);
};

template <PacketType packet_type>
struct TaskIdsRequest
{
    static constexpr PacketType type = packet_type;

    UInt64 request_id = 0;
    Strings task_ids;

    void write(WriteBuffer & out) const
    {
        writeVarUInt(request_id, out);
        writeTaskIds(task_ids, out);
    }

    void read(ReadBuffer & in)
    {
        readVarUInt(request_id, in);
        task_ids = readTaskIds(in);
    }
};

using CancelTasks = TaskIdsRequest<PacketType::CancelTasks>;
using ForgetTasks = TaskIdsRequest<PacketType::ForgetTasks>;

template <PacketType packet_type>
struct RequestIdOnly
{
    static constexpr PacketType type = packet_type;

    UInt64 request_id = 0;

    void write(WriteBuffer & out) const { writeVarUInt(request_id, out); }
    void read(ReadBuffer & in) { readVarUInt(request_id, in); }
};

using CancelTasksReply = RequestIdOnly<PacketType::CancelTasksReply>;
using ForgetTasksReply = RequestIdOnly<PacketType::ForgetTasksReply>;
using Ping = RequestIdOnly<PacketType::Ping>;
using Pong = RequestIdOnly<PacketType::Pong>;

struct Error
{
    static constexpr PacketType type = PacketType::Error;

    UInt64 request_id = 0;
    Int32 code = 0;
    String message;

    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);
};

}

}
