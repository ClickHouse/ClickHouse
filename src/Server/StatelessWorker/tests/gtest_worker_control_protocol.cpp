#include <gtest/gtest.h>

#include <Server/StatelessWorker/StatelessWorkerControlProtocol.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Core/Field.h>
#include <Common/Exception.h>

#include <fmt/format.h>

#include <cstring>
#include <functional>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LIMIT_EXCEEDED;
    extern const int PROTOCOL_VERSION_MISMATCH;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

using namespace DB;
using namespace DB::StatelessWorkerControlProtocol;

namespace
{

template <typename Message>
String frameOf(const Message & message)
{
    WriteBufferFromOwnString out;
    writeFrame(message, out);
    out.finalize();
    return out.str();
}

std::string_view bodyOf(const String & frame)
{
    return std::string_view(frame).substr(sizeof(PacketHeader));
}

PacketHeader headerOf(const String & frame)
{
    PacketHeader header{};
    memcpy(&header, frame.data(), sizeof(header));
    return header;
}

/// Reads `frame` with the blocking reader, then parses it from memory.
template <typename Message>
std::vector<Message> readBack(const String & frame, Sender sender)
{
    std::vector<Message> messages(2);
    ReadBufferFromString in(frame);
    const PacketHeader header = readHeader(in, sender);
    EXPECT_EQ(header.packet_type, static_cast<UInt64>(Message::type));
    EXPECT_EQ(header.bytes_size, frame.size() - sizeof(PacketHeader));
    readBody(header, in, messages[0]);
    EXPECT_TRUE(in.eof());
    messages[1] = parseBody<Message>(header, bodyOf(frame));
    return messages;
}

/// The error code `function` throws with, 0 when it does not throw.
int errorCodeOf(const std::function<void()> & function)
{
    try
    {
        function();
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    return 0;
}

template <typename Message>
void readAs(const PacketHeader & header, ReadBuffer & in)
{
    Message message;
    readBody(header, in, message);
}

/// Reads the body of any control packet, as the receiving side dispatches on the type.
void readAnyBody(const PacketHeader & header, ReadBuffer & in)
{
    switch (static_cast<PacketType>(header.packet_type))
    {
        case PacketType::ControlHello:
            readAs<ControlHello>(header, in);
            break;
        case PacketType::ControlHelloReply:
            readAs<ControlHelloReply>(header, in);
            break;
        case PacketType::StartTasks:
            readAs<StartTasks>(header, in);
            break;
        case PacketType::StartTasksReply:
            readAs<StartTasksReply>(header, in);
            break;
        case PacketType::GetStatuses:
            readAs<GetStatuses>(header, in);
            break;
        case PacketType::GetStatusesReply:
            readAs<GetStatusesReply>(header, in);
            break;
        case PacketType::CancelTasks:
            readAs<CancelTasks>(header, in);
            break;
        case PacketType::CancelTasksReply:
            readAs<CancelTasksReply>(header, in);
            break;
        case PacketType::ForgetTasks:
            readAs<ForgetTasks>(header, in);
            break;
        case PacketType::ForgetTasksReply:
            readAs<ForgetTasksReply>(header, in);
            break;
        case PacketType::Ping:
            readAs<Ping>(header, in);
            break;
        case PacketType::Pong:
            readAs<Pong>(header, in);
            break;
        case PacketType::Error:
            readAs<Error>(header, in);
            break;
    }
}

DistributedQueryTaskBatch::Task makeTask(const String & task_id, const String & input_exchange)
{
    DistributedQueryTaskBatch::Task batch_task;
    batch_task.task.task_id = task_id;
    batch_task.task.parameters.parameters["bucket"] = Field(UInt64(1));
    batch_task.task.input_exchange_streams = {ExchangeStreamId(input_exchange, "0", "0")};
    batch_task.exchange_stream_sources.stream_hosts[input_exchange + "_0_0"] = StreamSourceAddress{.host = "host-a", .port = 9001};
    return batch_task;
}

DistributedQueryTaskBatch makeBatch()
{
    DistributedQueryTaskBatch batch;
    batch.initial_query_id = "q1";
    ExchangeDescription exchange;
    exchange.name = "exchange_0";
    exchange.kind = ExchangeDescription::Kind::Streaming;
    batch.exchanges[exchange.name] = exchange;
    batch.settings_changes.emplace_back("max_threads", Field(UInt64(4)));
    batch.stages.push_back({"stage_1", std::make_shared<const String>("plan of stage_1"),
        {makeTask("stage_1_0", "exchange_0"), makeTask("stage_1_1", "exchange_0")}});
    batch.stages.push_back({"main", std::make_shared<const String>("plan of main"), {makeTask("main", "exchange_1")}});
    return batch;
}

StartTasks makeStartTasks()
{
    StartTasks request;
    request.request_id = 3;
    request.temp_path = "tmp/q1";
    request.collectors.logs = true;
    request.trace_context.trace_id = OpenTelemetry::TracingContext::generateTraceId();
    request.trace_context.span_id = 42;
    request.trace_context.tracestate = "vendor=value";
    request.trace_context.trace_flags = OpenTelemetry::TRACE_FLAG_SAMPLED;
    request.batch = makeBatch();
    return request;
}

GetStatusesReply makeStatusesReply()
{
    GetStatusesReply reply;
    reply.request_id = 5;
    reply.entries.resize(2);
    reply.entries[0].task_id = "q::stage_1_0";
    reply.entries[0].status.status = "Running";
    reply.entries[0].status.progress.read_rows = 42;
    reply.entries[0].collectors.logs = true;
    reply.entries[0].status.logs = TaskLogsPayload{.begin_offset = 7, .dropped_total = 2, .rows = {}};
    reply.entries[1].task_id = "q::main";
    reply.entries[1].status.status = "Failed";
    reply.entries[1].status.error_message = "boom";
    reply.entries[1].status.error_code = 241;
    return reply;
}

/// A frame of every message type, with the side that sends it.
std::vector<std::pair<String, Sender>> framesOfEveryType()
{
    return {
        {frameOf(ControlHello{.unique_query_id = "q", .auth_token = "token", .user = "u", .password = "p", .initiator = "host"}), Sender::Initiator},
        {frameOf(ControlHelloReply{.message = "ok", .task_serialization_version = 5, .plan_serialization_version = 21}), Sender::Worker},
        {frameOf(makeStartTasks()), Sender::Initiator},
        {frameOf(StartTasksReply{.request_id = 3, .code = 0, .message = ""}), Sender::Worker},
        {frameOf(GetStatuses{.request_id = 4, .wait_milliseconds = 300, .task_ids = {"q::a", "q::b"}}), Sender::Initiator},
        {frameOf(makeStatusesReply()), Sender::Worker},
        {frameOf(CancelTasks{.request_id = 6, .task_ids = {"q::a"}}), Sender::Initiator},
        {frameOf(CancelTasksReply{.request_id = 6}), Sender::Worker},
        {frameOf(ForgetTasks{.request_id = 7, .task_ids = {"q::a", "q::b"}}), Sender::Initiator},
        {frameOf(ForgetTasksReply{.request_id = 7}), Sender::Worker},
        {frameOf(Ping{.request_id = 8}), Sender::Initiator},
        {frameOf(Pong{.request_id = 8}), Sender::Worker},
        {frameOf(Error{.request_id = 0, .code = 202, .message = "busy"}), Sender::Worker},
    };
}

}

TEST(WorkerControlProtocol, PacketTypesAreApartFromExchangeOnes)
{
    for (UInt64 type : {StreamingExchangeProtocol::SourceHello, StreamingExchangeProtocol::SinkHello,
             StreamingExchangeProtocol::Data, StreamingExchangeProtocol::NoMoreDataNeeded})
        EXPECT_FALSE(isControlPacketType(type)) << type;
    for (const auto & [frame, sender] : framesOfEveryType())
        EXPECT_TRUE(isControlPacketType(headerOf(frame).packet_type));
}

TEST(WorkerControlProtocol, HelloRoundTrip)
{
    const ControlHello hello{
        .purpose = Purpose::Probe, .unique_query_id = "q1", .auth_token = "token", .user = "interserver", .password = "secret",
        .initiator = "host-a:9000"};
    const String frame = frameOf(hello);
    for (const auto & restored : readBack<ControlHello>(frame, Sender::Initiator))
    {
        EXPECT_EQ(restored.version, PROTOCOL_VERSION);
        EXPECT_EQ(restored.purpose, Purpose::Probe);
        EXPECT_EQ(restored.unique_query_id, "q1");
        EXPECT_EQ(restored.auth_token, "token");
        EXPECT_EQ(restored.user, "interserver");
        EXPECT_EQ(restored.password, "secret");
        EXPECT_EQ(restored.initiator, "host-a:9000");
        EXPECT_EQ(frameOf(restored), frame);
    }

    const ControlHelloReply reply{.code = 202, .message = "busy", .task_serialization_version = 5, .plan_serialization_version = 21};
    const String reply_frame = frameOf(reply);
    for (const auto & restored : readBack<ControlHelloReply>(reply_frame, Sender::Worker))
    {
        EXPECT_EQ(restored.code, 202);
        EXPECT_EQ(restored.message, "busy");
        EXPECT_EQ(restored.task_serialization_version, 5u);
        EXPECT_EQ(restored.plan_serialization_version, 21u);
        EXPECT_EQ(frameOf(restored), reply_frame);
    }
}

TEST(WorkerControlProtocol, StartTasksRoundTrip)
{
    const StartTasks request = makeStartTasks();
    const String frame = frameOf(request);
    for (const auto & restored : readBack<StartTasks>(frame, Sender::Initiator))
    {
        EXPECT_EQ(restored.request_id, 3u);
        EXPECT_EQ(restored.temp_path, "tmp/q1");
        EXPECT_TRUE(restored.collectors.logs);
        EXPECT_EQ(restored.trace_context.trace_id, request.trace_context.trace_id);
        EXPECT_EQ(restored.trace_context.span_id, 42u);
        EXPECT_EQ(restored.trace_context.tracestate, "vendor=value");
        EXPECT_EQ(restored.trace_context.trace_flags, OpenTelemetry::TRACE_FLAG_SAMPLED);
        ASSERT_EQ(restored.batch.stages.size(), 2u);
        EXPECT_EQ(*restored.batch.stages[0].serialized_query_plan, "plan of stage_1");
        EXPECT_EQ(restored.batch.stages[0].tasks[1].task.task_id, "stage_1_1");
        EXPECT_EQ(frameOf(restored), frame);
    }

    /// With the plan writer, the body is the same once each stage plan is put where the writer got it.
    WriteBufferFromOwnString without_plans;
    std::vector<std::pair<size_t, SerializedQueryPlanPtr>> plans;
    request.write(without_plans, [&](const SerializedQueryPlanPtr & plan) { plans.emplace_back(without_plans.count(), plan); });
    without_plans.finalize();
    ASSERT_EQ(plans.size(), 2u);
    String assembled;
    size_t written = 0;
    for (const auto & [offset, plan] : plans)
    {
        assembled += without_plans.str().substr(written, offset - written);
        assembled += *plan;
        written = offset;
    }
    assembled += without_plans.str().substr(written);
    EXPECT_EQ(assembled, bodyOf(frame));
}

TEST(WorkerControlProtocol, StatusesRoundTrip)
{
    const GetStatuses request{.request_id = 4, .wait_milliseconds = 300, .task_ids = {"q::stage_1_0", "q::main"}};
    const String frame = frameOf(request);
    for (const auto & restored : readBack<GetStatuses>(frame, Sender::Initiator))
    {
        EXPECT_EQ(restored.request_id, 4u);
        EXPECT_EQ(restored.wait_milliseconds, 300u);
        EXPECT_EQ(restored.task_ids, request.task_ids);
        EXPECT_EQ(frameOf(restored), frame);
    }

    const GetStatusesReply reply = makeStatusesReply();
    const String reply_frame = frameOf(reply);
    for (auto & restored : readBack<GetStatusesReply>(reply_frame, Sender::Worker))
    {
        EXPECT_EQ(restored.request_id, 5u);
        ASSERT_EQ(restored.entries.size(), 2u);
        EXPECT_EQ(restored.entries[0].task_id, "q::stage_1_0");
        EXPECT_EQ(restored.entries[0].status.status, "Running");
        EXPECT_EQ(restored.entries[0].status.progress.read_rows.load(), 42u);
        ASSERT_TRUE(restored.entries[0].status.logs.has_value());
        EXPECT_EQ(restored.entries[0].status.logs->begin_offset, 7u);
        EXPECT_EQ(restored.entries[0].status.logs->dropped_total, 2u);
        EXPECT_EQ(restored.entries[1].status.status, "Failed");
        EXPECT_EQ(restored.entries[1].status.error_message, "boom");
        EXPECT_EQ(restored.entries[1].status.error_code, 241);
        EXPECT_FALSE(restored.entries[1].status.logs.has_value());
        /// The collectors are not sent; they decide which payloads are written.
        restored.entries[0].collectors.logs = true;
        EXPECT_EQ(frameOf(restored), reply_frame);
    }
}

TEST(WorkerControlProtocol, OtherMessagesRoundTrip)
{
    const String start_reply = frameOf(StartTasksReply{.request_id = 3, .code = 241, .message = "Cannot schedule"});
    for (const auto & restored : readBack<StartTasksReply>(start_reply, Sender::Worker))
    {
        EXPECT_EQ(restored.request_id, 3u);
        EXPECT_EQ(restored.code, 241);
        EXPECT_EQ(restored.message, "Cannot schedule");
    }

    const Strings task_ids = {"q::stage_1_0", "q::stage_1_1"};
    for (const auto & restored : readBack<CancelTasks>(frameOf(CancelTasks{.request_id = 6, .task_ids = task_ids}), Sender::Initiator))
    {
        EXPECT_EQ(restored.request_id, 6u);
        EXPECT_EQ(restored.task_ids, task_ids);
    }
    for (const auto & restored : readBack<ForgetTasks>(frameOf(ForgetTasks{.request_id = 7, .task_ids = task_ids}), Sender::Initiator))
    {
        EXPECT_EQ(restored.request_id, 7u);
        EXPECT_EQ(restored.task_ids, task_ids);
    }

    for (const auto & restored : readBack<CancelTasksReply>(frameOf(CancelTasksReply{.request_id = 6}), Sender::Worker))
        EXPECT_EQ(restored.request_id, 6u);
    for (const auto & restored : readBack<ForgetTasksReply>(frameOf(ForgetTasksReply{.request_id = 7}), Sender::Worker))
        EXPECT_EQ(restored.request_id, 7u);
    for (const auto & restored : readBack<Ping>(frameOf(Ping{.request_id = 1ULL << 40}), Sender::Initiator))
        EXPECT_EQ(restored.request_id, 1ULL << 40);
    for (const auto & restored : readBack<Pong>(frameOf(Pong{.request_id = 8}), Sender::Worker))
        EXPECT_EQ(restored.request_id, 8u);

    for (const auto & restored : readBack<Error>(frameOf(Error{.request_id = 0, .code = 202, .message = "busy"}), Sender::Worker))
    {
        EXPECT_EQ(restored.request_id, 0u);
        EXPECT_EQ(restored.code, 202);
        EXPECT_EQ(restored.message, "busy");
    }
}

TEST(WorkerControlProtocol, VersionMismatchNamesBothVersions)
{
    const String hello = frameOf(ControlHello{
        .version = PROTOCOL_VERSION + 1, .unique_query_id = "q", .auth_token = "", .user = "u", .password = "", .initiator = "host"});
    ReadBufferFromString in(hello);
    const PacketHeader header = readHeader(in, Sender::Initiator);
    ControlHello restored;
    try
    {
        readBody(header, in, restored);
        ADD_FAILURE() << "a hello of another version was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::PROTOCOL_VERSION_MISMATCH);
        EXPECT_NE(e.message().find(fmt::format("version {}", PROTOCOL_VERSION + 1)), String::npos) << e.message();
        EXPECT_NE(e.message().find(fmt::format("version {}", PROTOCOL_VERSION)), String::npos) << e.message();
    }

    const String reply = frameOf(ControlHelloReply{.version = PROTOCOL_VERSION + 1, .code = 902, .message = "mismatch"});
    EXPECT_EQ(errorCodeOf([&] { parseBody<ControlHelloReply>(headerOf(reply), bodyOf(reply)); }), ErrorCodes::PROTOCOL_VERSION_MISMATCH);
}

TEST(WorkerControlProtocol, BodiesOverTheCapAreRejected)
{
    for (const auto & [frame, sender] : framesOfEveryType())
    {
        const auto type = static_cast<PacketType>(headerOf(frame).packet_type);
        const UInt64 cap = maxBodyBytes(type);
        EXPECT_EQ(makeHeader(type, cap).bytes_size, cap);
        EXPECT_EQ(errorCodeOf([&] { makeHeader(type, cap + 1); }), ErrorCodes::LIMIT_EXCEEDED) << packetTypeName(static_cast<UInt64>(type));

        PacketHeader oversized{.packet_type = static_cast<UInt64>(type), .bytes_size = cap + 1};
        String oversized_frame(reinterpret_cast<const char *>(&oversized), sizeof(oversized));
        ReadBufferFromString in(oversized_frame);
        EXPECT_EQ(errorCodeOf([&] { readHeader(in, sender); }), ErrorCodes::INCORRECT_DATA) << packetTypeName(static_cast<UInt64>(type));
    }

    /// 20 ids of 1 MiB are over the 16 MiB of a request with task ids.
    const GetStatuses too_many{.request_id = 1, .wait_milliseconds = 0, .task_ids = Strings(20, String(1024 * 1024, 'x'))};
    EXPECT_EQ(errorCodeOf([&] { frameOf(too_many); }), ErrorCodes::LIMIT_EXCEEDED);

    /// A long message is cut, so the error still fits its frame.
    const String error = frameOf(Error{.request_id = 1, .code = 1, .message = String(200 * 1024, 'm')});
    for (const auto & restored : readBack<Error>(error, Sender::Worker))
        EXPECT_EQ(restored.message, String(16 * 1024, 'm'));
}

TEST(WorkerControlProtocol, TruncatedFramesAreRejected)
{
    for (const auto & [frame, sender] : framesOfEveryType())
    {
        const PacketHeader header = headerOf(frame);
        const String name = packetTypeName(header.packet_type);
        for (size_t size = 0; size < frame.size(); ++size)
        {
            /// The stream ends inside the frame.
            ReadBufferFromString in(frame.substr(0, size));
            EXPECT_NE(errorCodeOf([&] { readAnyBody(readHeader(in, sender), in); }), 0) << name << " cut at " << size;

            if (size < sizeof(PacketHeader))
                continue;
            const std::string_view body = std::string_view(frame).substr(sizeof(PacketHeader), size - sizeof(PacketHeader));
            /// Less body than the header says.
            EXPECT_EQ(errorCodeOf([&] { parseBody(header, static_cast<PacketType>(header.packet_type), body, [](ReadBuffer &) {}); }),
                ErrorCodes::INCORRECT_DATA) << name << " cut at " << size;
            /// A header that matches the shorter body, which ends inside the message.
            PacketHeader shorter = header;
            shorter.bytes_size = body.size();
            ReadBufferFromMemory body_in(body.data(), body.size());
            EXPECT_NE(errorCodeOf([&] { readAnyBody(shorter, body_in); }), 0) << name << " cut at " << size;
        }
    }
}

TEST(WorkerControlProtocol, TrailingDataIsRejected)
{
    for (const auto & [frame, sender] : framesOfEveryType())
    {
        PacketHeader header = headerOf(frame);
        ++header.bytes_size;
        const String body = String(bodyOf(frame)) + "x";
        String longer(reinterpret_cast<const char *>(&header), sizeof(header));
        longer += body;

        ReadBufferFromString in(longer);
        EXPECT_EQ(errorCodeOf([&] { readAnyBody(readHeader(in, sender), in); }), ErrorCodes::INCORRECT_DATA)
            << packetTypeName(header.packet_type);
    }
}

TEST(WorkerControlProtocol, WrongTypesAreRejected)
{
    const String ping = frameOf(Ping{.request_id = 1});
    const String pong = frameOf(Pong{.request_id = 1});

    /// The worker reads what the initiator sends, the initiator what the worker sends.
    ReadBufferFromString pong_at_worker(pong);
    EXPECT_EQ(errorCodeOf([&] { readHeader(pong_at_worker, Sender::Initiator); }), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
    ReadBufferFromString ping_at_initiator(ping);
    EXPECT_EQ(errorCodeOf([&] { readHeader(ping_at_initiator, Sender::Worker); }), ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);

    /// An exchange hello is no control packet.
    const PacketHeader source_hello{.packet_type = StreamingExchangeProtocol::SourceHello, .bytes_size = 8};
    EXPECT_EQ(errorCodeOf([&] { checkHeader(source_hello, Sender::Initiator); }), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
    const PacketHeader sink_hello{.packet_type = StreamingExchangeProtocol::SinkHello, .bytes_size = 8};
    EXPECT_EQ(errorCodeOf([&] { checkHeader(sink_hello, Sender::Worker); }), ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);

    /// A body read as another message than its header names.
    const String cancel = frameOf(CancelTasks{.request_id = 2, .task_ids = {"q::a"}});
    ReadBufferFromString cancel_in(cancel);
    const PacketHeader cancel_header = readHeader(cancel_in, Sender::Initiator);
    ForgetTasks forget;
    EXPECT_EQ(errorCodeOf([&] { readBody(cancel_header, cancel_in, forget); }), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
    const String error = frameOf(Error{.request_id = 2, .code = 1, .message = "m"});
    EXPECT_EQ(errorCodeOf([&] { parseBody<Pong>(headerOf(error), bodyOf(error)); }), ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);

    /// A hello with a purpose this build does not know.
    WriteBufferFromOwnString unknown_purpose;
    writeVarUInt(PROTOCOL_VERSION, unknown_purpose);
    writeVarUInt(7, unknown_purpose);
    unknown_purpose.finalize();
    const PacketHeader hello_header = makeHeader(PacketType::ControlHello, unknown_purpose.str().size());
    EXPECT_EQ(errorCodeOf([&] { parseBody<ControlHello>(hello_header, unknown_purpose.str()); }), ErrorCodes::INCORRECT_DATA);
}
