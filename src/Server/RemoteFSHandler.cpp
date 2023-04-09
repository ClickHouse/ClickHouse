#include <algorithm>
#include <iterator>
#include <memory>
#include <mutex>
#include <vector>
#include <string_view>
#include <cstring>
#include <base/types.h>
#include <base/scope_guard.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>
#include <Common/NetException.h>
#include <Common/setThreadName.h>
#include <Common/OpenSSLHelpers.h>
#include <IO/Progress.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/TablesStatus.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/Session.h>
#include <Server/TCPServer.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/StorageS3Cluster.h>
#include <Core/ExternalTable.h>
#include <Access/AccessControl.h>
#include <Access/Credentials.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Compression/CompressionFactory.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <fmt/format.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>

#include "Core/Protocol.h"
#include "Storages/MergeTree/RequestResponse.h"
#include "RemoteFSHandler.h"

using namespace DB;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CLIENT_HAS_CONNECTED_TO_WRONG_PORT;
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_DISK;
    extern const int POCO_EXCEPTION;
    extern const int SOCKET_TIMEOUT;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_PROTOCOL;
    extern const int AUTHENTICATION_FAILED;
}

enum {
    Hello = 0,
    Ping = 1,
    GetTotalSpace = 2,
    GetAvailableSpace = 3,
    GetUnreservedSpace = 4,
    Exists = 5, // OK
    IsFile = 6, // OK
    IsDirectory = 7, // OK
    GetFileSize = 8,
    CreateDirectory = 9, // OK
    CreateDirectories = 10, // OK
    ClearDirectory = 11,
    MoveDirectory = 12,
    IterateDirectory = 13, // ---------- HARD ----------
    CreateFile = 14, // OK
    MoveFile = 15,
    ReplaceFile = 16, 
    Copy = 17,
    CopyDirectoryContent = 18,
    ListFiles = 19,
    ReadFile = 20, // ---------- HARD ----------
    WriteFile = 21, // ---------- HARD ----------
    RemoveFile = 22,
    RemoveFileIfExists = 23,
    RemoveDirectory = 24,
    RemoveRecursive = 25,
    SetLastModified = 26,
    GetLastModified = 27,
    GetLastChanged = 28,
    SetReadOnly = 29,
    CreateHardLink = 30,
    TruncateFile = 31,
    Error = 255
};


RemoteFSHandler::RemoteFSHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, std::string server_display_name_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , log(&Poco::Logger::get("RemoteFSHandler"))
    , server_display_name(std::move(server_display_name_))
{
}
RemoteFSHandler::RemoteFSHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, TCPProtocolStackData & stack_data, std::string server_display_name_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , log(&Poco::Logger::get("RemoteFSHandler"))
    , forwarded_for(stack_data.forwarded_for)
    , certificate(stack_data.certificate)
    , server_display_name(std::move(server_display_name_))
{
}

RemoteFSHandler::~RemoteFSHandler()
{
    try
    {
        // TODO
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}
void RemoteFSHandler::run()
{
    setThreadName("RemoteFSHandler");
    ThreadStatus thread_status;

    extractConnectionSettingsFromContext(server.context());

    socket().setReceiveTimeout(receive_timeout);
    socket().setSendTimeout(send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    if (in->eof())
    {
        LOG_INFO(log, "Client has not sent any data.");
        return;
    }

    try
    {
        receiveHello();
        sendHello();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
        {
            LOG_INFO(log, "Client has gone away.");
            return;
        }
        if (e.code() == ErrorCodes::UNKNOWN_DISK)
        {
            LOG_TRACE(log, "Got error {}", e.message());
            sendError(e.message());
            return;
        }
        throw;
    }

    while (tcp_server.isOpen())
    {
        /// We are waiting for a packet from the client. Thus, every `poll_interval` seconds check whether we need to shut down.
        {
            Stopwatch idle_time;
            UInt64 timeout_ms = std::min(poll_interval, idle_connection_timeout) * 1000000;
            while (tcp_server.isOpen() && !server.isCancelled() && !static_cast<ReadBufferFromPocoSocket &>(*in).poll(timeout_ms))
            {
                if (idle_time.elapsedSeconds() > idle_connection_timeout)
                {
                    LOG_TRACE(log, "Closing idle connection");
                    return;
                }
            }
        }

        /// If we need to shut down, or client disconnects.
        if (!tcp_server.isOpen() || server.isCancelled() || in->eof())
        {
            LOG_TEST(log, "Closing connection (open: {}, cancelled: {}, eof: {})", tcp_server.isOpen(), server.isCancelled(), in->eof());
            break;
        }

        try
        {
            receivePacket();
        }
        catch (...)
        {
            sendError(getCurrentExceptionMessage(false));
            throw;
        }
    }
}

void RemoteFSHandler::extractConnectionSettingsFromContext(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    send_exception_with_stack_trace = settings.calculate_text_stack_trace;
    send_timeout = settings.send_timeout;
    receive_timeout = settings.receive_timeout;
    poll_interval = settings.poll_interval;
    idle_connection_timeout = settings.idle_connection_timeout;
    interactive_delay = settings.interactive_delay;
    sleep_in_send_tables_status = settings.sleep_in_send_tables_status_ms;
    unknown_packet_in_send_data = settings.unknown_packet_in_send_data;
    sleep_in_receive_cancel = settings.sleep_in_receive_cancel_ms;
    sleep_after_receiving_query = settings.sleep_after_receiving_query_ms;
}

void RemoteFSHandler::receiveHello()
{
    /// Receive `hello` packet.
    UInt64 packet_type = 0;

    readVarUInt(packet_type, *in);
    LOG_TRACE(log, "Received hello");
    if (packet_type != Hello)
    {
        throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet from client");
    }
    std::string disk_name;
    readStringBinary(disk_name, *in);
    disk = server.context()->getDisk(disk_name);
}

void RemoteFSHandler::receivePacket()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);

    LOG_TRACE(log, "Received {}", packet_type);

    std::string path;
    bool boolRes;

    switch (packet_type)
    {
        case Hello:
            receiveUnexpectedHello();
        case Ping:
            writeVarUInt(Ping, *out);
            out->next();
            break;
        case Exists:
            receivePath(path);
            boolRes = disk->exists(path);
            writeVarUInt(Exists, *out);
            writeBoolText(boolRes, *out);
            out->next();
            break;
        case IsFile:
            receivePath(path);
            boolRes = disk->isFile(path);
            writeVarUInt(IsFile, *out);
            writeBoolText(boolRes, *out);
            out->next();
            break;
        case IsDirectory:
            receivePath(path);
            boolRes = disk->isDirectory(path);
            writeVarUInt(IsDirectory, *out);
            writeBoolText(boolRes, *out);
            out->next();
            break;
        case CreateDirectory:
            receivePath(path);
            disk->createDirectory(path);
            writeVarUInt(CreateDirectory, *out);
            out->next();
            break;
        case CreateDirectories:
            receivePath(path);
            disk->createDirectories(path);
            writeVarUInt(CreateDirectories, *out);
            out->next();
            break;
        case CreateFile:
            receivePath(path);
            disk->createFile(path);
            writeVarUInt(CreateFile, *out);
            out->next();
            break;
        default:
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown packet {} from client", toString(packet_type));
    }
}

void RemoteFSHandler::receivePath(std::string &path)
{
    readStringBinary(path, *in);
    LOG_TRACE(log, "Received path {}", path);
}

void RemoteFSHandler::receiveUnexpectedHello()
{
    String skip_string;

    readStringBinary(skip_string, *in);

    throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Hello received from client");
}

void RemoteFSHandler::sendHello()
{
    writeVarUInt(Hello, *out);
    out->next();
}


void RemoteFSHandler::sendError(std::string errorMsg) 
{
    writeVarUInt(Error, *out);
    writeStringBinary(errorMsg, *out);
    out->next();
}

}
