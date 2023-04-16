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
    Exists = 5,
    IsFile = 6,
    IsDirectory = 7,
    GetFileSize = 8,
    CreateDirectory = 9,
    CreateDirectories = 10,
    ClearDirectory = 11,
    MoveDirectory = 12,
    IterateDirectory = 13,
    EndIterateDirectory = 113,
    CreateFile = 14,
    MoveFile = 15,
    ReplaceFile = 16,
    Copy = 17,
    CopyDirectoryContent = 18,  // TODO: fix test
    ListFiles = 19,             // TODO: later
    ReadFile = 20,              // TODO: improve
    WriteFile = 21,             // TODO: improve
    EndWriteFile = 121,
    RemoveFile = 22,
    RemoveFileIfExists = 23,
    RemoveDirectory = 24,
    RemoveRecursive = 25,
    SetLastModified = 26,
    GetLastModified = 27,
    GetLastChanged = 28,
    SetReadOnly = 29,           // TODO fix test
    CreateHardLink = 30,
    TruncateFile = 31,
    DataPacket = 55,
    DirIterEntry = 56,
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
            // TODO: split on different cases
            sendError(getCurrentExceptionMessage(false));
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
    std::string path2;
    bool boolVar;
    size_t sizeTVar;
    time_t timeVar;

    switch (packet_type)
    {
        case Hello:
            receiveUnexpectedHello();
        case Ping:
            writeVarUInt(Ping, *out);
            out->next();
            break;
        case GetTotalSpace:
            writeVarUInt(GetTotalSpace, *out);
            writeVarUInt(disk->getTotalSpace(), *out);
            out->next();
            break;
        case GetAvailableSpace:
            writeVarUInt(GetAvailableSpace, *out);
            writeVarUInt(disk->getAvailableSpace(), *out);
            out->next();
            break;
        case Exists:
            receivePath(path);
            boolVar = disk->exists(path);
            writeVarUInt(Exists, *out);
            writeBoolText(boolVar, *out);
            out->next();
            break;
        case IsFile:
            receivePath(path);
            boolVar = disk->isFile(path);
            writeVarUInt(IsFile, *out);
            writeBoolText(boolVar, *out);
            out->next();
            break;
        case IsDirectory:
            receivePath(path);
            boolVar = disk->isDirectory(path);
            writeVarUInt(IsDirectory, *out);
            writeBoolText(boolVar, *out);
            out->next();
            break;
        case GetFileSize:
            receivePath(path);
            sizeTVar = disk->getFileSize(path);
            writeVarUInt(GetFileSize, *out);
            writeVarUInt(sizeTVar, *out);
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
        case ClearDirectory:
            receivePath(path);
            disk->clearDirectory(path);
            writeVarUInt(ClearDirectory, *out);
            out->next();
            break;
        case MoveDirectory:
            receivePath(path);
            receivePath(path2);
            disk->moveDirectory(path, path2);
            writeVarUInt(MoveDirectory, *out);
            out->next();
            break;
        case IterateDirectory:
            iterateDirectory();
            break;
        case CreateFile:
            receivePath(path);
            disk->createFile(path);
            writeVarUInt(CreateFile, *out);
            out->next();
            break;
        case MoveFile:
            receivePath(path);
            receivePath(path2);
            disk->moveFile(path, path2);
            writeVarUInt(MoveFile, *out);
            out->next();
            break;
        case ReplaceFile:
            receivePath(path);
            receivePath(path2);
            disk->replaceFile(path, path2);
            writeVarUInt(ReplaceFile, *out);
            out->next();
            break;
        case Copy:
            receivePath(path);
            receivePath(path2);
            disk->copy(path, disk, path2);
            writeVarUInt(Copy, *out);
            out->next();
            break;
        case CopyDirectoryContent:
            receivePath(path);
            receivePath(path2);
            disk->copyDirectoryContent(path, disk, path2);
            writeVarUInt(CopyDirectoryContent, *out);
            out->next();
            break;
        case ReadFile:
            readFile();
            break;
        case WriteFile:
            writeFile();
            break;
        case RemoveFile:
            receivePath(path);
            disk->removeFile(path);
            writeVarUInt(RemoveFile, *out);
            out->next();
            break;
        case RemoveFileIfExists:
            receivePath(path);
            disk->removeFileIfExists(path);
            writeVarUInt(RemoveFileIfExists, *out);
            out->next();
            break;
        case RemoveDirectory:
            receivePath(path);
            disk->removeDirectory(path);
            writeVarUInt(RemoveDirectory, *out);
            out->next();
            break;
        case RemoveRecursive:
            receivePath(path);
            disk->removeRecursive(path);
            writeVarUInt(RemoveRecursive, *out);
            out->next();
            break;
        case SetLastModified:
            receivePath(path);
            readVarUInt(timeVar, *in);
            disk->setLastModified(path, Poco::Timestamp::fromEpochTime(timeVar));
            writeVarUInt(SetLastModified, *out);
            out->next();
            break;
        case GetLastModified:
            receivePath(path);
            timeVar = disk->getLastModified(path).epochTime();
            writeVarUInt(GetLastModified, *out);
            writeVarUInt(timeVar, *out);
            out->next();
            break;
        case GetLastChanged:
            receivePath(path);
            timeVar = disk->getLastChanged(path);
            writeVarUInt(GetLastChanged, *out);
            writeVarUInt(timeVar, *out);
            out->next();
            break;
        case SetReadOnly:
            receivePath(path);
            disk->setReadOnly(path);
            writeVarUInt(SetReadOnly, *out);
            out->next();
            break;
        case CreateHardLink:
            receivePath(path);
            receivePath(path2);
            disk->createHardLink(path, path2);
            writeVarUInt(CreateHardLink, *out);
            out->next();
            break;
        case TruncateFile:
            receivePath(path);
            readVarUInt(sizeTVar, *in);
            disk->truncateFile(path, sizeTVar);
            writeVarUInt(TruncateFile, *out);
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

void RemoteFSHandler::iterateDirectory()
{
    std::string path;
    receivePath(path);
    for (auto iter = disk->iterateDirectory(path); iter->isValid(); iter->next()) {
        LOG_TRACE(log, "Writing dir entry {}", iter->path());
        writeVarUInt(DirIterEntry, *out);
        writeStringBinary(iter->path(), *out);
    }
    writeVarUInt(EndIterateDirectory, *out);
    out->next();
}

void RemoteFSHandler::readFile()
{
    std::string strData;
    receivePath(strData); // Read path
    size_t offset;
    readVarUInt(offset, *in);
    LOG_TRACE(log, "Received offset {}", offset);
    size_t size;
    readVarUInt(size, *in);
    LOG_TRACE(log, "Received size {}", size);
    auto readBuf = disk->readFile(strData);
    readBuf->seek(offset, SEEK_SET);
    strData.resize(size);
    auto bytes_read = readBuf->read(strData.data(), size);
    strData.resize(bytes_read);
    writeVarUInt(ReadFile, *out);
    writeStringBinary(strData, *out);
    out->next();
}

void RemoteFSHandler::writeFile()
{
    std::string strData;
    receivePath(strData);
    size_t buf_size;
    readVarUInt(buf_size, *in);
    LOG_TRACE(log, "Received buf_size {}", buf_size);
    uint modeRaw;
    readVarUInt(modeRaw, *in);
    WriteMode mode = WriteMode(modeRaw);
    LOG_TRACE(log, "Received mode {}", mode);
    auto writeBuf = disk->writeFile(strData, buf_size, mode);
    writeVarUInt(WriteFile, *out);
    out->next();
    UInt64 packet_type = 0;
    while (true) {
        readVarUInt(packet_type, *in);
        switch (packet_type) {
            case DataPacket:
                readStringBinary(strData, *in);
                LOG_TRACE(log, "Received data {}", strData);
                writeString(strData, *writeBuf);
                writeBuf->next(); // TODO maybe remove this line
                writeVarUInt(DataPacket, *out);
                out->next();
                break;
            case EndWriteFile:
                LOG_TRACE(log, "Close file");
                writeBuf->sync();
                writeVarUInt(EndWriteFile, *out);
                out->next();
                return;
            default:
                throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unknown packet {} from client", toString(packet_type));
        }
    }
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
