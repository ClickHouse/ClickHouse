#include <algorithm>
#include <cstring>
#include <iterator>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>
#include <Access/AccessControl.h>
#include <Access/Credentials.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ExternalTable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/LimitReadBuffer.h>
#include <IO/Progress.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/Session.h>
#include <Interpreters/TablesStatus.h>
#include <Interpreters/executeQuery.h>
#include <Server/TCPServer.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageS3Cluster.h>
#include <base/scope_guard.h>
#include <base/types.h>
#include <fmt/format.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/NetException.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>

#include "Core/RemoteFSProtocol.h"
#include "RemoteFSHandler.h"
#include "Storages/MergeTree/RequestResponse.h"

using namespace DB;

namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_DISK;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}


RemoteFSHandler::RemoteFSHandler(
    IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, std::string server_display_name_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , log(&Poco::Logger::get("RemoteFSHandler"))
    , server_display_name(std::move(server_display_name_))
{
}
RemoteFSHandler::RemoteFSHandler(
    IServer & server_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    TCPProtocolStackData & stack_data,
    std::string server_display_name_)
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
            sendException(e);
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
            receiveRequest();
        }
        catch (const Exception & e)
        {
            sendException(e);
        }
        catch (...)
        {
            // TODO: split on different cases
            LOG_ERROR(log, "Got exception {}", getCurrentExceptionMessage(false));
            throw;
        }
    }
}

void RemoteFSHandler::extractConnectionSettingsFromContext(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    send_timeout = settings.send_timeout;
    receive_timeout = settings.receive_timeout;
    poll_interval = settings.poll_interval;
    idle_connection_timeout = settings.idle_connection_timeout;
    interactive_delay = settings.interactive_delay;
}

void RemoteFSHandler::receiveHello()
{
    /// Receive `hello` packet.
    UInt64 packet_type = 0;

    readVarUInt(packet_type, *in);
    LOG_TRACE(log, "Received hello");
    if (packet_type != RemoteFSProtocol::Hello)
    {
        throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet from client");
    }
    std::string disk_name;
    readStringBinary(disk_name, *in);
    disk = server.context()->getDisk(disk_name);
}

void RemoteFSHandler::receiveRequest()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);

    LOG_TRACE(log, "Received {}", packet_type);

    std::string path;
    std::string path2;
    bool bool_var;
    size_t size_t_var;
    time_t time_var;

    switch (packet_type)
    {
        case RemoteFSProtocol::Hello:
            receiveUnexpectedHello();
        case RemoteFSProtocol::Ping:
            writeVarUInt(RemoteFSProtocol::Ping, *out);
            out->next();
            break;
        case RemoteFSProtocol::GetTotalSpace:
            writeVarUInt(RemoteFSProtocol::GetTotalSpace, *out);
            writeVarUInt(disk->getTotalSpace(), *out);
            out->next();
            break;
        case RemoteFSProtocol::GetAvailableSpace:
            writeVarUInt(RemoteFSProtocol::GetAvailableSpace, *out);
            writeVarUInt(disk->getAvailableSpace(), *out);
            out->next();
            break;
        case RemoteFSProtocol::Exists:
            receivePath(path);
            bool_var = disk->exists(path);
            writeVarUInt(RemoteFSProtocol::Exists, *out);
            writeBoolText(bool_var, *out);
            out->next();
            break;
        case RemoteFSProtocol::IsFile:
            receivePath(path);
            bool_var = disk->isFile(path);
            writeVarUInt(RemoteFSProtocol::IsFile, *out);
            writeBoolText(bool_var, *out);
            out->next();
            break;
        case RemoteFSProtocol::IsDirectory:
            receivePath(path);
            bool_var = disk->isDirectory(path);
            writeVarUInt(RemoteFSProtocol::IsDirectory, *out);
            writeBoolText(bool_var, *out);
            out->next();
            break;
        case RemoteFSProtocol::GetFileSize:
            receivePath(path);
            size_t_var = disk->getFileSize(path);
            writeVarUInt(RemoteFSProtocol::GetFileSize, *out);
            writeVarUInt(size_t_var, *out);
            out->next();
            break;
        case RemoteFSProtocol::CreateDirectory:
            receivePath(path);
            disk->createDirectory(path);
            writeVarUInt(RemoteFSProtocol::CreateDirectory, *out);
            out->next();
            break;
        case RemoteFSProtocol::CreateDirectories:
            receivePath(path);
            disk->createDirectories(path);
            writeVarUInt(RemoteFSProtocol::CreateDirectories, *out);
            out->next();
            break;
        case RemoteFSProtocol::ClearDirectory:
            receivePath(path);
            disk->clearDirectory(path);
            writeVarUInt(RemoteFSProtocol::ClearDirectory, *out);
            out->next();
            break;
        case RemoteFSProtocol::MoveDirectory:
            receivePath(path);
            receivePath(path2);
            disk->moveDirectory(path, path2);
            writeVarUInt(RemoteFSProtocol::MoveDirectory, *out);
            out->next();
            break;
        case RemoteFSProtocol::IterateDirectory:
            iterateDirectory();
            break;
        case RemoteFSProtocol::CreateFile:
            receivePath(path);
            disk->createFile(path);
            writeVarUInt(RemoteFSProtocol::CreateFile, *out);
            out->next();
            break;
        case RemoteFSProtocol::MoveFile:
            receivePath(path);
            receivePath(path2);
            disk->moveFile(path, path2);
            writeVarUInt(RemoteFSProtocol::MoveFile, *out);
            out->next();
            break;
        case RemoteFSProtocol::ReplaceFile:
            receivePath(path);
            receivePath(path2);
            disk->replaceFile(path, path2);
            writeVarUInt(RemoteFSProtocol::ReplaceFile, *out);
            out->next();
            break;
        case RemoteFSProtocol::Copy:
            receivePath(path);
            receivePath(path2);
            disk->copy(path, disk, path2);
            writeVarUInt(RemoteFSProtocol::Copy, *out);
            out->next();
            break;
        case RemoteFSProtocol::CopyDirectoryContent:
            receivePath(path);
            receivePath(path2);
            disk->copyDirectoryContent(path, disk, path2);
            writeVarUInt(RemoteFSProtocol::CopyDirectoryContent, *out);
            out->next();
            break;
        case RemoteFSProtocol::ListFiles:
            listFiles();
            break;
        case RemoteFSProtocol::ReadFile:
            readFile();
            break;
        case RemoteFSProtocol::WriteFile:
            writeFile();
            break;
        case RemoteFSProtocol::RemoveFile:
            receivePath(path);
            disk->removeFile(path);
            writeVarUInt(RemoteFSProtocol::RemoveFile, *out);
            out->next();
            break;
        case RemoteFSProtocol::RemoveFileIfExists:
            receivePath(path);
            disk->removeFileIfExists(path);
            writeVarUInt(RemoteFSProtocol::RemoveFileIfExists, *out);
            out->next();
            break;
        case RemoteFSProtocol::RemoveDirectory:
            receivePath(path);
            disk->removeDirectory(path);
            writeVarUInt(RemoteFSProtocol::RemoveDirectory, *out);
            out->next();
            break;
        case RemoteFSProtocol::RemoveRecursive:
            receivePath(path);
            disk->removeRecursive(path);
            writeVarUInt(RemoteFSProtocol::RemoveRecursive, *out);
            out->next();
            break;
        case RemoteFSProtocol::SetLastModified:
            receivePath(path);
            readVarUInt(time_var, *in);
            disk->setLastModified(path, Poco::Timestamp::fromEpochTime(time_var));
            writeVarUInt(RemoteFSProtocol::SetLastModified, *out);
            out->next();
            break;
        case RemoteFSProtocol::GetLastModified:
            receivePath(path);
            time_var = disk->getLastModified(path).epochTime();
            writeVarUInt(RemoteFSProtocol::GetLastModified, *out);
            writeVarUInt(time_var, *out);
            out->next();
            break;
        case RemoteFSProtocol::GetLastChanged:
            receivePath(path);
            time_var = disk->getLastChanged(path);
            writeVarUInt(RemoteFSProtocol::GetLastChanged, *out);
            writeVarUInt(time_var, *out);
            out->next();
            break;
        case RemoteFSProtocol::SetReadOnly:
            receivePath(path);
            disk->setReadOnly(path);
            writeVarUInt(RemoteFSProtocol::SetReadOnly, *out);
            out->next();
            break;
        case RemoteFSProtocol::CreateHardLink:
            receivePath(path);
            receivePath(path2);
            disk->createHardLink(path, path2);
            writeVarUInt(RemoteFSProtocol::CreateHardLink, *out);
            out->next();
            break;
        case RemoteFSProtocol::TruncateFile:
            receivePath(path);
            readVarUInt(size_t_var, *in);
            disk->truncateFile(path, size_t_var);
            writeVarUInt(RemoteFSProtocol::TruncateFile, *out);
            out->next();
            break;
        default:
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown packet {} from client", toString(packet_type));
    }
}

void RemoteFSHandler::receivePath(std::string & path)
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
    for (auto iter = disk->iterateDirectory(path); iter->isValid(); iter->next())
    {
        LOG_TRACE(log, "Writing dir entry {}", iter->path());
        writeVarUInt(RemoteFSProtocol::DataPacket, *out);
        writeStringBinary(iter->path(), *out);
    }
    writeVarUInt(RemoteFSProtocol::EndIterateDirectory, *out);
    out->next();
}

void RemoteFSHandler::listFiles()
{
    std::string path;
    receivePath(path);
    std::vector<String> files;
    disk->listFiles(path, files);
    for (auto & file : files)
    {
        LOG_TRACE(log, "Writing file name {}", file);
        writeVarUInt(RemoteFSProtocol::DataPacket, *out);
        writeStringBinary(file, *out);
    }
    disk->listFiles(path, files);
    writeVarUInt(RemoteFSProtocol::EndListFiles, *out);
    out->next();
}

void RemoteFSHandler::readFile()
{
    std::string str_data;
    size_t offset;
    size_t size;
    receivePath(str_data);
    readVarUInt(offset, *in);
    LOG_TRACE(log, "Received offset {}", offset);
    readVarUInt(size, *in);
    LOG_TRACE(log, "Received size {}", size);

    auto read_buf = disk->readFile(str_data);
    read_buf->seek(offset, SEEK_SET);
    str_data.resize(size);
    auto bytes_read = read_buf->read(str_data.data(), size);
    str_data.resize(bytes_read);

    writeVarUInt(RemoteFSProtocol::ReadFile, *out);
    writeStringBinary(str_data, *out);
    out->next();
}

void RemoteFSHandler::writeFile()
{
    std::string str_data;
    size_t buf_size;
    uint mode_raw;
    receivePath(str_data);
    readVarUInt(buf_size, *in);
    LOG_TRACE(log, "Received buf_size {}", buf_size);
    readVarUInt(mode_raw, *in);
    WriteMode mode = WriteMode(mode_raw);
    LOG_TRACE(log, "Received mode {}", mode);

    auto write_buf = disk->writeFile(str_data, buf_size, mode);
    writeVarUInt(RemoteFSProtocol::WriteFile, *out);
    out->next();

    UInt64 packet_type;
    while (true)
    {
        readVarUInt(packet_type, *in);
        switch (packet_type)
        {
            case RemoteFSProtocol::DataPacket:
                readStringBinary(str_data, *in);
                LOG_TRACE(log, "Received data {}", str_data);
                writeString(str_data, *write_buf);
                write_buf->next(); // TODO maybe remove this line
                writeVarUInt(RemoteFSProtocol::DataPacket, *out);
                out->next();
                break;
            case RemoteFSProtocol::EndWriteFile:
                LOG_TRACE(log, "Close file");
                write_buf->sync();
                writeVarUInt(RemoteFSProtocol::EndWriteFile, *out);
                out->next();
                return;
            default:
                throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unknown packet {} from client", toString(packet_type));
        }
    }
}

void RemoteFSHandler::sendHello()
{
    writeVarUInt(RemoteFSProtocol::Hello, *out);
    out->next();
}


void RemoteFSHandler::sendException(const Exception & e)
{
    writeVarUInt(RemoteFSProtocol::Exception, *out);
    writeException(e, *out, false);
    out->next();
}

}
