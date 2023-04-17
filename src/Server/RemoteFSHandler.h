#pragma once

#include <Poco/Net/TCPServerConnection.h>

#include <base/getFQDNOrHostName.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadStatus.h>
#include <Core/Protocol.h>
#include <Core/QueryProcessingStage.h>
#include <IO/Progress.h>
#include <IO/TimeoutSetter.h>
#include <QueryPipeline/BlockIO.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>

#include "IServer.h"
#include "Server/TCPProtocolStackData.h"
#include "Storages/MergeTree/RequestResponse.h"
#include "base/types.h"


namespace DB
{

class TCPServer;

class RemoteFSHandler : public Poco::Net::TCPServerConnection
{
public:
    RemoteFSHandler(
        IServer & server_,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        std::string server_display_name_);
    RemoteFSHandler(
        IServer & server_,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        TCPProtocolStackData & stack_data,
        std::string server_display_name_);
    ~RemoteFSHandler() override;

    void run() override;

private:
    IServer & server;
    TCPServer & tcp_server;
    Poco::Logger * log;

    String forwarded_for;
    String certificate;

    /// Connection settings, which are extracted from a context.
    bool send_exception_with_stack_trace = true;
    Poco::Timespan send_timeout = DBMS_DEFAULT_SEND_TIMEOUT_SEC;
    Poco::Timespan receive_timeout = DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC;
    UInt64 poll_interval = DBMS_DEFAULT_POLL_INTERVAL;
    UInt64 idle_connection_timeout = 3600;
    UInt64 interactive_delay = 100000;
    Poco::Timespan sleep_in_send_tables_status;
    UInt64 unknown_packet_in_send_data = 0;
    Poco::Timespan sleep_in_receive_cancel;
    Poco::Timespan sleep_after_receiving_query;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    /// It is the name of the server that will be sent to the client.
    String server_display_name;

    DiskPtr disk;

    void extractConnectionSettingsFromContext(const ContextPtr & context);

    void receiveHello();
    void receivePacket();
    void receivePath(std::string &path);
    [[noreturn]] void receiveUnexpectedHello();

    void iterateDirectory();
    void listFiles();
    void readFile();
    void writeFile();

    void sendHello();
    void sendError(std::string errorMsg);
};
}
