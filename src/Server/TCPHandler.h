#pragma once

#include <Poco/Net/TCPServerConnection.h>

#include <common/getFQDNOrHostName.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Core/Protocol.h>
#include <Core/QueryProcessingStage.h>
#include <IO/Progress.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Client/TimeoutSetter.h>

#include "IServer.h"


namespace CurrentMetrics
{
    extern const Metric TCPConnection;
}

namespace Poco { class Logger; }

namespace DB
{

class ColumnsDescription;

/// State of query processing.
struct QueryState
{
    /// Identifier of the query.
    String query_id;

    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete;
    Protocol::Compression compression = Protocol::Compression::Disable;

    /// A queue with internal logs that will be passed to client. It must be
    /// destroyed after input/output blocks, because they may contain other
    /// threads that use this queue.
    InternalTextLogsQueuePtr logs_queue;
    BlockOutputStreamPtr logs_block_out;

    /// From where to read data for INSERT.
    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    BlockInputStreamPtr block_in;

    /// Where to write result data.
    std::shared_ptr<WriteBuffer> maybe_compressed_out;
    BlockOutputStreamPtr block_out;

    /// Query text.
    String query;
    /// Streams of blocks, that are processing the query.
    BlockIO io;

    /// Is request cancelled
    bool is_cancelled = false;
    bool is_connection_closed = false;
    /// empty or not
    bool is_empty = true;
    /// Data was sent.
    bool sent_all_data = false;
    /// Request requires data from the client (INSERT, but not INSERT SELECT).
    bool need_receive_data_for_insert = false;
    /// Temporary tables read
    bool temporary_tables_read = false;

    /// Request requires data from client for function input()
    bool need_receive_data_for_input = false;
    /// temporary place for incoming data block for input()
    Block block_for_input;
    /// sample block from StorageInput
    Block input_header;

    /// To output progress, the difference after the previous sending of progress.
    Progress progress;

    /// Timeouts setter for current query
    std::unique_ptr<TimeoutSetter> timeout_setter;

    void reset()
    {
        *this = QueryState();
    }

    bool empty()
    {
        return is_empty;
    }
};


struct LastBlockInputParameters
{
    Protocol::Compression compression = Protocol::Compression::Disable;
    Block header;
};

class TCPHandler : public Poco::Net::TCPServerConnection
{
public:
    TCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_)
        : Poco::Net::TCPServerConnection(socket_)
        , server(server_)
        , log(&Poco::Logger::get("TCPHandler"))
        , connection_context(server.context())
        , query_context(server.context())
    {
        server_display_name = server.config().getString("display_name", getFQDNOrHostName());
    }

    void run() override;

    /// This method is called right before the query execution.
    virtual void customizeContext(DB::Context & /*context*/) {}

private:
    IServer & server;
    Poco::Logger * log;

    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_version_patch = 0;
    UInt64 client_tcp_protocol_version = 0;

    Context connection_context;
    std::optional<Context> query_context;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    /// Time after the last check to stop the request and send the progress.
    Stopwatch after_check_cancelled;
    Stopwatch after_send_progress;

    String default_database;

    /// For inter-server secret (remote_server.*.secret)
    String salt;
    String cluster;
    String cluster_secret;


    /// At the moment, only one ongoing query in the connection is supported at a time.
    QueryState state;

    /// Last block input parameters are saved to be able to receive unexpected data packet sent after exception.
    LastBlockInputParameters last_block_in;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TCPConnection};

    /// It is the name of the server that will be sent to the client.
    String server_display_name;

    void runImpl();

    void receiveHello();
    bool receivePacket();
    void receiveQuery();
    bool receiveData(bool scalar);
    bool readDataNext(const size_t & poll_interval, const int & receive_timeout);
    void readData(const Settings & connection_settings);
    std::tuple<size_t, int> getReadTimeouts(const Settings & connection_settings);

    [[noreturn]] void receiveUnexpectedData();
    [[noreturn]] void receiveUnexpectedQuery();
    [[noreturn]] void receiveUnexpectedHello();
    [[noreturn]] void receiveUnexpectedTablesStatusRequest();

    /// Process INSERT query
    void processInsertQuery(const Settings & connection_settings);

    /// Process a request that does not require the receiving of data blocks from the client
    void processOrdinaryQuery();

    void processOrdinaryQueryWithProcessors();

    void processTablesStatusRequest();

    void sendHello();
    void sendData(const Block & block);    /// Write a block to the network.
    void sendLogData(const Block & block);
    void sendTableColumns(const ColumnsDescription & columns);
    void sendException(const Exception & e, bool with_stack_trace);
    void sendProgress();
    void sendLogs();
    void sendEndOfStream();
    void sendProfileInfo(const BlockStreamProfileInfo & info);
    void sendTotals(const Block & totals);
    void sendExtremes(const Block & extremes);

    void receiveClusterNameAndSalt();

    /// Creates state.block_in/block_out for blocks read/write, depending on whether compression is enabled.
    void initBlockInput();
    void initBlockOutput(const Block & block);
    void initLogsBlockOutput(const Block & block);

    bool isQueryCancelled();

    /// This function is called from different threads.
    void updateProgress(const Progress & value);
};

}
