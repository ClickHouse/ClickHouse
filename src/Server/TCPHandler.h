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

#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

#include "IServer.h"
#include "base/types.h"


namespace CurrentMetrics
{
    extern const Metric TCPConnection;
}

namespace Poco { class Logger; }

namespace DB
{

class Session;
struct Settings;
class ColumnsDescription;
struct ProfileInfo;
class TCPServer;
class NativeWriter;
class NativeReader;

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
    std::unique_ptr<NativeWriter> logs_block_out;

    InternalProfileEventsQueuePtr profile_queue;
    std::unique_ptr<NativeWriter> profile_events_block_out;

    /// From where to read data for INSERT.
    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    std::unique_ptr<NativeReader> block_in;

    /// Where to write result data.
    std::shared_ptr<WriteBuffer> maybe_compressed_out;
    std::unique_ptr<NativeWriter> block_out;
    Block block_for_insert;

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
    /// Data was read.
    bool read_all_data = false;

    /// A state got uuids to exclude from a query
    std::optional<std::vector<UUID>> part_uuids_to_ignore;

    /// Request requires data from client for function input()
    bool need_receive_data_for_input = false;
    /// temporary place for incoming data block for input()
    Block block_for_input;
    /// sample block from StorageInput
    Block input_header;

    /// If true, the data packets will be skipped instead of reading. Used to recover after errors.
    bool skipping_data = false;

    /// To output progress, the difference after the previous sending of progress.
    Progress progress;
    Stopwatch watch;
    UInt64 prev_elapsed_ns = 0;

    /// Timeouts setter for current query
    std::unique_ptr<TimeoutSetter> timeout_setter;

    void reset()
    {
        *this = QueryState();
    }

    bool empty() const
    {
        return is_empty;
    }
};


struct LastBlockInputParameters
{
    Protocol::Compression compression = Protocol::Compression::Disable;
};

class TCPHandler : public Poco::Net::TCPServerConnection
{
public:
    /** parse_proxy_protocol_ - if true, expect and parse the header of PROXY protocol in every connection
      * and set the information about forwarded address accordingly.
      * See https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
      *
      * Note: immediate IP address is always used for access control (accept-list of IP networks),
      *  because it allows to check the IP ranges of the trusted proxy.
      * Proxy-forwarded (original client) IP address is used for quota accounting if quota is keyed by forwarded IP.
      */
    TCPHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, bool parse_proxy_protocol_, std::string server_display_name_);
    ~TCPHandler() override;

    void run() override;

    /// This method is called right before the query execution.
    virtual void customizeContext(ContextMutablePtr /*context*/) {}

private:
    IServer & server;
    TCPServer & tcp_server;
    bool parse_proxy_protocol = false;
    Poco::Logger * log;

    String forwarded_for;

    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_version_patch = 0;
    UInt64 client_tcp_protocol_version = 0;

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

    std::unique_ptr<Session> session;
    ContextMutablePtr query_context;
    ClientInfo::QueryKind query_kind = ClientInfo::QueryKind::NO_QUERY;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    /// Time after the last check to stop the request and send the progress.
    Stopwatch after_check_cancelled;
    Stopwatch after_send_progress;

    String default_database;

    /// For inter-server secret (remote_server.*.secret)
    bool is_interserver_mode = false;
    String salt;
    String cluster;

    std::mutex task_callback_mutex;
    std::mutex fatal_error_mutex;

    /// At the moment, only one ongoing query in the connection is supported at a time.
    QueryState state;

    /// Last block input parameters are saved to be able to receive unexpected data packet sent after exception.
    LastBlockInputParameters last_block_in;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TCPConnection};

    ProfileEvents::ThreadIdToCountersSnapshot last_sent_snapshots;

    /// It is the name of the server that will be sent to the client.
    String server_display_name;

    void runImpl();

    void extractConnectionSettingsFromContext(const ContextPtr & context);

    std::unique_ptr<Session> makeSession();

    bool receiveProxyHeader();
    void receiveHello();
    bool receivePacket();
    void receiveQuery();
    void receiveIgnoredPartUUIDs();
    String receiveReadTaskResponseAssumeLocked();
    std::optional<PartitionReadResponse> receivePartitionMergeTreeReadTaskResponseAssumeLocked();
    bool receiveData(bool scalar);
    bool readDataNext();
    void readData();
    void skipData();
    void receiveClusterNameAndSalt();

    bool receiveUnexpectedData(bool throw_exception = true);
    [[noreturn]] void receiveUnexpectedQuery();
    [[noreturn]] void receiveUnexpectedIgnoredPartUUIDs();
    [[noreturn]] void receiveUnexpectedHello();
    [[noreturn]] void receiveUnexpectedTablesStatusRequest();

    /// Process INSERT query
    void processInsertQuery();

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
    void sendPartUUIDs();
    void sendReadTaskRequestAssumeLocked();
    void sendMergeTreeReadTaskRequestAssumeLocked(PartitionReadRequest request);
    void sendProfileInfo(const ProfileInfo & info);
    void sendTotals(const Block & totals);
    void sendExtremes(const Block & extremes);
    void sendProfileEvents();
    void sendSelectProfileEvents();
    void sendInsertProfileEvents();

    /// Creates state.block_in/block_out for blocks read/write, depending on whether compression is enabled.
    void initBlockInput();
    void initBlockOutput(const Block & block);
    void initLogsBlockOutput(const Block & block);
    void initProfileEventsBlockOutput(const Block & block);

    bool isQueryCancelled();

    /// This function is called from different threads.
    void updateProgress(const Progress & value);
};

}
