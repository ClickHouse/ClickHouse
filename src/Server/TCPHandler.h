#pragma once

#include <memory>
#include <optional>
#include <Poco/Net/TCPServerConnection.h>

#include <Core/Protocol.h>
#include <Core/QueryProcessingStage.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/Progress.h>
#include <IO/ReadBufferFromPocoSocketChunked.h>
#include <IO/TimeoutSetter.h>
#include <IO/WriteBufferFromPocoSocketChunked.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/ProfileEventsExt.h>
#include <QueryPipeline/BlockIO.h>
#include <base/getFQDNOrHostName.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <IO/WriteBuffer.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Server/TCPProtocolStackData.h>
#include <Storages/MergeTree/RequestResponse.h>

#include "Client/IServerConnection.h"
#include "IServer.h"


namespace CurrentMetrics
{
    extern const Metric TCPConnection;
}

namespace Poco { class Logger; }

namespace DB
{

class Session;
struct Settings;
struct QueryPlanAndSets;
class ColumnsDescription;
struct ProfileInfo;
class TCPServer;
class NativeWriter;
class NativeReader;
struct ClusterFunctionReadTaskResponse;
using ClusterFunctionReadTaskResponsePtr = std::shared_ptr<ClusterFunctionReadTaskResponse>;

/// State of query processing.
struct QueryState
{
    /// Identifier of the query.
    String query_id;

    ContextMutablePtr query_context;

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
    std::shared_ptr<QueryPlanAndSets> plan_and_sets;
    /// Parsed query
    ASTPtr parsed_query;
    /// Streams of blocks, that are processing the query.
    BlockIO io;

    /// Is request cancelled
    bool allow_partial_result_on_first_cancel = false;
    bool stop_read_return_partial_result = false;
    bool stop_query = false;

    /// Data was sent.
    bool sent_all_data = false;
    /// Request requires data from the client (INSERT, but not INSERT SELECT).
    bool need_receive_data_for_insert = false;
    /// Data was read.
    bool read_all_data = true;

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
    bool query_duration_already_logged = false;

    ProfileEvents::ThreadIdToCountersSnapshot last_sent_snapshots;

    /// To output progress, the difference after the previous sending of progress.
    Progress progress;
    Stopwatch watch;
    UInt64 prev_elapsed_ns = 0;

    /// Timeouts setter for current query
    std::unique_ptr<TimeoutSetter> timeout_setter;

    void finalizeOut(std::shared_ptr<WriteBufferFromPocoSocketChunked> & raw_out) const
    {
        if (maybe_compressed_out && maybe_compressed_out.get() != raw_out.get())
            maybe_compressed_out->finalize();
        if (raw_out)
            raw_out->next();
    }

    void cancelOut(std::shared_ptr<WriteBufferFromPocoSocketChunked> & raw_out) const
    {
        if (maybe_compressed_out && maybe_compressed_out.get() != raw_out.get())
            maybe_compressed_out->cancel();
        if (raw_out)
            raw_out->cancel();
    }

    bool isEnanbledPartialResultOnFirstCancel() const;
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
    TCPHandler(
        IServer & server_,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        bool parse_proxy_protocol_,
        String server_display_name_,
        String host_name_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());
    TCPHandler(
        IServer & server_,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        TCPProtocolStackData & stack_data,
        String server_display_name_,
        String host_name_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());
    ~TCPHandler() override;

    void run() override;

    /// This method is called right before the query execution.
    virtual void customizeContext(ContextMutablePtr /*context*/) {}

private:
    IServer & server;
    TCPServer & tcp_server;
    bool parse_proxy_protocol = false;
    LoggerPtr log;

    String forwarded_for;
    String certificate;

    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_version_patch = 0;
    UInt32 client_tcp_protocol_version = 0;
    UInt32 client_parallel_replicas_protocol_version = 0;
    String proto_send_chunked_cl = "notchunked";
    String proto_recv_chunked_cl = "notchunked";
    String quota_key;

    /// Connection settings, which are extracted from a context.
    bool send_exception_with_stack_trace = true;
    Poco::Timespan send_timeout = Poco::Timespan(DBMS_DEFAULT_SEND_TIMEOUT_SEC, 0);
    Poco::Timespan receive_timeout = Poco::Timespan(DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, 0);
    UInt64 poll_interval = DBMS_DEFAULT_POLL_INTERVAL;
    UInt64 idle_connection_timeout = 3600;
    UInt64 interactive_delay = 100000;
    Poco::Timespan sleep_in_send_tables_status;
    UInt64 unknown_packet_in_send_data = 0;
    Poco::Timespan sleep_after_receiving_query;

    std::unique_ptr<Session> session;
    ClientInfo::QueryKind query_kind = ClientInfo::QueryKind::NO_QUERY;

    /// A state got uuids to exclude from a query
    std::optional<std::vector<UUID>> part_uuids_to_ignore;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBufferFromPocoSocketChunked> in;
    std::shared_ptr<WriteBufferFromPocoSocketChunked> out;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

    /// Time after the last check to stop the request and send the progress.
    Stopwatch after_check_cancelled;
    Stopwatch after_send_progress;

    String default_database;

    bool is_ssh_based_auth = false; /// authentication is via SSH pub-key challenge
    /// For inter-server secret (remote_server.*.secret)
    bool is_interserver_mode = false;
    bool is_interserver_authenticated = false;
    /// For DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET
    String salt;
    /// For DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2
    std::optional<UInt64> nonce;
    String cluster;

    /// `callback_mutex` protects using `out` (WriteBuffer), `in` (ReadBuffer) and other members concurrent inside callbacks.
    /// All the methods which are run inside callbacks are marked with TSA_REQUIRES.
    std::mutex callback_mutex;

    /// Last block input parameters are saved to be able to receive unexpected data packet sent after exception.
    LastBlockInputParameters last_block_in;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::TCPConnection};

    /// It is the name of the server that will be sent to the client.
    String server_display_name;
    String host_name;

    void runImpl();

    void extractConnectionSettingsFromContext(const ContextPtr & context);

    std::unique_ptr<Session> makeSession();

    void checkIfQueryCanceled(QueryState & state) TSA_REQUIRES(callback_mutex);

    bool receiveProxyHeader();
    void receiveHello();
    bool receiveQueryPlan(QueryState & state);
    void receiveAddendum();
    bool receivePacketsExpectQuery(std::optional<QueryState> & state);
    bool receivePacketsExpectData(QueryState & state) TSA_REQUIRES(callback_mutex);
    bool receivePacketsExpectDataConcurrentWithExecutor(QueryState & state);
    void receivePacketsExpectCancel(QueryState & state) TSA_REQUIRES(callback_mutex);

    ClusterFunctionReadTaskResponsePtr receiveClusterFunctionReadTaskResponse(QueryState & state) TSA_REQUIRES(callback_mutex);

    std::optional<ParallelReadResponse> receivePartitionMergeTreeReadTaskResponse(QueryState & state) TSA_REQUIRES(callback_mutex);

    void processCancel(QueryState & state, bool throw_exception = true) TSA_REQUIRES(callback_mutex);
    void processQuery(std::optional<QueryState> & state);
    void processIgnoredPartUUIDs();
    bool processData(QueryState & state, bool scalar) TSA_REQUIRES(callback_mutex);
    void processClusterNameAndSalt();

    void readTemporaryTables(QueryState & state) TSA_REQUIRES(callback_mutex);
    void skipData(QueryState & state) TSA_REQUIRES(callback_mutex);

    bool processUnexpectedData();
    [[noreturn]] void processUnexpectedQuery();
    [[noreturn]] void processUnexpectedIgnoredPartUUIDs();
    [[noreturn]] void processUnexpectedHello();
    [[noreturn]] void processUnexpectedTablesStatusRequest();

    /// Process INSERT query
    void startInsertQuery(QueryState & state);
    void processInsertQuery(QueryState & state);
    AsynchronousInsertQueue::PushResult processAsyncInsertQuery(QueryState & state, AsynchronousInsertQueue & insert_queue);

    /// Process a request that does not require the receiving of data blocks from the client
    void processOrdinaryQuery(QueryState & state);

    void processTablesStatusRequest();

    void sendHello();
    void sendData(QueryState & state, const Block & block); /// Write a block to the network.
    void sendLogData(QueryState & state, const Block & block);
    void sendTableColumns(QueryState & state, const ColumnsDescription & columns);
    void sendException(const Exception & e, bool with_stack_trace);
    void sendProgress(QueryState & state);
    void sendLogs(QueryState & state);
    void sendEndOfStream(QueryState & state);
    void sendPartUUIDs(QueryState & state);
    void sendReadTaskRequest() TSA_REQUIRES(callback_mutex);
    void sendMergeTreeAllRangesAnnouncement(QueryState & state, InitialAllRangesAnnouncement announcement) TSA_REQUIRES(callback_mutex);
    void sendMergeTreeReadTaskRequest(ParallelReadRequest request) TSA_REQUIRES(callback_mutex);
    void sendProfileInfo(QueryState & state, const ProfileInfo & info);
    void sendTotals(QueryState & state, const Block & totals);
    void sendExtremes(QueryState & state, const Block & extremes);
    void sendProfileEvents(QueryState & state);
    void sendSelectProfileEvents(QueryState & state);
    void sendInsertProfileEvents(QueryState & state);
    void sendTimezone(QueryState & state);

    /// Creates state.block_in/block_out for blocks read/write, depending on whether compression is enabled.
    void initBlockInput(QueryState & state);
    void initBlockOutput(QueryState & state, const Block & block);
    void initLogsBlockOutput(QueryState & state, const Block & block);
    void initProfileEventsBlockOutput(QueryState & state, const Block & block);
    static CompressionCodecPtr getCompressionCodec(const Settings & query_settings, Protocol::Compression compression);

    /// This function is called from different threads.
    void updateProgress(QueryState & state, const Progress & value);
    void logQueryDuration(QueryState & state);

    Poco::Net::SocketAddress getClientAddress(const ClientInfo & client_info);
};

}
