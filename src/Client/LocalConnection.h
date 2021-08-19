#include "Connection.h"
#include <Interpreters/Context.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <IO/TimeoutSetter.h>


namespace DB
{

/// State of query processing.
struct LocalQueryState
{
    /// Identifier of the query.
    String query_id;

    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete;

    /// A queue with internal logs that will be passed to client. It must be
    /// destroyed after input/output blocks, because they may contain other
    /// threads that use this queue.
    InternalTextLogsQueuePtr logs_queue;
    BlockOutputStreamPtr logs_block_out;

    /// Query text.
    String query;
    /// Streams of blocks, that are processing the query.
    BlockIO io;
    /// Current stream to pull blocks from.
    std::unique_ptr<AsynchronousBlockInputStream> async_in;
    std::unique_ptr<PullingAsyncPipelineExecutor> executor;

    std::optional<Exception> exception;

    /// Last polled block.
    std::optional<Block> block;

    /// Is request cancelled
    bool is_cancelled = false;
    /// Is query finished == !has_pending_data
    bool is_finished = false;

    bool sent_totals = false;
    bool sent_extremes = false;

    /// Request requires data from the client (INSERT, but not INSERT SELECT).
    bool need_receive_data_for_insert = false;
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
};


class LocalConnection : public IServerConnection, WithContext
{
public:
    explicit LocalConnection(ContextPtr context_);
    void setDefaultDatabase(const String & database) override;

    void getServerVersion(const ConnectionTimeouts & timeouts,
                          String & name,
                          UInt64 & version_major,
                          UInt64 & version_minor,
                          UInt64 & version_patch,
                          UInt64 & revision) override;

    UInt64 getServerRevision(const ConnectionTimeouts & timeouts) override;

    const String & getServerTimezone(const ConnectionTimeouts & timeouts) override;
    const String & getServerDisplayName(const ConnectionTimeouts & timeouts) override;

    const String & getDescription() const override;

    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const String & query_id_ /* = "" */,
        UInt64 stage/* = QueryProcessingStage::Complete */,
        const Settings * settings /* = nullptr */,
        const ClientInfo * client_info /* = nullptr */,
        bool with_pending_data /* = false */) override;

    void sendCancel() override;

    void sendData(const Block &, const String &, bool) override;

    void sendExternalTablesData(ExternalTablesData &) override {}

    bool poll(size_t timeout_microseconds) override;

    bool hasReadPendingData() const override;

    std::optional<UInt64> checkPacket(size_t timeout_microseconds) override;

    Packet receivePacket() override;

    void forceConnected(const ConnectionTimeouts &) override {}

    bool isConnected() const override { return true; }

    bool checkConnected() override { return true; }

    void disconnect() override {}

    void setThrottler(const ThrottlerPtr &) override {}

private:
    ContextMutablePtr query_context;

    String description;

    String server_name;
    UInt64 server_version_major = 0;
    UInt64 server_version_minor = 0;
    UInt64 server_version_patch = 0;
    UInt64 server_revision = 0;
    String server_timezone;
    String server_display_name;
    String default_database;

    UInt64 interactive_delay = 100000;

    /// At the moment, only one ongoing query in the connection is supported at a time.
    std::optional<LocalQueryState> state;

    /// Last "server" packet.
    std::optional<UInt64> next_packet_type;

    /// Time after the last check to stop the request and send the progress.
    Stopwatch after_check_cancelled;
    Stopwatch after_send_progress;

    void initBlockInput();

    void processOrdinaryQuery();

    void processOrdinaryQueryWithProcessors();

    void updateState();

    bool pullBlock(Block & block);

    void finishQuery();

    void updateProgress(const Progress & value);

    void processInsertQuery();

    bool pollImpl();
};
}
