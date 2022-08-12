#pragma once

#include "Connection.h"
#include <Interpreters/Context.h>
#include <QueryPipeline/BlockIO.h>
#include <IO/TimeoutSetter.h>
#include <Interpreters/Session.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{
class PullingAsyncPipelineExecutor;
class PushingAsyncPipelineExecutor;
class PushingPipelineExecutor;

/// State of query processing.
struct LocalQueryState
{
    /// Identifier of the query.
    String query_id;
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete;

    /// Query text.
    String query;
    /// Streams of blocks, that are processing the query.
    BlockIO io;
    /// Current stream to pull blocks from.
    std::unique_ptr<PullingAsyncPipelineExecutor> executor;
    std::unique_ptr<PushingPipelineExecutor> pushing_executor;
    std::unique_ptr<PushingAsyncPipelineExecutor> pushing_async_executor;
    InternalProfileEventsQueuePtr profile_queue;

    std::unique_ptr<Exception> exception;

    /// Current block to be sent next.
    std::optional<Block> block;
    std::optional<ColumnsDescription> columns_description;
    std::optional<ProfileInfo> profile_info;

    /// Is request cancelled
    bool is_cancelled = false;
    bool is_finished = false;

    bool sent_totals = false;
    bool sent_extremes = false;
    bool sent_progress = false;
    bool sent_profile_info = false;
    bool sent_profile_events = false;

    /// To output progress, the difference after the previous sending of progress.
    Progress progress;
    /// Time after the last check to stop the request and send the progress.
    Stopwatch after_send_progress;
    Stopwatch after_send_profile_events;

    std::unique_ptr<CurrentThread::QueryScope> query_scope_holder;
};


class LocalConnection : public IServerConnection, WithContext
{
public:
    explicit LocalConnection(
        ContextPtr context_, bool send_progress_ = false, bool send_profile_events_ = false, const String & server_display_name_ = "");

    ~LocalConnection() override;

    IServerConnection::Type getConnectionType() const override { return IServerConnection::Type::LOCAL; }

    static ServerConnectionPtr createConnection(
        const ConnectionParameters & connection_parameters,
        ContextPtr current_context,
        bool send_progress = false,
        bool send_profile_events = false,
        const String & server_display_name = "");

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

    const String & getDescription() const override { return description; }

    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const String & query_id/* = "" */,
        UInt64 stage/* = QueryProcessingStage::Complete */,
        const Settings * settings/* = nullptr */,
        const ClientInfo * client_info/* = nullptr */,
        bool with_pending_data/* = false */,
        std::function<void(const Progress &)> process_progress_callback) override;

    void sendCancel() override;

    void sendData(const Block & block, const String & name/* = "" */, bool scalar/* = false */) override;

    void sendExternalTablesData(ExternalTablesData &) override;

    void sendMergeTreeReadTaskResponse(const PartitionReadResponse & response) override;

    bool poll(size_t timeout_microseconds/* = 0 */) override;

    bool hasReadPendingData() const override;

    std::optional<UInt64> checkPacket(size_t timeout_microseconds/* = 0*/) override;

    Packet receivePacket() override;

    void forceConnected(const ConnectionTimeouts &) override {}

    bool isConnected() const override { return true; }

    bool checkConnected(const ConnectionTimeouts & /*timeouts*/) override { return true; }

    void disconnect() override {}

    void setThrottler(const ThrottlerPtr &) override {}

private:
    void initBlockInput();

    void processOrdinaryQuery();

    void processOrdinaryQueryWithProcessors();

    void updateState();

    bool pullBlock(Block & block);

    void finishQuery();

    void updateProgress(const Progress & value);

    void sendProfileEvents();

    bool pollImpl();

    ContextMutablePtr query_context;
    Session session;

    bool send_progress;
    bool send_profile_events;
    String server_display_name;
    String description = "clickhouse-local";

    std::optional<LocalQueryState> state;
    std::optional<ThreadStatus> thread_status;

    /// Last "server" packet.
    std::optional<UInt64> next_packet_type;

    String current_database;

    ProfileEvents::ThreadIdToCountersSnapshot last_sent_snapshots;
};
}
