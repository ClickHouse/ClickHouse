#pragma once

#include "Connection.h"
#include <Interpreters/Context.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <IO/TimeoutSetter.h>
#include <Interpreters/Session.h>


namespace DB
{


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
    std::unique_ptr<AsynchronousBlockInputStream> async_in;
    std::unique_ptr<PullingAsyncPipelineExecutor> executor;

    std::optional<Exception> exception;

    /// Current block to be sent next.
    std::optional<Block> block;

    /// Is request cancelled
    bool is_cancelled = false;
    /// Is query finished == !has_pending_data
    bool is_finished = false;

    bool sent_totals = false;
    bool sent_extremes = false;
    bool sent_progress = false;

    /// To output progress, the difference after the previous sending of progress.
    Progress progress;
    /// Time after the last check to stop the request and send the progress.
    Stopwatch after_send_progress;
    Stopwatch query_execution_time;
};


class LocalConnection : public IServerConnection, WithContext
{
public:
    explicit LocalConnection(ContextPtr context_);
    ~LocalConnection() override;

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
        const String & query_id_ = "",
        UInt64 stage = QueryProcessingStage::Complete,
        const Settings * settings = nullptr,
        const ClientInfo * client_info = nullptr,
        bool with_pending_data = false) override;

    void sendCancel() override;

    void sendData(const Block &, const String &, bool) override;

    void sendExternalTablesData(ExternalTablesData &) override {}

    bool poll(size_t timeout_microseconds = 0) override;

    bool hasReadPendingData() const override;

    std::optional<UInt64> checkPacket(size_t timeout_microseconds = 0) override;

    Packet receivePacket() override;

    void forceConnected(const ConnectionTimeouts &) override {}

    bool isConnected() const override { return true; }

    bool checkConnected() override { return true; }

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

    bool pollImpl();

    ContextMutablePtr query_context;
    Session session;

    /// At the moment, only one ongoing query in the connection is supported at a time.
    std::optional<LocalQueryState> state;

    /// Last "server" packet.
    std::optional<UInt64> next_packet_type;

    String description = "clickhouse-local";
    UInt64 server_revision = 0;
    String server_timezone;
    String server_display_name;
    String default_database;
};
}
