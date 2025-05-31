#include <algorithm>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>
#include <Access/AccessControl.h>
#include <Access/Credentials.h>
#include <Common/DateLUTImpl.h>
#include <Common/VersionNumber.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ExternalTable.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Formats/FormatFactory.h>
#include <IO/LimitReadBuffer.h>
#include <IO/Progress.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/Session.h>
#include <Interpreters/Squashing.h>
#include <Interpreters/TablesStatus.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Server/TCPServer.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/ObjectStorage/StorageObjectStorageCluster.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/NetException.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <base/defines.h>
#include <base/scope_guard.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/QueryPlan/QueryPlan.h>

#if USE_SSL
#    include <Poco/Net/SecureStreamSocket.h>
#    include <Poco/Net/SecureStreamSocketImpl.h>
#    include <Common/Crypto/X509Certificate.h>
#endif

#include <Core/Protocol.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Interpreters/ClientInfo.h>

#include "TCPHandler.h"

#include <Common/config_version.h>

#include <fmt/format.h>

#include <fmt/ostream.h>
#include <Common/StringUtils.h>

using namespace std::literals;
using namespace DB;

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_experimental_codecs;
    extern const SettingsBool allow_experimental_query_deduplication;
    extern const SettingsBool allow_suspicious_codecs;
    extern const SettingsBool async_insert;
    extern const SettingsUInt64 async_insert_max_data_size;
    extern const SettingsBool calculate_text_stack_trace;
    extern const SettingsBool deduplicate_blocks_in_dependent_materialized_views;
    extern const SettingsBool enable_deflate_qpl_codec;
    extern const SettingsBool enable_zstd_qat_codec;
    extern const SettingsUInt64 idle_connection_timeout;
    extern const SettingsBool input_format_defaults_for_omitted_fields;
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsBool low_cardinality_allow_in_native_format;
    extern const SettingsString network_compression_method;
    extern const SettingsInt64 network_zstd_compression_level;
    extern const SettingsBool partial_result_on_first_cancel;
    extern const SettingsUInt64 poll_interval;
    extern const SettingsSeconds receive_timeout;
    extern const SettingsLogsLevel send_logs_level;
    extern const SettingsString send_logs_source_regexp;
    extern const SettingsSeconds send_timeout;
    extern const SettingsTimezone session_timezone;
    extern const SettingsMilliseconds sleep_after_receiving_query_ms;
    extern const SettingsMilliseconds sleep_in_send_data_ms;
    extern const SettingsMilliseconds sleep_in_send_tables_status_ms;
    extern const SettingsBool throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert;
    extern const SettingsUInt64 unknown_packet_in_send_data;
    extern const SettingsBool wait_for_async_insert;
    extern const SettingsSeconds wait_for_async_insert_timeout;
    extern const SettingsBool use_concurrency_control;
    extern const SettingsBool apply_settings_from_server;
}

namespace ServerSetting
{
    extern const ServerSettingsBool validate_tcp_client_information;
    extern const ServerSettingsBool process_query_plan_packet;
}
}

namespace CurrentMetrics
{
    extern const Metric QueryThread;
    extern const Metric ReadTaskRequestsSent;
    extern const Metric MergeTreeReadTaskRequestsSent;
    extern const Metric MergeTreeAllRangesAnnouncementsSent;
}

namespace ProfileEvents
{
    extern const Event ReadTaskRequestsSent;
    extern const Event MergeTreeReadTaskRequestsSent;
    extern const Event MergeTreeAllRangesAnnouncementsSent;
    extern const Event ReadTaskRequestsSentElapsedMicroseconds;
    extern const Event MergeTreeReadTaskRequestsSentElapsedMicroseconds;
    extern const Event MergeTreeAllRangesAnnouncementsSentElapsedMicroseconds;
}

namespace DB::ErrorCodes
{
    extern const int ABORTED;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int AUTHENTICATION_FAILED;
    extern const int CLIENT_HAS_CONNECTED_TO_WRONG_PORT;
    extern const int CLIENT_INFO_DOES_NOT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_PROTOCOL;
    extern const int UNSUPPORTED_METHOD;
    extern const int USER_EXPIRED;
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_TABLE;

    // We have to distinguish the case when query is killed by `KILL QUERY` statement
    // and when it is killed by `Protocol::Client::Cancel` packet.

    // When query is killed by `KILL QUERY` statement we have to end the execution
    // and send the exception to the actual client which initiated the TCP connection.

    // When query is killed by `Protocol::Client::Cancel` packet we just stop execution,
    // there is no need to send the exception which has been caused by the cancel packet.
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
}

namespace
{
// This function corrects the wrong client_name from the old client.
// Old clients 28.7 and some intermediate versions of 28.7 were sending different ClientInfo.client_name
// "ClickHouse client" was sent with the hello message.
// "ClickHouse" or "ClickHouse " was sent with the query message.
void correctQueryClientInfo(const ClientInfo & session_client_info, ClientInfo & client_info)
{
    if (VersionNumber(client_info.client_version_major, client_info.client_version_minor, client_info.client_version_patch)
            <= VersionNumber(23, 8, 1)
        && session_client_info.client_name == "ClickHouse client"
        && (client_info.client_name == "ClickHouse" || client_info.client_name == "ClickHouse "))
    {
        client_info.client_name = "ClickHouse client";
    }
}

void validateClientInfo(const ClientInfo & session_client_info, const ClientInfo & client_info)
{
    // Secondary query may contain different client_info.
    // In the case of select from distributed table or 'select * from remote' from non-tcp handler. Server sends the initial client_info data.
    //
    // Example 1: curl -q -s --max-time 60 -sS "http://127.0.0.1:8123/?" -d "SELECT 1 FROM remote('127.0.0.1', system.one)"
    // HTTP handler initiates TCP connection with remote 127.0.0.1 (session on remote 127.0.0.1 use TCP interface)
    // HTTP handler sends client_info with HTTP interface and HTTP data by TCP protocol in Protocol::Client::Query message.
    //
    // Example 2: select * from <distributed_table>  --host shard_1 // distributed table has 2 shards: shard_1, shard_2
    // shard_1 receives a message with 'ClickHouse client' client_name
    // shard_1 initiates TCP connection with shard_2 with 'ClickHouse server' client_name.
    // shard_1 sends 'ClickHouse client' client_name in Protocol::Client::Query message to shard_2.
    if (client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        return;

    if (session_client_info.interface != client_info.interface)
    {
        throw Exception(
            DB::ErrorCodes::CLIENT_INFO_DOES_NOT_MATCH,
            "Client info's interface does not match: {} not equal to {}",
            toString(session_client_info.interface),
            toString(client_info.interface));
    }

    if (session_client_info.interface == ClientInfo::Interface::TCP)
    {
        if (session_client_info.client_name != client_info.client_name)
            throw Exception(
                DB::ErrorCodes::CLIENT_INFO_DOES_NOT_MATCH,
                "Client info's client_name does not match: {} not equal to {}",
                session_client_info.client_name,
                client_info.client_name);

        // TCP handler got patch version 0 always for backward compatibility.
        if (!session_client_info.clientVersionEquals(client_info, false))
            throw Exception(
                DB::ErrorCodes::CLIENT_INFO_DOES_NOT_MATCH,
                "Client info's version does not match: {} not equal to {}",
                session_client_info.getVersionStr(),
                client_info.getVersionStr());

        // os_user, quota_key, client_trace_context can be different.
    }
}
struct TurnOffBoolSettingTemporary
{
    bool & setting;
    bool prev_val;

    explicit TurnOffBoolSettingTemporary(bool & setting_)
        : setting(setting_)
        , prev_val(setting_)
    {
        if (prev_val)
            setting = false;
    }

    ~TurnOffBoolSettingTemporary()
    {
        if (prev_val)
            setting = true;
    }
};

}

namespace DB
{

TCPHandler::TCPHandler(
    IServer & server_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    bool parse_proxy_protocol_,
    std::string server_display_name_,
    std::string host_name_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , parse_proxy_protocol(parse_proxy_protocol_)
    , log(getLogger("TCPHandler"))
    , read_event(read_event_)
    , write_event(write_event_)
    , server_display_name(std::move(server_display_name_))
    , host_name(std::move(host_name_))
{
}

TCPHandler::TCPHandler(
    IServer & server_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    TCPProtocolStackData & stack_data,
    std::string server_display_name_,
    std::string host_name_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , log(getLogger("TCPHandler"))
    , forwarded_for(stack_data.forwarded_for)
    , certificate(stack_data.certificate)
    , read_event(read_event_)
    , write_event(write_event_)
    , default_database(stack_data.default_database)
    , server_display_name(std::move(server_display_name_))
    , host_name(std::move(host_name_))
{
    if (!forwarded_for.empty())
        LOG_TRACE(log, "Forwarded client address: {}", forwarded_for);
}


TCPHandler::~TCPHandler() = default;


void TCPHandler::runImpl()
{
    setThreadName("TCPHandler");

    extractConnectionSettingsFromContext(server.context());

    socket().setReceiveTimeout(receive_timeout);
    socket().setSendTimeout(send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocketChunked>(socket(), read_event);

    /// Support for PROXY protocol
    if (parse_proxy_protocol && !receiveProxyHeader())
        return;

    if (in->eof())
    {
        LOG_INFO(log, "Client has not sent any data.");
        return;
    }

    out = std::make_shared<AutoCanceledWriteBuffer<WriteBufferFromPocoSocketChunked>>(socket(), write_event);

    /// User will be authenticated here. It will also set settings from user profile into connection_context.
    try
    {
        receiveHello();

        if (!default_database.empty())
            DatabaseCatalog::instance().assertDatabaseExists(default_database);

        /// In interserver mode queries are executed without a session context.
        if (!is_interserver_mode)
            session->makeSessionContext();

        sendHello();

        if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM)
            receiveAddendum();

        {
            /// Server side of chunked protocol negotiation.
            /// Server advertises its protocol capabilities (separate for send and receive channels) by sending
            /// in its 'Hello' response one of four types - chunked, notchunked, chunked_optional, notchunked_optional.
            /// Not optional types are strict meaning that server only supports this type, optional means that
            /// server prefer this type but capable to work in opposite.
            /// Client selects which type it is going to communicate based on the settings from config or arguments,
            /// and sends either "chunked" or "notchunked" protocol request in addendum section of handshake.
            /// Client can detect if server's protocol capabilities are not compatible with client's settings (for example
            /// server strictly requires chunked protocol but client's settings only allows notchunked protocol) - in such case
            /// client should interrupt this connection. However if client continues with incompatible protocol type request, server
            /// will send appropriate exception and disconnect client.

            auto is_chunked = [](const String & chunked_srv_str, const String & chunked_cl_str, const String & direction)
            {
                bool chunked_srv = chunked_srv_str.starts_with("chunked");
                bool optional_srv = chunked_srv_str.ends_with("_optional");
                bool chunked_cl = chunked_cl_str.starts_with("chunked");

                if (optional_srv)
                    return chunked_cl;

                if (chunked_cl != chunked_srv)
                    throw NetException(
                        ErrorCodes::NETWORK_ERROR,
                        "Incompatible protocol: {} is {}, client requested {}",
                        direction,
                        chunked_srv ? "chunked" : "notchunked",
                        chunked_cl ? "chunked" : "notchunked");

                return chunked_srv;
            };

            bool out_chunked = is_chunked(server.config().getString("proto_caps.send", "notchunked"), proto_recv_chunked_cl, "send");
            bool in_chunked = is_chunked(server.config().getString("proto_caps.recv", "notchunked"), proto_send_chunked_cl, "recv");

            if (out_chunked)
                out->enableChunked();
            if (in_chunked)
                in->enableChunked();
        }

        if (!is_interserver_mode)
        {
            /// If session created, then settings in session context has been updated.
            /// So it's better to update the connection settings for flexibility.
            extractConnectionSettingsFromContext(session->sessionContext());

            /// When connecting, the default database could be specified.
            if (!default_database.empty())
                session->sessionContext()->setCurrentDatabase(default_database);
        }
    }
    catch (const Exception & e) /// Typical for an incorrect username, password, or address.
    {
        if (e.code() == ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT)
        {
            LOG_DEBUG(log, "Client has connected to wrong port.");
            return;
        }

        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
        {
            LOG_INFO(log, "Client has gone away.");
            return;
        }

        try
        {
            /// We try to send error information to the client.
            sendException(e, send_exception_with_stack_trace);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        throw;
    }

    while (tcp_server.isOpen())
    {
        /// We don't really have session in interserver mode, new one is created for each query. It's better to reset it now.
        if (is_interserver_mode)
            session.reset();

        /// We are waiting for a packet from the client. Thus, every `poll_interval` seconds check whether we need to shut down.
        {
            Stopwatch idle_time;
            UInt64 timeout_ms = std::min(poll_interval, idle_connection_timeout) * 1000000;

            while (tcp_server.isOpen() && !server.isCancelled() && !in->poll(timeout_ms))
            {
                if (idle_time.elapsedSeconds() > idle_connection_timeout)
                {
                    LOG_TRACE(log, "Closing idle connection");
                    return;
                }
            }

            /// If we need to shut down, or client disconnects.
            if (!tcp_server.isOpen() || server.isCancelled() || in->eof())
            {
                LOG_TEST(log, "Closing connection (open: {}, cancelled: {}, eof: {})", tcp_server.isOpen(), server.isCancelled(), in->eof());
                return;
            }
        }

        /** An exception during the execution of request (it must be sent over the network to the client).
         *  The client will be able to accept it, if it did not happen while sending another packet and the client has not disconnected yet.
         */
        std::unique_ptr<DB::Exception> exception;

        SCOPE_EXIT({
            if (exception)
            {
                if (exception->code() == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT)
                    LOG_INFO(log, getExceptionMessageAndPattern(*exception, send_exception_with_stack_trace));
                else
                    LOG_ERROR(log, getExceptionMessageAndPattern(*exception, send_exception_with_stack_trace));
            }
        });

        OpenTelemetry::TracingContextHolderPtr thread_trace_context;
        /// Initialized later. It has to be destroyed after query_state is destroyed.
        std::optional<CurrentThread::QueryScope> query_scope;
        /// QueryState should be cleared before QueryScope, since otherwise
        /// the MemoryTracker will be wrong for possible deallocations.
        /// (i.e. deallocations from the Aggregator with two-level aggregation)
        /// Also it resets socket's timeouts.
        std::optional<QueryState> query_state;

        try
        {
            /** If Query - process it. If Ping or Cancel - go back to the beginning.
            *  There may come settings for a separate query that modify `query_context`.
            *  It's possible to receive part uuids packet before the query, so then receivePacket has to be called twice.
            */
            if (!receivePacketsExpectQuery(query_state))
                continue;

            /** If part_uuids got received in previous packet, trying to read again.
            */
            if (part_uuids_to_ignore.has_value() && !receivePacketsExpectQuery(query_state))
                continue;

            chassert(query_state.has_value());

            /// Set up tracing context for this query on current thread
            thread_trace_context = std::make_unique<OpenTelemetry::TracingContextHolder>("TCPHandler",
                query_state->query_context->getClientInfo().client_trace_context,
                query_state->query_context->getSettingsRef(),
                query_state->query_context->getOpenTelemetrySpanLog());
            thread_trace_context->root_span.kind = OpenTelemetry::SpanKind::SERVER;
            thread_trace_context->root_span.addAttribute("client.version", query_state->query_context->getClientInfo().getVersionStr());

            query_scope.emplace(query_state->query_context, /* fatal_error_callback */ [this, &query_state]
            {
                std::lock_guard lock(callback_mutex);
                sendLogs(query_state.value());
            });

            /// If query received, then settings in query_context has been updated.
            /// So it's better to update the connection settings for flexibility.
            extractConnectionSettingsFromContext(query_state->query_context);

            /// Sync timeouts on client and server during current query to avoid dangling queries on server.
            /// It should be reset at the end of query.
            query_state->timeout_setter = std::make_unique<TimeoutSetter>(socket(), send_timeout, receive_timeout);

            SCOPE_EXIT(logQueryDuration(query_state.value()));

            /// Should we send internal logs to client?
            const auto client_logs_level = query_state->query_context->getSettingsRef()[Setting::send_logs_level];
            if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_LOGS
                && client_logs_level != LogsLevel::none)
            {
                query_state->logs_queue = std::make_shared<InternalTextLogsQueue>();
                query_state->logs_queue->max_priority = Poco::Logger::parseLevel(client_logs_level.toString());
                query_state->logs_queue->setSourceRegexp(query_state->query_context->getSettingsRef()[Setting::send_logs_source_regexp]);
                CurrentThread::attachInternalTextLogsQueue(query_state->logs_queue, client_logs_level);
            }

            if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS)
            {
                query_state->profile_queue = std::make_shared<InternalProfileEventsQueue>(std::numeric_limits<int>::max());
                CurrentThread::attachInternalProfileEventsQueue(query_state->profile_queue);
            }

            if (!is_interserver_mode)
                session->checkIfUserIsStillValid();

            if (query_state->stage == QueryProcessingStage::QueryPlan)
            {
                if (!session->globalContext()->getServerSettings()[ServerSetting::process_query_plan_packet])
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "Reading of QueryPlan packet is disabled. "
                        "Enable process_query_plan_packet in server config or disable serialize_query_plan setting.");

                query_state->query_context->setQueryPlanDeserializationCallback([&query_state]()
                {
                    if (!query_state->plan_and_sets)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected query plan packet for QueryPlan stage");

                    return query_state->plan_and_sets;
                });
            }

            query_state->query_context->setExternalTablesInitializer([this, &query_state] (ContextPtr context)
            {
                if (context != query_state->query_context)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in external tables initializer");

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(query_state.value());

                /// Get blocks of temporary tables
                readTemporaryTables(query_state.value());

                /// Reset the input stream, as we received an empty block while receiving external table data.
                /// So, the stream has been marked as cancelled and we can't read from it anymore.
                query_state->block_in.reset();
                query_state->maybe_compressed_in.reset(); /// For more accurate accounting by MemoryTracker.
            });

            /// Send structure of columns to client for function input()
            query_state->query_context->setInputInitializer([this, &query_state] (ContextPtr context, const StoragePtr & input_storage)
            {

                if (context != query_state->query_context)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in Input initializer");

                auto metadata_snapshot = input_storage->getInMemoryMetadataPtr();

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(query_state.value());

                query_state->need_receive_data_for_input = true;

                /// Send ColumnsDescription for input storage.
                if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA
                    && query_state->query_context->getSettingsRef()[Setting::input_format_defaults_for_omitted_fields])
                {
                    sendTableColumns(query_state.value(), metadata_snapshot->getColumns());
                }

                /// Send block to the client - input storage structure.
                query_state->input_header = metadata_snapshot->getSampleBlock();
                sendData(query_state.value(), query_state->input_header);
                sendTimezone(query_state.value());

                /// Update flag after reading external tables
                query_state->read_all_data = false;
            });

            query_state->query_context->setInputBlocksReaderCallback([this, &query_state] (ContextPtr context) -> Block
            {
                if (context != query_state->query_context)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in InputBlocksReader");

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(query_state.value());

                if (receivePacketsExpectData(query_state.value()))
                    return query_state->block_for_input;

                query_state->block_in.reset();
                query_state->maybe_compressed_in.reset();
                return {};
            });

            customizeContext(query_state->query_context);

            /// This callback is needed for requesting read tasks inside pipeline for distributed processing
            query_state->query_context->setReadTaskCallback([this, &query_state]() -> String
            {
                Stopwatch watch;
                CurrentMetrics::Increment callback_metric_increment(CurrentMetrics::ReadTaskRequestsSent);

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(query_state.value());

                sendReadTaskRequest();

                ProfileEvents::increment(ProfileEvents::ReadTaskRequestsSent);
                auto res = receiveReadTaskResponse(query_state.value());
                ProfileEvents::increment(ProfileEvents::ReadTaskRequestsSentElapsedMicroseconds, watch.elapsedMicroseconds());

                return res;
            });

            query_state->query_context->setMergeTreeAllRangesCallback([this, &query_state](InitialAllRangesAnnouncement announcement)
            {
                Stopwatch watch;
                CurrentMetrics::Increment callback_metric_increment(CurrentMetrics::MergeTreeAllRangesAnnouncementsSent);

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(query_state.value());

                sendMergeTreeAllRangesAnnouncement(query_state.value(), announcement);
                ProfileEvents::increment(ProfileEvents::MergeTreeAllRangesAnnouncementsSent);
                ProfileEvents::increment(ProfileEvents::MergeTreeAllRangesAnnouncementsSentElapsedMicroseconds, watch.elapsedMicroseconds());
            });

            query_state->query_context->setMergeTreeReadTaskCallback([this, &query_state](ParallelReadRequest request) -> std::optional<ParallelReadResponse>
            {
                Stopwatch watch;
                CurrentMetrics::Increment callback_metric_increment(CurrentMetrics::MergeTreeReadTaskRequestsSent);

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(*query_state);

                sendMergeTreeReadTaskRequest(std::move(request));

                ProfileEvents::increment(ProfileEvents::MergeTreeReadTaskRequestsSent);
                auto res = receivePartitionMergeTreeReadTaskResponse(query_state.value());
                ProfileEvents::increment(ProfileEvents::MergeTreeReadTaskRequestsSentElapsedMicroseconds, watch.elapsedMicroseconds());
                return res;
            });

            /// Processing Query
            std::tie(query_state->parsed_query, query_state->io) = executeQuery(query_state->query, query_state->query_context, QueryFlags{}, query_state->stage);

            after_check_cancelled.restart();
            after_send_progress.restart();

            if (query_state->io.pipeline.pushing())
            {
                /// FIXME: check explicitly that insert query suggests to receive data via native protocol,
                query_state->need_receive_data_for_insert = true;
                processInsertQuery(query_state.value());
                query_state->io.onFinish();
            }
            else if (query_state->io.pipeline.pulling())
            {
                processOrdinaryQuery(query_state.value());
                query_state->io.onFinish();
            }
            else if (query_state->io.pipeline.completed())
            {
                {
                    CompletedPipelineExecutor executor(query_state->io.pipeline);

                    /// Should not check for cancel in case of input.
                    if (!query_state->need_receive_data_for_input)
                    {
                        auto callback = [this, &query_state]()
                        {
                            std::lock_guard lock(callback_mutex);

                            receivePacketsExpectCancel(query_state.value());

                            if (query_state->stop_read_return_partial_result)
                                return true;

                            sendProgress(query_state.value());
                            sendSelectProfileEvents(query_state.value());
                            sendLogs(query_state.value());
                            return false;
                        };

                        executor.setCancelCallback(std::move(callback), interactive_delay / 1000);
                    }

                    executor.execute();
                }

                query_state->io.onFinish();

                /// Send final progress after calling onFinish(), since it will update the progress.
                ///
                /// NOTE: we cannot send Progress for regular INSERT (with VALUES)
                /// without breaking protocol compatibility, but it can be done
                /// by increasing revision.
                sendProgress(query_state.value());
                sendSelectProfileEvents(query_state.value());
            }
            else
            {
                query_state->io.onFinish();
            }

            /// Do it before sending end of stream, to have a chance to show log message in client.
            query_scope->logPeakMemoryUsage();

            sendLogs(query_state.value());
            sendEndOfStream(query_state.value());

            query_state->finalizeOut(out);
        }
        catch (const Exception & e)
        {
            exception.reset(e.clone());
        }
        catch (const Poco::Exception & e)
        {
            exception = std::make_unique<DB::Exception>(Exception::CreateFromPocoTag{}, e);
        }
// Server should die on std logic errors in debug, like with assert()
// or ErrorCodes::LOGICAL_ERROR. This helps catch these errors in tests.
#ifdef DEBUG_OR_SANITIZER_BUILD
        catch (const std::logic_error & e)
        {
            if (query_state.has_value())
                query_state->io.onException();
            exception = std::make_unique<DB::Exception>(Exception::CreateFromSTDTag{}, e);
            sendException(*exception, send_exception_with_stack_trace);
            std::abort();
        }
#endif
        catch (const std::exception & e)
        {
            exception = std::make_unique<DB::Exception>(Exception::CreateFromSTDTag{}, e);
        }
        catch (...)
        {
            exception = std::make_unique<DB::Exception>(getCurrentExceptionMessageAndPattern(false), ErrorCodes::UNKNOWN_EXCEPTION);
        }

        if (exception)
        {
            auto exception_code = exception->code();

            if (!query_state.has_value())
                return;

            try
            {
                exception->rethrow();
            }
            catch (...)
            {
                query_state->io.onException(exception_code != ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT);
            }

             /// Authentication failure with interserver secret
            /// - early exit without trying to send the exception to the client.
            /// Because the server should not try to skip (parse, decompress) the remaining packets sent by the client,
            /// as it will lead to additional work and unneeded exposure to unauthenticated connections.

            /// Note that the exception AUTHENTICATION_FAILED can be here in two cases:
            /// 1. The authentication in receiveHello is skipped with "interserver secret",
            /// postponed to receiving the query, and then failed.
            /// 2. Receiving exception from a query using a table function to authenticate with another server.
            /// In this case, the user is already authenticated with this server,
            /// is_interserver_mode is false, and we can send the exception to the client normally.

            if (is_interserver_mode && !is_interserver_authenticated)
            {
                /// Interserver authentication is done only after we read the query.
                /// This fact can be abused by producing exception before or while we read the query.
                /// To avoid any potential exploits, we simply close connection on any exceptions
                /// that happen before the first query is authenticated with the cluster secret.
                query_state->cancelOut(out);
                return;
            }

            if (exception_code == ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT)
            {
                query_state->cancelOut(out);
                return;
            }

            if (thread_trace_context)
                thread_trace_context->root_span.addAttribute(*exception);

            if (!out || out->isCanceled())
            {
                query_state->cancelOut(out);
                return;
            }

            try
            {
                std::lock_guard lock(callback_mutex);

                /// Try to send logs to client, but it could be risky too
                /// Assume that we can't break output here
                sendLogs(query_state.value());

                if (exception_code == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT)
                    sendEndOfStream(query_state.value());
                else
                    sendException(*exception, send_exception_with_stack_trace);

                /// A query packet is always followed by one or more data packets.
                /// If some of those data packets are left, try to skip them.
                if (!query_state->read_all_data)
                    skipData(query_state.value());

                LOG_TRACE(log, "Logs and exception has been sent. The connection is preserved.");
            }
            catch (...)
            {
                query_state->cancelOut(out);
                tryLogCurrentException(log, "Can't send logs or exception to client. Close connection.");
                return;
            }

            if (exception->code() == ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT
                || exception->code() == ErrorCodes::USER_EXPIRED)
            {
                LOG_DEBUG(log, "Going to close connection due to exception: {}", exception->message());
                query_state->finalizeOut(out);
                return;
            }
        }

        query_state->finalizeOut(out);
    }
}


void TCPHandler::logQueryDuration(QueryState & state)
{
    if (state.query_duration_already_logged)
        return;
    state.query_duration_already_logged = true;
    auto elapsed_sec = state.watch.elapsedSeconds();
    /// We already logged more detailed info if we read some rows
    if (elapsed_sec < 1.0 && state.progress.read_rows)
        return;
    LOG_DEBUG(log, "Processed in {} sec.", elapsed_sec);
}


void TCPHandler::extractConnectionSettingsFromContext(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    send_exception_with_stack_trace = settings[Setting::calculate_text_stack_trace];
    send_timeout = settings[Setting::send_timeout];
    receive_timeout = settings[Setting::receive_timeout];
    poll_interval = settings[Setting::poll_interval];
    idle_connection_timeout = settings[Setting::idle_connection_timeout];
    interactive_delay = settings[Setting::interactive_delay];
    sleep_in_send_tables_status = settings[Setting::sleep_in_send_tables_status_ms];
    unknown_packet_in_send_data = settings[Setting::unknown_packet_in_send_data];
    sleep_after_receiving_query = settings[Setting::sleep_after_receiving_query_ms];
}


bool TCPHandler::receivePacketsExpectQuery(std::optional<QueryState> & state)
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);

    switch (packet_type)
    {
        case Protocol::Client::Hello:
            processUnexpectedHello();

        case Protocol::Client::Data:
        case Protocol::Client::Scalar:
            processUnexpectedData();
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Data received from client");

        case Protocol::Client::Ping:
            writeVarUInt(Protocol::Server::Pong, *out);
            out->finishChunk();
            out->next();
            return false;

        case Protocol::Client::Cancel:
            return false;

        case Protocol::Client::TablesStatusRequest:
            processTablesStatusRequest();
            return false;

        case Protocol::Client::IgnoredPartUUIDs:
            /// Part uuids packet if any comes before query.
            processIgnoredPartUUIDs();
            return true;

        case Protocol::Client::Query:
            processQuery(state);
            return true;

        default:
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown packet {} from client", toString(packet_type));
    }

    chassert(server.isCancelled() || !tcp_server.isOpen());
    throw Exception(ErrorCodes::ABORTED, "Server shutdown is called");
}


bool TCPHandler::receivePacketsExpectDataConcurrentWithExecutor(QueryState & state)
{
    std::lock_guard lock(callback_mutex);
    return receivePacketsExpectData(state);
}

bool TCPHandler::receivePacketsExpectData(QueryState & state)
{
    /// Poll interval should not be greater than receive_timeout
    constexpr UInt64 min_timeout_us = 5000; // 5 ms
    UInt64 timeout_us = std::max(
            min_timeout_us,
            std::min(
                poll_interval * 1000000,
                static_cast<UInt64>(receive_timeout.totalMicroseconds())));

    Stopwatch watch;

    while (!server.isCancelled() && tcp_server.isOpen())
    {
        while (!in->poll(timeout_us))
        {
            size_t elapsed = size_t(watch.elapsedSeconds());
            if (elapsed > size_t(receive_timeout.totalSeconds()))
            {
                throw NetException(ErrorCodes::SOCKET_TIMEOUT,
                                "Timeout exceeded while receiving data from client. Waited for {} seconds, timeout is {} seconds.",
                                elapsed, receive_timeout.totalSeconds());
            }
        }

        UInt64 packet_type = 0;
        readVarUInt(packet_type, *in);

        switch (packet_type)
        {
            case Protocol::Client::IgnoredPartUUIDs:
                processUnexpectedIgnoredPartUUIDs();

            case Protocol::Client::Query:
                processUnexpectedQuery();

            case Protocol::Client::Hello:
                processUnexpectedHello();

            case Protocol::Client::TablesStatusRequest:
                processUnexpectedTablesStatusRequest();

            case Protocol::Client::Data:
            case Protocol::Client::Scalar:
            {
                bool empty_block;
                if (state.skipping_data)
                    empty_block = !processUnexpectedData();
                else
                    empty_block = !processData(state, packet_type == Protocol::Client::Scalar);
                if (empty_block)
                    state.read_all_data = true;
                return !empty_block;
            }

            case Protocol::Client::QueryPlan:
                return receiveQueryPlan(state);

            case Protocol::Client::Ping:
                writeVarUInt(Protocol::Server::Pong, *out);
                out->finishChunk();
                out->next();
                continue;

            case Protocol::Client::Cancel:
                processCancel(state);
                return false; // We return false from this function as if no more data received

            default:
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown packet {} from client", toString(packet_type));
        }
    }

    chassert(server.isCancelled() || !tcp_server.isOpen());
    throw Exception(ErrorCodes::ABORTED, "Server shutdown is called");
}


void TCPHandler::readTemporaryTables(QueryState & state)
{
    sendLogs(state);

    /// no sense in partial_result_on_first_cancel setting when temporary data is read.
    auto off_setting_guard = TurnOffBoolSettingTemporary(state.allow_partial_result_on_first_cancel);

    while (receivePacketsExpectData(state))
    {
        sendLogs(state);
        sendInsertProfileEvents(state);
    }
}


void TCPHandler::skipData(QueryState & state)
{
    state.skipping_data = true;
    SCOPE_EXIT({ state.skipping_data = false; });

    size_t blocks = 0;
    while (receivePacketsExpectData(state))
        ++blocks;
    LOG_TRACE(log, "Discarded {} blocks", blocks);
}


void TCPHandler::startInsertQuery(QueryState & state)
{
    std::lock_guard lock(callback_mutex);

    /// Send ColumnsDescription for insertion table
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA)
    {
        const auto & table_id = state.query_context->getInsertionTable();
        if (state.query_context->getSettingsRef()[Setting::input_format_defaults_for_omitted_fields])
        {
            if (!table_id.empty())
            {
                auto storage_ptr = DatabaseCatalog::instance().getTable(table_id, state.query_context);
                if (!storage_ptr)
                    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} does not exist", table_id.getNameForLogs());
                sendTableColumns(state, storage_ptr->getInMemoryMetadataPtr()->getColumns());
            }
        }
    }

    /// Send block to the client - table structure.
    sendData(state, state.io.pipeline.getHeader());
    sendLogs(state);

    /// Update flag after reading external tables
    state.read_all_data = false;
}


AsynchronousInsertQueue::PushResult TCPHandler::processAsyncInsertQuery(QueryState & state, AsynchronousInsertQueue & insert_queue)
{
    using PushResult = AsynchronousInsertQueue::PushResult;

    startInsertQuery(state);
    Squashing squashing(state.input_header, 0, state.query_context->getSettingsRef()[Setting::async_insert_max_data_size]);

    while (receivePacketsExpectDataConcurrentWithExecutor(state))
    {
        squashing.setHeader(state.block_for_insert.cloneEmpty());
        auto result_chunk = Squashing::squash(squashing.add({state.block_for_insert.getColumns(), state.block_for_insert.rows()}, /*flush_if_enough_size*/ true));

        sendLogs(state);
        sendInsertProfileEvents(state);

        if (result_chunk)
        {
            auto result = squashing.getHeader().cloneWithColumns(result_chunk.detachColumns());
            return PushResult
            {
                .status = PushResult::TOO_MUCH_DATA,
                .insert_block = std::move(result),
            };
        }
    }

    Chunk result_chunk = Squashing::squash(squashing.flush());
    if (!result_chunk)
    {
        return insert_queue.pushQueryWithBlock(state.parsed_query, squashing.getHeader().cloneWithoutColumns(), state.query_context);
    }

    auto result = squashing.getHeader().cloneWithColumns(result_chunk.detachColumns());
    return insert_queue.pushQueryWithBlock(state.parsed_query, std::move(result), state.query_context);
}


void TCPHandler::processInsertQuery(QueryState & state)
{
    size_t num_threads = state.io.pipeline.getNumThreads();

    auto run_executor = [&](auto & executor, Block processed_data)
    {
        try
        {
            /// Made above the rest of the lines,
            /// so that in case of `start` function throws an exception,
            /// client receive exception before sending data.
            executor.start();

            if (processed_data)
                executor.push(std::move(processed_data));
            else
                startInsertQuery(state);

            while (receivePacketsExpectDataConcurrentWithExecutor(state))
            {
                executor.push(std::move(state.block_for_insert));

                sendLogs(state);
                sendInsertProfileEvents(state);
            }

            executor.finish();
        }
        catch (...)
        {
            executor.cancel();
            throw;
        }
    };

    Block processed_block;
    const auto & settings = state.query_context->getSettingsRef();

    auto * insert_queue = state.query_context->tryGetAsynchronousInsertQueue();
    const auto & insert_query = assert_cast<const ASTInsertQuery &>(*state.parsed_query);

    bool async_insert_enabled = settings[Setting::async_insert];
    if (insert_query.table_id)
        if (auto table = DatabaseCatalog::instance().tryGetTable(insert_query.table_id, state.query_context))
            async_insert_enabled |= table->areAsynchronousInsertsEnabled();

    if (insert_queue && async_insert_enabled && !insert_query.select)
    {
        /// Let's agree on terminology and say that a mini-INSERT is an asynchronous INSERT
        /// which typically contains not a lot of data inside and a big-INSERT in an INSERT
        /// which was formed by concatenating several mini-INSERTs together.
        /// In case when the client had to retry some mini-INSERTs then they will be properly deduplicated
        /// by the source tables. This functionality is controlled by a setting `async_insert_deduplicate`.
        /// But then they will be glued together into a block and pushed through a chain of Materialized Views if any.
        /// The process of forming such blocks is not deteministic so each time we retry mini-INSERTs the resulting
        /// block may be concatenated differently.
        /// That's why deduplication in dependent Materialized Views doesn't make sense in presence of async INSERTs.
        if (settings[Setting::throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert]
            && settings[Setting::deduplicate_blocks_in_dependent_materialized_views])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                    "Deduplication in dependent materialized view cannot work together with async inserts. "\
                    "Please disable either `deduplicate_blocks_in_dependent_materialized_views` or `async_insert` setting.");

        auto result = processAsyncInsertQuery(state, *insert_queue);
        if (result.status == AsynchronousInsertQueue::PushResult::OK)
        {
            /// Reset pipeline because it may hold write lock for some storages.
            state.io.pipeline.cancel();
            state.io.pipeline.reset();
            if (settings[Setting::wait_for_async_insert])
            {
                size_t timeout_ms = settings[Setting::wait_for_async_insert_timeout].totalMilliseconds();
                auto wait_status = result.future.wait_for(std::chrono::milliseconds(timeout_ms));

                if (wait_status == std::future_status::deferred)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got future in deferred state");

                if (wait_status == std::future_status::timeout)
                    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Wait for async insert timeout ({} ms) exceeded)", timeout_ms);

                result.future.get();
            }

            sendInsertProfileEvents(state);
            return;
        }
        if (result.status == AsynchronousInsertQueue::PushResult::TOO_MUCH_DATA)
        {
            LOG_DEBUG(log, "Setting async_insert=1, but INSERT query will be executed synchronously because it has too much data");
            processed_block = std::move(result.insert_block);
        }
    }

    if (num_threads > 1)
    {
        PushingAsyncPipelineExecutor executor(state.io.pipeline);
        run_executor(executor, std::move(processed_block));
    }
    else
    {
        PushingPipelineExecutor executor(state.io.pipeline);
        run_executor(executor, std::move(processed_block));
    }

    sendInsertProfileEvents(state);
}


void TCPHandler::processOrdinaryQuery(QueryState & state)
{
    auto & pipeline = state.io.pipeline;

    if (state.query_context->getSettingsRef()[Setting::allow_experimental_query_deduplication])
    {
        sendPartUUIDs(state);
    }

    /// Send header-block, to allow client to prepare output format for data to send.
    {
        const auto & header = pipeline.getHeader();

        if (header)
        {
            sendData(state, header);
        }
    }

    {
        PullingAsyncPipelineExecutor executor(pipeline);
        pipeline.setConcurrencyControl(state.query_context->getSettingsRef()[Setting::use_concurrency_control]);
        CurrentMetrics::Increment query_thread_metric_increment{CurrentMetrics::QueryThread};

        try
        {
            Block block;
            while (executor.pull(block, interactive_delay / 1000))
            {

                {
                    std::lock_guard lock(callback_mutex);
                    receivePacketsExpectCancel(state);
                }

                if (state.stop_read_return_partial_result)
                {
                    executor.cancelReading();
                }

                {
                    std::lock_guard lock(callback_mutex);

                    if (after_send_progress.elapsed() / 1000 >= interactive_delay)
                    {
                        /// Some time passed and there is a progress.
                        after_send_progress.restart();
                        sendProgress(state);
                        sendSelectProfileEvents(state);
                    }

                    sendLogs(state);

                    if (block)
                    {
                        if (!state.io.null_format)
                            sendData(state, block);
                    }
                }
            }
        }
        catch (...)
        {
            executor.cancel();
            throw;
        }

        /** If data has run out, we will send the profiling data and total values to
          * the last zero block to be able to use
          * this information in the suffix output of stream.
          * If the request was interrupted, then `sendTotals` and other methods could not be called,
          *  because we have not read all the data yet,
          *  and there could be ongoing calculations in other threads at the same time.
          */


        std::lock_guard lock(callback_mutex);

        receivePacketsExpectCancel(state);

        sendTotals(state, executor.getTotalsBlock());
        sendExtremes(state, executor.getExtremesBlock());
        sendProfileInfo(state, executor.getProfileInfo());
        sendProgress(state);
        sendLogs(state);
        sendSelectProfileEvents(state);

        sendData(state, {});

        sendProgress(state);
    }
}


void TCPHandler::processTablesStatusRequest()
{
    TablesStatusRequest request;
    request.read(*in, client_tcp_protocol_version);

    ContextPtr context_to_resolve_table_names;
    if (is_interserver_mode)
    {
        /// In the interserver mode session context does not exist, because authentication is done for each query.
        /// We also cannot create query context earlier, because it cannot be created before authentication,
        /// but query is not received yet. So we have to do this trick.
        ContextMutablePtr fake_interserver_context = Context::createCopy(server.context());
        if (!default_database.empty())
            fake_interserver_context->setCurrentDatabase(default_database);
        context_to_resolve_table_names = fake_interserver_context;
    }
    else
    {
        assert(session);
        context_to_resolve_table_names = session->sessionContext();
    }

    TablesStatusResponse response;
    for (const QualifiedTableName & table_name: request.tables)
    {
        auto resolved_id = context_to_resolve_table_names->tryResolveStorageID({table_name.database, table_name.table});
        StoragePtr table = DatabaseCatalog::instance().tryGetTable(resolved_id, context_to_resolve_table_names);
        if (!table)
            continue;

        TableStatus status;
        if (auto * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
        {
            status.is_replicated = true;
            status.absolute_delay = static_cast<UInt32>(replicated_table->getAbsoluteDelay());
            status.is_readonly = replicated_table->isTableReadOnly();
        }
        else
            status.is_replicated = false;

        response.table_states_by_id.emplace(table_name, std::move(status));
    }

    writeVarUInt(Protocol::Server::TablesStatusResponse, *out);

    /// For testing hedged requests
    if (unlikely(sleep_in_send_tables_status.totalMilliseconds()))
    {
        out->next();
        std::chrono::milliseconds ms(sleep_in_send_tables_status.totalMilliseconds());
        std::this_thread::sleep_for(ms);
    }

    response.write(*out, client_tcp_protocol_version);

    out->finishChunk();
    out->next();
}


void TCPHandler::processUnexpectedTablesStatusRequest()
{
    TablesStatusRequest skip_request;
    skip_request.read(*in, client_tcp_protocol_version);

    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet TablesStatusRequest received from client");
}


void TCPHandler::sendPartUUIDs(QueryState & state)
{
    auto uuids = state.query_context->getPartUUIDs()->get();
    if (uuids.empty())
        return;

    writeVarUInt(Protocol::Server::PartUUIDs, *out);
    writeVectorBinary(uuids, *out);

    out->finishChunk();
    out->next();
}


void TCPHandler::sendReadTaskRequest()
{
    writeVarUInt(Protocol::Server::ReadTaskRequest, *out);

    out->finishChunk();
    out->next();
}


void TCPHandler::sendMergeTreeAllRangesAnnouncement(QueryState &, InitialAllRangesAnnouncement announcement)
{
    writeVarUInt(Protocol::Server::MergeTreeAllRangesAnnouncement, *out);
    announcement.serialize(*out, client_parallel_replicas_protocol_version);

    out->finishChunk();
    out->next();
}


void TCPHandler::sendMergeTreeReadTaskRequest(ParallelReadRequest request)
{
    writeVarUInt(Protocol::Server::MergeTreeReadTaskRequest, *out);
    request.serialize(*out, client_parallel_replicas_protocol_version);

    out->finishChunk();
    out->next();
}


void TCPHandler::sendProfileInfo(QueryState &, const ProfileInfo & info)
{
    writeVarUInt(Protocol::Server::ProfileInfo, *out);
    info.write(*out, client_tcp_protocol_version);

    out->finishChunk();
    out->next();
}


void TCPHandler::sendTotals(QueryState & state, const Block & totals)
{
    if (!totals)
        return;

    initBlockOutput(state, totals);

    writeVarUInt(Protocol::Server::Totals, *out);
    writeStringBinary("", *out);

    state.block_out->write(totals);
    state.maybe_compressed_out->next();

    out->finishChunk();
    out->next();
}


void TCPHandler::sendExtremes(QueryState & state, const Block & extremes)
{
    if (!extremes)
        return;

    initBlockOutput(state, extremes);

    writeVarUInt(Protocol::Server::Extremes, *out);
    writeStringBinary("", *out);

    state.block_out->write(extremes);
    state.maybe_compressed_out->next();

    out->finishChunk();
    out->next();
}


void TCPHandler::sendProfileEvents(QueryState & state)
{
    Stopwatch stopwatch;
    Block block = ProfileEvents::getProfileEvents(host_name, state.profile_queue, state.last_sent_snapshots);
    if (block.rows() != 0)
    {
        initProfileEventsBlockOutput(state, block);

        writeVarUInt(Protocol::Server::ProfileEvents, *out);
        writeStringBinary("", *out);

        state.profile_events_block_out->write(block);

        out->finishChunk();
        out->next();

        auto elapsed_milliseconds = stopwatch.elapsedMilliseconds();
        if (elapsed_milliseconds > 100)
            LOG_DEBUG(log, "Sending profile events block with {} rows, {} bytes took {} milliseconds",
                block.rows(), block.bytes(), elapsed_milliseconds);
    }
}


void TCPHandler::sendSelectProfileEvents(QueryState & state)
{
    if (client_tcp_protocol_version < DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS)
        return;

    sendProfileEvents(state);
}


void TCPHandler::sendInsertProfileEvents(QueryState & state)
{
    if (client_tcp_protocol_version < DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT)
        return;
    if (query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        return;

    sendProfileEvents(state);
}


void TCPHandler::sendTimezone(QueryState & state)
{
    if (client_tcp_protocol_version < DBMS_MIN_PROTOCOL_VERSION_WITH_TIMEZONE_UPDATES)
        return;

    const String & tz = state.query_context->getSettingsRef()[Setting::session_timezone].value;

    LOG_DEBUG(log, "TCPHandler::sendTimezone(): {}", tz);
    writeVarUInt(Protocol::Server::TimezoneUpdate, *out);
    writeStringBinary(tz, *out);

    out->finishChunk();
    out->next();
}


bool TCPHandler::receiveProxyHeader()
{
    if (in->eof())
    {
        LOG_WARNING(log, "Client has not sent any data.");
        return false;
    }

    String forwarded_address;

    /// Only PROXYv1 is supported.
    /// Validation of protocol is not fully performed.

    LimitReadBuffer limit_in(*in, {.read_no_more=107, .expect_eof=true}); /// Maximum length from the specs.

    assertString("PROXY ", limit_in);

    if (limit_in.eof())
    {
        LOG_WARNING(log, "Incomplete PROXY header is received.");
        return false;
    }

    /// TCP4 / TCP6 / UNKNOWN
    if ('T' == *limit_in.position())
    {
        assertString("TCP", limit_in);

        if (limit_in.eof())
        {
            LOG_WARNING(log, "Incomplete PROXY header is received.");
            return false;
        }

        if ('4' != *limit_in.position() && '6' != *limit_in.position())
        {
            LOG_WARNING(log, "Unexpected protocol in PROXY header is received.");
            return false;
        }

        bool is_tcp6 = ('6' == *limit_in.position());

        ++limit_in.position();
        assertChar(' ', limit_in);

        /// Read the first field and ignore other.
        readStringUntilWhitespace(forwarded_address, limit_in);

        if (is_tcp6)
            forwarded_address = "[" + forwarded_address + "]";

        /// Skip second field (destination address)
        assertChar(' ', limit_in);
        skipStringUntilWhitespace(limit_in);
        assertChar(' ', limit_in);

        /// Read source port
        String port;
        readStringUntilWhitespace(port, limit_in);

        forwarded_address += ":" + port;

        /// Skip until \r\n
        while (!limit_in.eof() && *limit_in.position() != '\r')
            ++limit_in.position();
        assertString("\r\n", limit_in);
    }
    else if (checkString("UNKNOWN", limit_in))
    {
        /// This is just a health check, there is no subsequent data in this connection.

        while (!limit_in.eof() && *limit_in.position() != '\r')
            ++limit_in.position();
        assertString("\r\n", limit_in);
        return false;
    }
    else
    {
        LOG_WARNING(log, "Unexpected protocol in PROXY header is received.");
        return false;
    }

    LOG_TRACE(log, "Forwarded client address from PROXY header: {}", forwarded_address);
    forwarded_for = std::move(forwarded_address);
    return true;
}


namespace
{

std::string formatHTTPErrorResponseWhenUserIsConnectedToWrongPort(const Poco::Util::AbstractConfiguration& config)
{
    std::string result = fmt::format(
        "HTTP/1.0 400 Bad Request\r\n\r\n"
        "Port {} is for clickhouse-client program\r\n",
        config.getString("tcp_port"));

    if (config.has("http_port"))
    {
        result += fmt::format(
            "You must use port {} for HTTP.\r\n",
            config.getString("http_port"));
    }

    return result;
}

}


std::unique_ptr<Session> TCPHandler::makeSession()
{
    auto interface = is_interserver_mode ? ClientInfo::Interface::TCP_INTERSERVER : ClientInfo::Interface::TCP;

    auto res = std::make_unique<Session>(server.context(), interface, socket().secure(), certificate);

    res->setForwardedFor(forwarded_for);
    res->setClientName(client_name);
    res->setClientVersion(client_version_major, client_version_minor, client_version_patch, client_tcp_protocol_version);
    res->setConnectionClientVersion(client_version_major, client_version_minor, client_version_patch, client_tcp_protocol_version);
    res->setClientInterface(interface);

    return res;
}


void TCPHandler::receiveHello()
{
    /// Receive `hello` packet.
    UInt64 packet_type = 0;
    String user;
    String password;
    String default_db;

    readVarUInt(packet_type, *in);

    if (packet_type != Protocol::Client::Hello)
    {
        /** If you accidentally accessed the HTTP protocol for a port destined for an internal TCP protocol,
          * Then instead of the packet type, there will be G (GET) or P (POST), in most cases.
          */
        if (packet_type == 'G' || packet_type == 'P')
        {
            writeString(formatHTTPErrorResponseWhenUserIsConnectedToWrongPort(server.config()), *out);
            out->next();
            throw Exception(ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT, "Client has connected to wrong port");
        }
        else
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                               "Unexpected packet from client (expected Hello, got {})", packet_type);
    }

    readStringBinary(client_name, *in);
    readVarUInt(client_version_major, *in);
    readVarUInt(client_version_minor, *in);
    // NOTE For backward compatibility of the protocol, client cannot send its version_patch.
    readVarUInt(client_tcp_protocol_version, *in);
    readStringBinary(default_db, *in);
    if (!default_db.empty())
        default_database = default_db;
    readStringBinary(user, *in);
    readStringBinary(password, *in);

    if (user.empty())
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet from client (no user in Hello package)");

    LOG_DEBUG(log, "Connected {} version {}.{}.{}, revision: {}{}{}.",
        client_name,
        client_version_major, client_version_minor, client_version_patch,
        client_tcp_protocol_version,
        (!default_database.empty() ? ", database: " + default_database : ""),
        (!user.empty() ? ", user: " + user : "")
    );

    is_interserver_mode = (user == EncodedUserInfo::USER_INTERSERVER_MARKER) && password.empty();
    if (is_interserver_mode)
    {
        if (client_tcp_protocol_version < DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2)
            LOG_WARNING(LogFrequencyLimiter(log, 10),
                        "Using deprecated interserver protocol because the client is too old. Consider upgrading all nodes in cluster.");
        processClusterNameAndSalt();
        return;
    }

    is_ssh_based_auth = user.starts_with(EncodedUserInfo::SSH_KEY_AUTHENTICAION_MARKER) && password.empty();
    if (is_ssh_based_auth)
        user.erase(0, std::string_view(EncodedUserInfo::SSH_KEY_AUTHENTICAION_MARKER).size());

    session = makeSession();
    const auto & client_info = session->getClientInfo();

#if USE_SSL
    /// Authentication with SSL user certificate
    if (dynamic_cast<Poco::Net::SecureStreamSocketImpl*>(socket().impl()))
    {
        Poco::Net::SecureStreamSocket secure_socket(socket());
        if (secure_socket.havePeerCertificate())
        {
            try
            {
                session->authenticate(
                    SSLCertificateCredentials{user, X509Certificate(secure_socket.peerCertificate()).extractAllSubjects()},
                    getClientAddress(client_info));
                return;
            }
            catch (const Exception & e)
            {
                if (e.code() != DB::ErrorCodes::AUTHENTICATION_FAILED)
                    throw;

                tryLogCurrentException(log, "SSL authentication failed, falling back to password authentication", LogsLevel::information);
                /// ^^ Log at debug level instead of default error level as authentication failures are not an unusual event.
            }
        }
    }
#endif

#if USE_SSH
    /// Perform handshake for SSH authentication
    if (is_ssh_based_auth)
    {
        const auto authentication_types = session->getAuthenticationTypesOrLogInFailure(user);

        bool user_supports_ssh_authentication = std::find_if(
            authentication_types.begin(),
            authentication_types.end(),
            [](auto authentication_type)
            {
               return authentication_type ==  AuthenticationType::SSH_KEY;
            }) != authentication_types.end();

        if (!user_supports_ssh_authentication)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Expected authentication with SSH key");

        if (client_tcp_protocol_version < DBMS_MIN_REVISION_WITH_SSH_AUTHENTICATION)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Cannot authenticate user with SSH key, because client version is too old");

        readVarUInt(packet_type, *in);
        if (packet_type != Protocol::Client::SSHChallengeRequest)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Server expected to receive a packet for requesting a challenge string");

        auto create_challenge = []()
        {
            pcg64_fast rng(randomSeed());
            UInt64 rand = rng();
            return encodeSHA256(&rand, sizeof(rand));
        };

        String challenge = create_challenge();
        writeVarUInt(Protocol::Server::SSHChallenge, *out);
        writeStringBinary(challenge, *out);
        out->next();

        String signature;
        readVarUInt(packet_type, *in);
        if (packet_type != Protocol::Client::SSHChallengeResponse)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Server expected to receive a packet with a response for a challenge");
        readStringBinary(signature, *in);

        auto prepare_string_for_ssh_validation = [&](const String & username, const String & challenge_)
        {
            String output;
            output.append(std::to_string(client_tcp_protocol_version));
            output.append(default_database);
            output.append(username);
            output.append(challenge_);
            return output;
        };

        auto cred = SshCredentials(user, signature, prepare_string_for_ssh_validation(user, challenge));
        session->authenticate(cred, getClientAddress(client_info));
        return;
    }
#endif

    session->authenticate(user, password, getClientAddress(client_info));
}


void TCPHandler::receiveAddendum()
{
    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY)
        readStringBinary(quota_key, *in);

    if (!is_interserver_mode)
        session->setQuotaClientKey(quota_key);

    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS)
    {
        readStringBinary(proto_send_chunked_cl, *in);
        readStringBinary(proto_recv_chunked_cl, *in);
    }

    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL)
        readVarUInt(client_parallel_replicas_protocol_version, *in);
}


void TCPHandler::processUnexpectedHello()
{
    UInt64 skip_uint_64;
    String skip_string;

    readStringBinary(skip_string, *in);
    readVarUInt(skip_uint_64, *in);
    readVarUInt(skip_uint_64, *in);
    readVarUInt(skip_uint_64, *in);
    readStringBinary(skip_string, *in);
    readStringBinary(skip_string, *in);
    readStringBinary(skip_string, *in);

    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Hello received from client");
}


void TCPHandler::sendHello()
{
    writeVarUInt(Protocol::Server::Hello, *out);
    writeStringBinary(VERSION_NAME, *out);
    writeVarUInt(VERSION_MAJOR, *out);
    writeVarUInt(VERSION_MINOR, *out);
    writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL)
        writeVarUInt(DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION, *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE)
        writeStringBinary(DateLUT::instance().getTimeZone(), *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME)
        writeStringBinary(server_display_name, *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
        writeVarUInt(VERSION_PATCH, *out);
    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS)
    {
        writeStringBinary(server.config().getString("proto_caps.send", "notchunked"), *out);
        writeStringBinary(server.config().getString("proto_caps.recv", "notchunked"), *out);
    }
    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES)
    {
        auto rules = server.context()->getAccessControl().getPasswordComplexityRules();

        writeVarUInt(rules.size(), *out);
        for (const auto & [original_pattern, exception_message] : rules)
        {
            writeStringBinary(original_pattern, *out);
            writeStringBinary(exception_message, *out);
        }
    }
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2)
    {
        chassert(!nonce.has_value());
        /// Contains lots of stuff (including time), so this should be enough for NONCE.
        nonce.emplace(thread_local_rng());
        writeIntBinary(nonce.value(), *out);
    }

    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_SETTINGS)
    {
        if (is_interserver_mode ||
            !session->sessionContext()->getSettingsRef()[Setting::apply_settings_from_server])
            Settings::writeEmpty(*out); // send empty list of setting changes
        else
            session->sessionContext()->getSettingsRef().write(*out, SettingsWriteFormat::STRINGS_WITH_FLAGS);
    }

    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION)
    {
        writeVarUInt(DBMS_QUERY_PLAN_SERIALIZATION_VERSION, *out);
    }

    out->next();
}


void TCPHandler::processIgnoredPartUUIDs()
{
    readVectorBinary(part_uuids_to_ignore.emplace(), *in);
}


void TCPHandler::processUnexpectedIgnoredPartUUIDs()
{
    std::vector<UUID> skip_part_uuids;
    readVectorBinary(skip_part_uuids, *in);
    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet IgnoredPartUUIDs received from client");
}


String TCPHandler::receiveReadTaskResponse(QueryState & state)
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);

    switch (packet_type)
    {
        case Protocol::Client::Cancel:
            processCancel(state, /* throw_exception */ true);
            return {};

        case Protocol::Client::ReadTaskResponse:
        {
            UInt64 version = 0;
            readVarUInt(version, *in);
            if (version != DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION)
                throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Protocol version for distributed processing mismatched");
            String response;
            readStringBinary(response, *in);
            return response;
        }

        default:
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Received {} packet after requesting read task",
                    Protocol::Client::toString(packet_type));
    }
}


std::optional<ParallelReadResponse> TCPHandler::receivePartitionMergeTreeReadTaskResponse(QueryState & state)
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);

    switch (packet_type)
    {
        case Protocol::Client::Cancel:
            processCancel(state, /* throw_exception */ true);
            return {};

        case Protocol::Client::MergeTreeReadTaskResponse:
        {
            ParallelReadResponse response;
            response.deserialize(*in);
            return response;
        }

        default:
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "Received {} packet after requesting read task",
                Protocol::Client::toString(packet_type));
    }
}


void TCPHandler::processClusterNameAndSalt()
{
    readStringBinary(cluster, *in);
    readStringBinary(salt, *in, 32);
}


void TCPHandler::processQuery(std::optional<QueryState> & state)
{
    UInt64 stage = 0;
    UInt64 compression = 0;

    state.emplace();

    if (part_uuids_to_ignore.has_value())
        state->part_uuids_to_ignore = std::move(part_uuids_to_ignore);

    readStringBinary(state->query_id, *in);

    /// In interserver mode,
    /// initial_user can be empty in case of Distributed INSERT via Buffer/Kafka,
    /// (i.e. when the INSERT is done with the global context without user),
    /// so it is better to reset session to avoid using old user.
    if (is_interserver_mode)
    {
        session = makeSession();
    }

    /// Read client info.
    ClientInfo client_info = session->getClientInfo();
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_CLIENT_INFO)
    {
        client_info.read(*in, client_tcp_protocol_version);

        correctQueryClientInfo(session->getClientInfo(), client_info);
        const auto & config_ref = Context::getGlobalContextInstance()->getServerSettings();
        if (config_ref[ServerSetting::validate_tcp_client_information])
            validateClientInfo(session->getClientInfo(), client_info);
    }

    /// Per query settings are also passed via TCP.
    /// We need to check them before applying due to they can violate the settings constraints.
    auto settings_format = (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS)
        ? SettingsWriteFormat::STRINGS_WITH_FLAGS
        : SettingsWriteFormat::BINARY;

    Settings passed_settings;
    passed_settings.read(*in, settings_format);

    std::string received_extra_roles;
    // TODO: check if having `is_interserver_mode` doesn't break interoperability with the CH-client.
    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES)
    {
        readStringBinary(received_extra_roles, *in);
    }

    /// Interserver secret.
    std::string received_hash;
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET)
    {
        readStringBinary(received_hash, *in, 32);
    }

    readVarUInt(stage, *in);
    state->stage = QueryProcessingStage::Enum(stage);

    readVarUInt(compression, *in);
    state->compression = static_cast<Protocol::Compression>(compression);
    last_block_in.compression = state->compression;

    readStringBinary(state->query, *in);

    Settings passed_params;
    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS)
        passed_params.read(*in, settings_format);

    if (is_interserver_mode)
    {
        client_info.interface = ClientInfo::Interface::TCP_INTERSERVER;
#if USE_SSL

        String cluster_secret;
        try
        {
            cluster_secret = server.context()->getCluster(cluster)->getSecret();
        }
        catch (const Exception & e)
        {
            auto exception = Exception::createRuntime(ErrorCodes::AUTHENTICATION_FAILED, e.message());
            session->onAuthenticationFailure(/* user_name= */ std::nullopt, socket().peerAddress(), exception);
            throw exception; /// NOLINT
        }

        if (salt.empty() || cluster_secret.empty())
        {
            auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED, "Interserver authentication failed (no salt/cluster secret)");
            session->onAuthenticationFailure(/* user_name= */ std::nullopt, socket().peerAddress(), exception);
            throw exception; /// NOLINT
        }

        if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 && !nonce.has_value())
        {
            auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED, "Interserver authentication failed (no nonce)");
            session->onAuthenticationFailure(/* user_name= */ std::nullopt, socket().peerAddress(), exception);
            throw exception; /// NOLINT
        }

        std::string data(salt);
        // For backward compatibility
        if (nonce.has_value())
            data += std::to_string(nonce.value());
        data += cluster_secret;
        data += state->query;
        data += state->query_id;
        data += client_info.initial_user;
        data += received_extra_roles;

        std::string calculated_hash = encodeSHA256(data);
        assert(calculated_hash.size() == 32);

        /// TODO maybe also check that peer address actually belongs to the cluster?
        if (calculated_hash != received_hash)
        {
            auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED, "Interserver authentication failed");
            session->onAuthenticationFailure(/* user_name */ std::nullopt, socket().peerAddress(), exception);
            throw exception; /// NOLINT
        }

        /// NOTE Usually we get some fields of client_info (including initial_address and initial_user) from user input,
        /// so we should not rely on that. However, in this particular case we got client_info from other clickhouse-server, so it's ok.
        if (client_info.initial_user.empty())
        {
            LOG_DEBUG(log, "User (no user, interserver mode) (client: {})", getClientAddress(client_info).toString());
        }
        else
        {
            // In a cluster, query originator may have an access to the external auth provider with role mapping (like LDAP server),
            // that grants specific roles to the user. We want these roles to be granted to the effective user on other nodes of cluster when
            // query is executed.
            Strings external_roles;
            if (!received_extra_roles.empty())
            {
                ReadBufferFromString buffer(received_extra_roles);

                readVectorBinary(external_roles, buffer);
                LOG_DEBUG(log, "Parsed extra roles [{}]", fmt::join(external_roles, ", "));
            }

            LOG_DEBUG(log, "User (initial, interserver mode): {} (client: {})", client_info.initial_user, getClientAddress(client_info).toString());
            /// In case of inter-server mode authorization is done with the
            /// initial address of the client, not the real address from which
            /// the query was come, since the real address is the address of
            /// the initiator server, while we are interested in client's
            /// address.
            session->authenticate(AlwaysAllowCredentials{client_info.initial_user}, *client_info.initial_address, external_roles);
        }

        is_interserver_authenticated = true;
#else
        auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "Inter-server secret support is disabled, because ClickHouse was built without SSL library");
        session->onAuthenticationFailure(/* user_name */ std::nullopt, socket().peerAddress(), exception);
        throw exception; /// NOLINT
#endif
    }

    state->query_context = session->makeQueryContext(client_info);

    /// Sets the default database if it wasn't set earlier for the session context.
    if (is_interserver_mode && !default_database.empty())
        state->query_context->setCurrentDatabase(default_database);

    if (state->part_uuids_to_ignore)
        state->query_context->getIgnoredPartUUIDs()->add(*state->part_uuids_to_ignore);

    state->query_context->setProgressCallback(
        [this, &state] (const Progress & value) { this->updateProgress(state.value(), value); });
    state->query_context->setFileProgressCallback(
        [this, &state](const FileProgress & value) { this->updateProgress(state.value(), Progress(value)); });

    ///
    /// Settings
    ///

    /// FIXME: Remove when allow_experimental_analyzer will become obsolete.
    /// Analyzer became Beta in 24.3 and started to be enabled by default.
    /// We have to disable it for ourselves to make sure we don't have different settings on
    /// different servers.
    if (query_kind == ClientInfo::QueryKind::SECONDARY_QUERY
        && VersionNumber(client_info.client_version_major, client_info.client_version_minor, client_info.client_version_patch)
            < VersionNumber(23, 3, 0)
        && !passed_settings[Setting::allow_experimental_analyzer].changed)
        passed_settings.set("allow_experimental_analyzer", false);

    auto settings_changes = passed_settings.changes();
    query_kind = state->query_context->getClientInfo().query_kind;
    if (query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        /// Throw an exception if the passed settings violate the constraints.
        state->query_context->checkSettingsConstraints(settings_changes, SettingSource::QUERY);
    }
    else
    {
        /// Quietly clamp to the constraints if it's not an initial query.
        state->query_context->clampToSettingsConstraints(settings_changes, SettingSource::QUERY);
    }
    state->query_context->applySettingsChanges(settings_changes);

    /// Use the received query id, or generate a random default. It is convenient
    /// to also generate the default OpenTelemetry trace id at the same time, and
    /// set the trace parent.
    /// Notes:
    /// 1) ClientInfo might contain upstream trace id, so we decide whether to use
    /// the default ids after we have received the ClientInfo.
    /// 2) There is the opentelemetry_start_trace_probability setting that
    /// controls when we start a new trace. It can be changed via Native protocol,
    /// so we have to apply the changes first.
    state->query_context->setCurrentQueryId(state->query_id);

    state->query_context->addQueryParameters(passed_params.toNameToNameMap());

    state->allow_partial_result_on_first_cancel = state->query_context->getSettingsRef()[Setting::partial_result_on_first_cancel];

    /// For testing hedged requests
    if (unlikely(sleep_after_receiving_query.totalMilliseconds()))
    {
        std::chrono::milliseconds ms(sleep_after_receiving_query.totalMilliseconds());
        std::this_thread::sleep_for(ms);
    }

    state->read_all_data = false;
}

void TCPHandler::processUnexpectedQuery()
{
    UInt64 skip_uint_64;
    String skip_string;

    readStringBinary(skip_string, *in);

    ClientInfo skip_client_info;
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        skip_client_info.read(*in, client_tcp_protocol_version);

    Settings skip_settings;
    auto settings_format = (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS) ? SettingsWriteFormat::STRINGS_WITH_FLAGS
                                                                                                      : SettingsWriteFormat::BINARY;
    skip_settings.read(*in, settings_format);

    std::string skip_hash;
    bool interserver_secret = client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET;
    if (interserver_secret)
        readStringBinary(skip_hash, *in, 32);

    readVarUInt(skip_uint_64, *in);

    readVarUInt(skip_uint_64, *in);
    last_block_in.compression = static_cast<Protocol::Compression>(skip_uint_64);

    readStringBinary(skip_string, *in);

    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS)
        skip_settings.read(*in, settings_format);

    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Query received from client");
}

bool TCPHandler::receiveQueryPlan(QueryState & state)
{
    bool unexpected_packet = state.stage != QueryProcessingStage::QueryPlan || state.plan_and_sets || !state.query_context || state.read_all_data;
    auto context = unexpected_packet ? Context::getGlobalContextInstance() : state.query_context;

    auto plan_and_sets = QueryPlan::deserialize(*in, context);
    LOG_TRACE(log, "Received query plan");

    if (!state.skipping_data && unexpected_packet)
        throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet QueryPlan received from client");

    state.plan_and_sets = std::make_shared<QueryPlanAndSets>(std::move(plan_and_sets));
    return true;
}

bool TCPHandler::processData(QueryState & state, bool scalar)
{
    initBlockInput(state);

    /// The name of the temporary table for writing data, default to empty string
    auto temporary_id = StorageID::createEmpty();
    readStringBinary(temporary_id.table_name, *in);

    /// Read one block from the network and write it down
    Block block = state.block_in->read();

    if (!block)
        return false;

    if (scalar)
    {
        /// Scalar value
        state.query_context->addScalar(temporary_id.table_name, block);
    }
    else if (!state.need_receive_data_for_insert && !state.need_receive_data_for_input)
    {
        /// Data for external tables

        auto resolved = state.query_context->tryResolveStorageID(temporary_id, Context::ResolveExternal);
        StoragePtr storage;
        /// If such a table does not exist, create it.
        if (resolved)
        {
            storage = DatabaseCatalog::instance().getTable(resolved, state.query_context);
        }
        else
        {
            NamesAndTypesList columns = block.getNamesAndTypesList();
            auto temporary_table = TemporaryTableHolder(state.query_context, ColumnsDescription{columns}, {});
            storage = temporary_table.getTable();
            state.query_context->addExternalTable(temporary_id.table_name, std::move(temporary_table));
        }
        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        /// The data will be written directly to the table.
        QueryPipeline temporary_table_out(storage->write(ASTPtr(), metadata_snapshot, state.query_context, /*async_insert=*/false));
        PushingPipelineExecutor executor(temporary_table_out);
        executor.start();
        executor.push(block);
        executor.finish();
    }
    else if (state.need_receive_data_for_input)
    {
        /// 'input' table function.
        state.block_for_input = block;
    }
    else
    {
        /// INSERT query.
        state.block_for_insert = block;
    }

    return true;
}


bool TCPHandler::processUnexpectedData()
{
    String skip_external_table_name;
    readStringBinary(skip_external_table_name, *in);

    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    if (last_block_in.compression == Protocol::Compression::Enable)
        maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in, /* allow_different_codecs */ true, /* external_data */ query_kind != ClientInfo::QueryKind::SECONDARY_QUERY);
    else
        maybe_compressed_in = in;

    auto skip_block_in = std::make_shared<NativeReader>(*maybe_compressed_in, client_tcp_protocol_version);
    bool empty_block = !skip_block_in->read();
    return !empty_block;
}


void TCPHandler::initBlockInput(QueryState & state)
{
    if (!state.block_in)
    {
        /// 'allow_different_codecs' is set to true, because some parts of compressed data can be precompressed in advance
        /// with another codec that the rest of the data. Example: data sent by Distributed tables.

        if (state.compression == Protocol::Compression::Enable)
            state.maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in, /* allow_different_codecs */ true, /* external_data */ query_kind != ClientInfo::QueryKind::SECONDARY_QUERY);
        else
            state.maybe_compressed_in = in;

        Block header;
        if (state.io.pipeline.pushing())
            header = state.io.pipeline.getHeader();
        else if (state.need_receive_data_for_input)
            header = state.input_header;

        state.block_in = std::make_unique<NativeReader>(
            *state.maybe_compressed_in,
            header,
            client_tcp_protocol_version,
            getFormatSettings(state.query_context));
    }
}


void TCPHandler::initBlockOutput(QueryState & state, const Block & block)
{
    if (!state.block_out)
    {
        const Settings & query_settings = state.query_context->getSettingsRef();
        if (!state.maybe_compressed_out)
        {
            std::string method = Poco::toUpper(query_settings[Setting::network_compression_method].toString());
            std::optional<int> level;
            if (method == "ZSTD")
                level = query_settings[Setting::network_zstd_compression_level];

            if (state.compression == Protocol::Compression::Enable)
            {
                CompressionCodecFactory::instance().validateCodec(
                    method,
                    level,
                    !query_settings[Setting::allow_suspicious_codecs],
                    query_settings[Setting::allow_experimental_codecs],
                    query_settings[Setting::enable_deflate_qpl_codec],
                    query_settings[Setting::enable_zstd_qat_codec]);

                state.maybe_compressed_out = std::make_shared<CompressedWriteBuffer>(
                    *out, CompressionCodecFactory::instance().get(method, level));
            }
            else
                state.maybe_compressed_out = out;
        }

        state.block_out = std::make_unique<NativeWriter>(
            *state.maybe_compressed_out,
            client_tcp_protocol_version,
            block.cloneEmpty(),
            getFormatSettings(state.query_context),
            !query_settings[Setting::low_cardinality_allow_in_native_format]);
    }
}


void TCPHandler::initLogsBlockOutput(QueryState & state, const Block & block)
{
    if (!state.logs_block_out)
    {
        /// Use uncompressed stream since log blocks usually contain only one row
        const Settings & query_settings = state.query_context->getSettingsRef();
        state.logs_block_out = std::make_unique<NativeWriter>(
            *out, client_tcp_protocol_version, block.cloneEmpty(), getFormatSettings(state.query_context), !query_settings[Setting::low_cardinality_allow_in_native_format]);
    }
}


void TCPHandler::initProfileEventsBlockOutput(QueryState & state, const Block & block)
{
    if (!state.profile_events_block_out)
    {
        const Settings & query_settings = state.query_context->getSettingsRef();
        state.profile_events_block_out = std::make_unique<NativeWriter>(
            *out, client_tcp_protocol_version, block.cloneEmpty(), getFormatSettings(state.query_context), !query_settings[Setting::low_cardinality_allow_in_native_format]);
    }
}

void TCPHandler::checkIfQueryCanceled(QueryState & state)
{
    if (state.stop_query)
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT, "Packet 'Cancel' has been received from the client, canceling the query.");
}

void TCPHandler::processCancel(QueryState & state, bool throw_exception)
{
    if (state.allow_partial_result_on_first_cancel && !state.stop_read_return_partial_result)
    {
        state.stop_read_return_partial_result = true;
        LOG_INFO(log, "Received 'Cancel' packet from the client, returning partial result.");
            return;
    }

    state.read_all_data = true;
    state.stop_query = true;

    if (throw_exception)
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT, "Received 'Cancel' packet from the client, canceling the query.");
    else
        LOG_INFO(log, "Received 'Cancel' packet from the client. Queries callbacks return nothing.");
}

void TCPHandler::receivePacketsExpectCancel(QueryState & state)
{
    if (after_check_cancelled.elapsed() / 1000 < interactive_delay)
        return;

    after_check_cancelled.restart();

    /// During request execution the only packet that can come from the client is stopping the query.
    if (in->poll(0))
    {
        if (in->eof())
            throw NetException(ErrorCodes::ABORTED, "Client has dropped the connection, cancel the query.");

        UInt64 packet_type = 0;
        readVarUInt(packet_type, *in);

        switch (packet_type)
        {
            case Protocol::Client::Cancel:
                processCancel(state);
                break;

            default:
                throw NetException(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown packet from client {}", toString(packet_type));
        }
    }
}


void TCPHandler::sendData(QueryState & state, const Block & block)
{
    initBlockOutput(state, block);

    size_t prev_bytes_written_out = out->count();
    size_t prev_bytes_written_compressed_out = state.maybe_compressed_out->count();

    try
    {
        /// For testing hedged requests
        if (unknown_packet_in_send_data)
        {
            constexpr UInt64 marker = (1ULL << 63) - 1;
            --unknown_packet_in_send_data;
            if (unknown_packet_in_send_data == 0)
                writeVarUInt(marker, *out);
        }

        writeVarUInt(Protocol::Server::Data, *out);

        /// For testing hedged requests
        if (block.rows() > 0 && state.query_context->getSettingsRef()[Setting::sleep_in_send_data_ms].totalMilliseconds())
        {
            /// This strange sequence is needed in case of chunked protocol is enabled, in order for client not to
            /// hang on receiving of at least packet type - chunk will not be processed unless either chunk footer
            /// or chunk continuation header is received - first 'next' is sending starting chunk containing packet type
            /// and second 'next' is sending chunk continuation header.
            out->next();
            /// Send external table name (empty name is the main table)
            writeStringBinary("", *out);
            out->next();
            std::chrono::milliseconds ms(state.query_context->getSettingsRef()[Setting::sleep_in_send_data_ms].totalMilliseconds());
            std::this_thread::sleep_for(ms);
        }
        else
        {
            /// Send external table name (empty name is the main table)
            writeStringBinary("", *out);
        }

        state.block_out->write(block);

        if (state.maybe_compressed_out != out)
            state.maybe_compressed_out->next();

        out->finishChunk();
        out->next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        /// In case of unsuccessful write, if the buffer with written data was not flushed,
        ///  we will rollback write to avoid breaking the protocol.
        /// (otherwise the client will not be able to receive exception after unfinished data
        ///  as it will expect the continuation of the data).
        /// It looks like hangs on client side or a message like "Data compressed with different methods".

        if (state.compression == Protocol::Compression::Enable)
        {
            auto extra_bytes_written_compressed = state.maybe_compressed_out->count() - prev_bytes_written_compressed_out;
            if (state.maybe_compressed_out->offset() >= extra_bytes_written_compressed)
                state.maybe_compressed_out->position() -= extra_bytes_written_compressed;
        }

        auto extra_bytes_written_out = out->count() - prev_bytes_written_out;
        if (out->offset() >= extra_bytes_written_out)
            out->position() -= extra_bytes_written_out;

        throw;
    }
}


void TCPHandler::sendLogData(QueryState & state, const Block & block)
{
    initLogsBlockOutput(state, block);

    if (out->isCanceled())
        return;

    writeVarUInt(Protocol::Server::Log, *out);
    /// Send log tag (empty tag is the default tag)
    writeStringBinary("", *out);

    state.logs_block_out->write(block);

    out->finishChunk();
    out->next();
}


void TCPHandler::sendTableColumns(QueryState &, const ColumnsDescription & columns)
{
    writeVarUInt(Protocol::Server::TableColumns, *out);

    /// Send external table name (empty name is the main table)
    writeStringBinary("", *out);
    writeStringBinary(columns.toString(), *out);

    out->finishChunk();
    out->next();
}


void TCPHandler::sendException(const Exception & e, bool with_stack_trace)
{
    if (out->isCanceled())
        return;

    writeVarUInt(Protocol::Server::Exception, *out);
    writeException(e, *out, with_stack_trace);

    out->finishChunk();
    out->next();
}


void TCPHandler::sendEndOfStream(QueryState & state)
{
    state.sent_all_data = true;
    state.io.setAllDataSent();

    writeVarUInt(Protocol::Server::EndOfStream, *out);

    out->finishChunk();
    out->next();
}


void TCPHandler::updateProgress(QueryState & state, const Progress & value)
{
    state.progress.incrementPiecewiseAtomically(value);
}


void TCPHandler::sendProgress(QueryState & state)
{
    writeVarUInt(Protocol::Server::Progress, *out);
    auto increment = state.progress.fetchValuesAndResetPiecewiseAtomically();
    UInt64 current_elapsed_ns = state.watch.elapsedNanoseconds();
    increment.elapsed_ns = current_elapsed_ns - state.prev_elapsed_ns;
    state.prev_elapsed_ns = current_elapsed_ns;
    increment.write(*out, client_tcp_protocol_version);

    out->finishChunk();
    out->next();
}


void TCPHandler::sendLogs(QueryState & state)
{
    if (!state.logs_queue)
        return;

    MutableColumns logs_columns;
    MutableColumns curr_logs_columns;
    size_t rows = 0;

    for (; state.logs_queue->tryPop(curr_logs_columns); ++rows)
    {
        if (rows == 0)
        {
            logs_columns = std::move(curr_logs_columns);
        }
        else
        {
            for (size_t j = 0; j < logs_columns.size(); ++j)
                logs_columns[j]->insertRangeFrom(*curr_logs_columns[j], 0, curr_logs_columns[j]->size());
        }
    }

    if (rows > 0)
    {
        Block block = InternalTextLogsQueue::getSampleBlock();
        block.setColumns(std::move(logs_columns));
        sendLogData(state, block);
    }
}


void TCPHandler::run()
{
    try
    {
        runImpl();
        LOG_DEBUG(log, "Done processing connection.");
    }
    catch (...)
    {
        tryLogCurrentException(log, "TCPHandler");
        throw;
    }
}


Poco::Net::SocketAddress TCPHandler::getClientAddress(const ClientInfo & client_info)
{
    /// Extract the last entry from comma separated list of forwarded_for addresses.
    /// Only the last proxy can be trusted (if any).
    auto forwarded_address = client_info.getLastForwardedFor();
    if (forwarded_address && server.config().getBool("auth_use_forwarded_address", false))
        return *forwarded_address;
    return socket().peerAddress();
}

}
