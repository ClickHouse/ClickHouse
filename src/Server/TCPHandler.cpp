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
#include <Core/ServerSettings.h>
#include <Access/AccessControl.h>
#include <Access/Credentials.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Compression/CompressionFactory.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/thread_local_rng.h>
#include <fmt/format.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>

#if USE_SSL
#   include <Poco/Net/SecureStreamSocket.h>
#   include <Poco/Net/SecureStreamSocketImpl.h>
#endif

#include "Core/Protocol.h"
#include "Storages/MergeTree/RequestResponse.h"
#include "TCPHandler.h"

#include "config_version.h"

using namespace std::literals;
using namespace DB;


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
    extern const int LOGICAL_ERROR;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CLIENT_HAS_CONNECTED_TO_WRONG_PORT;
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int POCO_EXCEPTION;
    extern const int SOCKET_TIMEOUT;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_PROTOCOL;
    extern const int AUTHENTICATION_FAILED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int CLIENT_INFO_DOES_NOT_MATCH;
}

namespace
{
NameToNameMap convertToQueryParameters(const Settings & passed_params)
{
    NameToNameMap query_parameters;
    for (const auto & param : passed_params)
    {
        std::string value;
        ReadBufferFromOwnString buf(param.getValueString());
        readQuoted(value, buf);
        query_parameters.emplace(param.getName(), value);
    }
    return query_parameters;
}

// This function corrects the wrong client_name from the old client.
// Old clients 28.7 and some intermediate versions of 28.7 were sending different ClientInfo.client_name
// "ClickHouse client" was sent with the hello message.
// "ClickHouse" or "ClickHouse " was sent with the query message.
void correctQueryClientInfo(const ClientInfo & session_client_info, ClientInfo & client_info)
{
    if (client_info.getVersionNumber() <= VersionNumber(23, 8, 1) &&
        session_client_info.client_name == "ClickHouse client" &&
        (client_info.client_name == "ClickHouse" || client_info.client_name == "ClickHouse "))
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
}

namespace DB
{

TCPHandler::TCPHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, bool parse_proxy_protocol_, std::string server_display_name_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , parse_proxy_protocol(parse_proxy_protocol_)
    , log(&Poco::Logger::get("TCPHandler"))
    , server_display_name(std::move(server_display_name_))
{
}

TCPHandler::TCPHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, TCPProtocolStackData & stack_data, std::string server_display_name_)
: Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , log(&Poco::Logger::get("TCPHandler"))
    , forwarded_for(stack_data.forwarded_for)
    , certificate(stack_data.certificate)
    , default_database(stack_data.default_database)
    , server_display_name(std::move(server_display_name_))
{
    if (!forwarded_for.empty())
        LOG_TRACE(log, "Forwarded client address: {}", forwarded_for);
}

TCPHandler::~TCPHandler()
{
    try
    {
        state.reset();
        if (out)
            out->next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void TCPHandler::runImpl()
{
    setThreadName("TCPHandler");
    ThreadStatus thread_status;

    extractConnectionSettingsFromContext(server.context());

    socket().setReceiveTimeout(receive_timeout);
    socket().setSendTimeout(send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    /// Support for PROXY protocol
    if (parse_proxy_protocol && !receiveProxyHeader())
        return;

    if (in->eof())
    {
        LOG_INFO(log, "Client has not sent any data.");
        return;
    }

    /// User will be authenticated here. It will also set settings from user profile into connection_context.
    try
    {
        receiveHello();

        /// In interserver mode queries are executed without a session context.
        if (!is_interserver_mode)
            session->makeSessionContext();

        sendHello();
        if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM)
            receiveAddendum();

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
        catch (...) {}

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

        state.reset();

        /// Initialized later.
        std::optional<CurrentThread::QueryScope> query_scope;
        OpenTelemetry::TracingContextHolderPtr thread_trace_context;

        /** An exception during the execution of request (it must be sent over the network to the client).
         *  The client will be able to accept it, if it did not happen while sending another packet and the client has not disconnected yet.
         */
        std::unique_ptr<DB::Exception> exception;
        bool network_error = false;
        bool query_duration_already_logged = false;
        auto log_query_duration = [this, &query_duration_already_logged]()
        {
            if (query_duration_already_logged)
                return;
            query_duration_already_logged = true;
            auto elapsed_sec = state.watch.elapsedSeconds();
            /// We already logged more detailed info if we read some rows
            if (elapsed_sec < 1.0 && state.progress.read_rows)
                return;
            LOG_DEBUG(log, "Processed in {} sec.", elapsed_sec);
        };

        try
        {
            /// If a user passed query-local timeouts, reset socket to initial state at the end of the query
            SCOPE_EXIT({state.timeout_setter.reset();});

            /** If Query - process it. If Ping or Cancel - go back to the beginning.
             *  There may come settings for a separate query that modify `query_context`.
             *  It's possible to receive part uuids packet before the query, so then receivePacket has to be called twice.
             */
            if (!receivePacket())
                continue;

            /** If part_uuids got received in previous packet, trying to read again.
              */
            if (state.empty() && state.part_uuids_to_ignore && !receivePacket())
                continue;

            /// Set up tracing context for this query on current thread
            thread_trace_context = std::make_unique<OpenTelemetry::TracingContextHolder>("TCPHandler",
                query_context->getClientInfo().client_trace_context,
                query_context->getSettingsRef(),
                query_context->getOpenTelemetrySpanLog());
            thread_trace_context->root_span.kind = OpenTelemetry::SERVER;

            query_scope.emplace(query_context, /* fatal_error_callback */ [this]
            {
                std::lock_guard lock(fatal_error_mutex);
                sendLogs();
            });

            /// If query received, then settings in query_context has been updated.
            /// So it's better to update the connection settings for flexibility.
            extractConnectionSettingsFromContext(query_context);

            /// Sync timeouts on client and server during current query to avoid dangling queries on server
            /// NOTE: We use send_timeout for the receive timeout and vice versa (change arguments ordering in TimeoutSetter),
            ///  because send_timeout is client-side setting which has opposite meaning on the server side.
            /// NOTE: these settings are applied only for current connection (not for distributed tables' connections)
            state.timeout_setter = std::make_unique<TimeoutSetter>(socket(), receive_timeout, send_timeout);

            /// Should we send internal logs to client?
            const auto client_logs_level = query_context->getSettingsRef().send_logs_level;
            if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_LOGS
                && client_logs_level != LogsLevel::none)
            {
                state.logs_queue = std::make_shared<InternalTextLogsQueue>();
                state.logs_queue->max_priority = Poco::Logger::parseLevel(client_logs_level.toString());
                state.logs_queue->setSourceRegexp(query_context->getSettingsRef().send_logs_source_regexp);
                CurrentThread::attachInternalTextLogsQueue(state.logs_queue, client_logs_level);
            }
            if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS)
            {
                state.profile_queue = std::make_shared<InternalProfileEventsQueue>(std::numeric_limits<int>::max());
                CurrentThread::attachInternalProfileEventsQueue(state.profile_queue);
            }

            query_context->setExternalTablesInitializer([this] (ContextPtr context)
            {
                if (context != query_context)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in external tables initializer");

                /// Get blocks of temporary tables
                readData();

                /// Reset the input stream, as we received an empty block while receiving external table data.
                /// So, the stream has been marked as cancelled and we can't read from it anymore.
                state.block_in.reset();
                state.maybe_compressed_in.reset(); /// For more accurate accounting by MemoryTracker.
            });

            /// Send structure of columns to client for function input()
            query_context->setInputInitializer([this] (ContextPtr context, const StoragePtr & input_storage)
            {
                if (context != query_context)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in Input initializer");

                auto metadata_snapshot = input_storage->getInMemoryMetadataPtr();
                state.need_receive_data_for_input = true;

                /// Send ColumnsDescription for input storage.
                if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA
                    && query_context->getSettingsRef().input_format_defaults_for_omitted_fields)
                {
                    sendTableColumns(metadata_snapshot->getColumns());
                }

                /// Send block to the client - input storage structure.
                state.input_header = metadata_snapshot->getSampleBlock();
                sendData(state.input_header);
                sendTimezone();
            });

            query_context->setInputBlocksReaderCallback([this] (ContextPtr context) -> Block
            {
                if (context != query_context)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in InputBlocksReader");

                if (!readDataNext())
                {
                    state.block_in.reset();
                    state.maybe_compressed_in.reset();
                    return Block();
                }
                return state.block_for_input;
            });

            customizeContext(query_context);

            /// This callback is needed for requesting read tasks inside pipeline for distributed processing
            query_context->setReadTaskCallback([this]() -> String
            {
                Stopwatch watch;
                CurrentMetrics::Increment callback_metric_increment(CurrentMetrics::ReadTaskRequestsSent);

                std::lock_guard lock(task_callback_mutex);

                if (state.cancellation_status == CancellationStatus::FULLY_CANCELLED)
                    return {};

                sendReadTaskRequestAssumeLocked();
                ProfileEvents::increment(ProfileEvents::ReadTaskRequestsSent);
                auto res = receiveReadTaskResponseAssumeLocked();
                ProfileEvents::increment(ProfileEvents::ReadTaskRequestsSentElapsedMicroseconds, watch.elapsedMicroseconds());
                return res;
            });

            query_context->setMergeTreeAllRangesCallback([this](InitialAllRangesAnnouncement announcement)
            {
                Stopwatch watch;
                CurrentMetrics::Increment callback_metric_increment(CurrentMetrics::MergeTreeAllRangesAnnouncementsSent);
                std::lock_guard lock(task_callback_mutex);

                if (state.cancellation_status == CancellationStatus::FULLY_CANCELLED)
                    return;

                sendMergeTreeAllRangesAnnounecementAssumeLocked(announcement);
                ProfileEvents::increment(ProfileEvents::MergeTreeAllRangesAnnouncementsSent);
                ProfileEvents::increment(ProfileEvents::MergeTreeAllRangesAnnouncementsSentElapsedMicroseconds, watch.elapsedMicroseconds());
            });

            query_context->setMergeTreeReadTaskCallback([this](ParallelReadRequest request) -> std::optional<ParallelReadResponse>
            {
                Stopwatch watch;
                CurrentMetrics::Increment callback_metric_increment(CurrentMetrics::MergeTreeReadTaskRequestsSent);
                std::lock_guard lock(task_callback_mutex);

                if (state.cancellation_status == CancellationStatus::FULLY_CANCELLED)
                    return std::nullopt;

                sendMergeTreeReadTaskRequestAssumeLocked(std::move(request));
                ProfileEvents::increment(ProfileEvents::MergeTreeReadTaskRequestsSent);
                auto res = receivePartitionMergeTreeReadTaskResponseAssumeLocked();
                ProfileEvents::increment(ProfileEvents::MergeTreeReadTaskRequestsSentElapsedMicroseconds, watch.elapsedMicroseconds());
                return res;
            });

            /// Processing Query
            state.io = executeQuery(state.query, query_context, false, state.stage);

            after_check_cancelled.restart();
            after_send_progress.restart();

            auto finish_or_cancel = [this]()
            {
                if (state.cancellation_status == CancellationStatus::FULLY_CANCELLED)
                    state.io.onCancelOrConnectionLoss();
                else
                    state.io.onFinish();
            };

            if (state.io.pipeline.pushing())
            {
                /// FIXME: check explicitly that insert query suggests to receive data via native protocol,
                state.need_receive_data_for_insert = true;
                processInsertQuery();
                finish_or_cancel();
            }
            else if (state.io.pipeline.pulling())
            {
                processOrdinaryQueryWithProcessors();
                finish_or_cancel();
            }
            else if (state.io.pipeline.completed())
            {
                {
                    CompletedPipelineExecutor executor(state.io.pipeline);

                    /// Should not check for cancel in case of input.
                    if (!state.need_receive_data_for_input)
                    {
                        auto callback = [this]()
                        {
                            std::scoped_lock lock(task_callback_mutex, fatal_error_mutex);

                            if (getQueryCancellationStatus() == CancellationStatus::FULLY_CANCELLED)
                                return true;

                            sendProgress();
                            sendSelectProfileEvents();
                            sendLogs();

                            return false;
                        };

                        executor.setCancelCallback(callback, interactive_delay / 1000);
                    }
                    executor.execute();
                }

                finish_or_cancel();

                std::lock_guard lock(task_callback_mutex);

                /// Send final progress after calling onFinish(), since it will update the progress.
                ///
                /// NOTE: we cannot send Progress for regular INSERT (with VALUES)
                /// without breaking protocol compatibility, but it can be done
                /// by increasing revision.
                sendProgress();
                sendSelectProfileEvents();
            }
            else
            {
                finish_or_cancel();
            }

            /// Do it before sending end of stream, to have a chance to show log message in client.
            query_scope->logPeakMemoryUsage();
            log_query_duration();

            if (state.is_connection_closed)
                break;

            {
                std::lock_guard lock(task_callback_mutex);
                sendLogs();
                sendEndOfStream();
            }

            /// QueryState should be cleared before QueryScope, since otherwise
            /// the MemoryTracker will be wrong for possible deallocations.
            /// (i.e. deallocations from the Aggregator with two-level aggregation)
            state.reset();
            last_sent_snapshots = ProfileEvents::ThreadIdToCountersSnapshot{};
            query_scope.reset();
            thread_trace_context.reset();
        }
        catch (const Exception & e)
        {
            state.io.onException();
            exception.reset(e.clone());

            if (e.code() == ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT)
                throw;

            /// If there is UNEXPECTED_PACKET_FROM_CLIENT emulate network_error
            /// to break the loop, but do not throw to send the exception to
            /// the client.
            if (e.code() == ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT)
                network_error = true;

            /// If a timeout occurred, try to inform client about it and close the session
            if (e.code() == ErrorCodes::SOCKET_TIMEOUT)
                network_error = true;

            if (network_error)
                LOG_TEST(log, "Going to close connection due to exception: {}", e.message());
        }
        catch (const Poco::Net::NetException & e)
        {
            /** We can get here if there was an error during connection to the client,
             *  or in connection with a remote server that was used to process the request.
             *  It is not possible to distinguish between these two cases.
             *  Although in one of them, we have to send exception to the client, but in the other - we can not.
             *  We will try to send exception to the client in any case - see below.
             */
            state.io.onException();
            exception = std::make_unique<DB::Exception>(Exception::CreateFromPocoTag{}, e);
        }
        catch (const Poco::Exception & e)
        {
            state.io.onException();
            exception = std::make_unique<DB::Exception>(Exception::CreateFromPocoTag{}, e);
        }
// Server should die on std logic errors in debug, like with assert()
// or ErrorCodes::LOGICAL_ERROR. This helps catch these errors in
// tests.
#ifdef ABORT_ON_LOGICAL_ERROR
        catch (const std::logic_error & e)
        {
            state.io.onException();
            exception = std::make_unique<DB::Exception>(Exception::CreateFromSTDTag{}, e);
            sendException(*exception, send_exception_with_stack_trace);
            std::abort();
        }
#endif
        catch (const std::exception & e)
        {
            state.io.onException();
            exception = std::make_unique<DB::Exception>(Exception::CreateFromSTDTag{}, e);
        }
        catch (...)
        {
            state.io.onException();
            exception = std::make_unique<DB::Exception>(ErrorCodes::UNKNOWN_EXCEPTION, "Unknown exception");
        }

        try
        {
            if (exception)
            {
                if (thread_trace_context)
                    thread_trace_context->root_span.addAttribute(*exception);

                try
                {
                    /// Try to send logs to client, but it could be risky too
                    /// Assume that we can't break output here
                    sendLogs();
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Can't send logs to client");
                }

                const auto & e = *exception;
                LOG_ERROR(log, getExceptionMessageAndPattern(e, send_exception_with_stack_trace));
                sendException(*exception, send_exception_with_stack_trace);
            }
        }
        catch (...)
        {
            /** Could not send exception information to the client. */
            network_error = true;
            LOG_WARNING(log, "Client has gone away.");
        }

        try
        {
            /// A query packet is always followed by one or more data packets.
            /// If some of those data packets are left, try to skip them.
            if (exception && !state.empty() && !state.read_all_data)
                skipData();
        }
        catch (...)
        {
            network_error = true;
            LOG_WARNING(log, "Can't skip data packets after query failure.");
        }

        log_query_duration();

        /// QueryState should be cleared before QueryScope, since otherwise
        /// the MemoryTracker will be wrong for possible deallocations.
        /// (i.e. deallocations from the Aggregator with two-level aggregation)
        state.reset();
        query_scope.reset();
        thread_trace_context.reset();

        /// It is important to destroy query context here. We do not want it to live arbitrarily longer than the query.
        query_context.reset();

        if (is_interserver_mode)
        {
            /// We don't really have session in interserver mode, new one is created for each query. It's better to reset it now.
            session.reset();
        }

        if (network_error)
            break;
    }
}


void TCPHandler::extractConnectionSettingsFromContext(const ContextPtr & context)
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
    sleep_after_receiving_query = settings.sleep_after_receiving_query_ms;
}


bool TCPHandler::readDataNext()
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);

    /// Poll interval should not be greater than receive_timeout
    constexpr UInt64 min_timeout_us = 5000; // 5 ms
    UInt64 timeout_us = std::max(min_timeout_us, std::min(poll_interval * 1000000, static_cast<UInt64>(receive_timeout.totalMicroseconds())));
    bool read_ok = false;

    /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
    while (true)
    {
        if (static_cast<ReadBufferFromPocoSocket &>(*in).poll(timeout_us))
        {
            /// If client disconnected.
            if (in->eof())
            {
                LOG_INFO(log, "Client has dropped the connection, cancel the query.");
                state.is_connection_closed = true;
                state.cancellation_status = CancellationStatus::FULLY_CANCELLED;
                break;
            }

            /// We accept and process data.
            read_ok = receivePacket();
            break;
        }

        /// Do we need to shut down?
        if (server.isCancelled())
            break;

        /** Have we waited for data for too long?
         *  If we periodically poll, the receive_timeout of the socket itself does not work.
         *  Therefore, an additional check is added.
         */
        Float64 elapsed = watch.elapsedSeconds();
        if (elapsed > static_cast<Float64>(receive_timeout.totalSeconds()))
        {
            throw Exception(ErrorCodes::SOCKET_TIMEOUT,
                            "Timeout exceeded while receiving data from client. Waited for {} seconds, timeout is {} seconds.",
                            static_cast<size_t>(elapsed), receive_timeout.totalSeconds());
        }
    }

    if (read_ok)
    {
        sendLogs();
        sendInsertProfileEvents();
    }
    else
        state.read_all_data = true;

    return read_ok;
}


void TCPHandler::readData()
{
    sendLogs();

    while (readDataNext())
        ;

    if (state.cancellation_status == CancellationStatus::FULLY_CANCELLED)
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
}


void TCPHandler::skipData()
{
    state.skipping_data = true;
    SCOPE_EXIT({ state.skipping_data = false; });

    while (readDataNext())
        ;

    if (state.cancellation_status == CancellationStatus::FULLY_CANCELLED)
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
}


void TCPHandler::processInsertQuery()
{
    size_t num_threads = state.io.pipeline.getNumThreads();

    auto run_executor = [&](auto & executor)
    {
        /// Made above the rest of the lines,
        /// so that in case of `writePrefix` function throws an exception,
        /// client receive exception before sending data.
        executor.start();

        /// Send ColumnsDescription for insertion table
        if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA)
        {
            const auto & table_id = query_context->getInsertionTable();
            if (query_context->getSettingsRef().input_format_defaults_for_omitted_fields)
            {
                if (!table_id.empty())
                {
                    auto storage_ptr = DatabaseCatalog::instance().getTable(table_id, query_context);
                    sendTableColumns(storage_ptr->getInMemoryMetadataPtr()->getColumns());
                }
            }
        }

        /// Send block to the client - table structure.
        sendData(executor.getHeader());
        sendLogs();

        while (readDataNext())
            executor.push(std::move(state.block_for_insert));

        if (state.cancellation_status == CancellationStatus::FULLY_CANCELLED)
            executor.cancel();
        else
            executor.finish();
    };

    if (num_threads > 1)
    {
        PushingAsyncPipelineExecutor executor(state.io.pipeline);
        run_executor(executor);
    }
    else
    {
        PushingPipelineExecutor executor(state.io.pipeline);
        run_executor(executor);
    }

    sendInsertProfileEvents();
}


void TCPHandler::processOrdinaryQueryWithProcessors()
{
    auto & pipeline = state.io.pipeline;

    if (query_context->getSettingsRef().allow_experimental_query_deduplication)
    {
        std::lock_guard lock(task_callback_mutex);
        sendPartUUIDs();
    }

    /// Send header-block, to allow client to prepare output format for data to send.
    {
        const auto & header = pipeline.getHeader();

        if (header)
        {
            std::lock_guard lock(task_callback_mutex);
            sendData(header);
        }
    }

    /// Defer locking to cover a part of the scope below and everything after it
    std::unique_lock progress_lock(task_callback_mutex, std::defer_lock);

    {
        PullingAsyncPipelineExecutor executor(pipeline);
        CurrentMetrics::Increment query_thread_metric_increment{CurrentMetrics::QueryThread};

        Block block;
        while (executor.pull(block, interactive_delay / 1000))
        {
            std::unique_lock lock(task_callback_mutex);

            auto cancellation_status = getQueryCancellationStatus();
            if (cancellation_status == CancellationStatus::FULLY_CANCELLED)
            {
                /// Several callback like callback for parallel reading could be called from inside the pipeline
                /// and we have to unlock the mutex from our side to prevent deadlock.
                lock.unlock();
                /// A packet was received requesting to stop execution of the request.
                executor.cancel();
                break;
            }
            else if (cancellation_status == CancellationStatus::READ_CANCELLED)
            {
                executor.cancelReading();
            }

            if (after_send_progress.elapsed() / 1000 >= interactive_delay)
            {
                /// Some time passed and there is a progress.
                after_send_progress.restart();
                sendProgress();
                sendSelectProfileEvents();
            }

            sendLogs();

            if (block)
            {
                if (!state.io.null_format)
                    sendData(block);
            }
        }

        /// This lock wasn't acquired before and we make .lock() call here
        /// so everything under this line is covered even together
        /// with sendProgress() out of the scope
        progress_lock.lock();

        /** If data has run out, we will send the profiling data and total values to
          * the last zero block to be able to use
          * this information in the suffix output of stream.
          * If the request was interrupted, then `sendTotals` and other methods could not be called,
          *  because we have not read all the data yet,
          *  and there could be ongoing calculations in other threads at the same time.
          */
        if (getQueryCancellationStatus() != CancellationStatus::FULLY_CANCELLED)
        {
            sendTotals(executor.getTotalsBlock());
            sendExtremes(executor.getExtremesBlock());
            sendProfileInfo(executor.getProfileInfo());
            sendProgress();
            sendLogs();
            sendSelectProfileEvents();
        }

        if (state.is_connection_closed)
            return;

        sendData({});
        last_sent_snapshots.clear();
    }

    sendProgress();
}


void TCPHandler::processTablesStatusRequest()
{
    TablesStatusRequest request;
    request.read(*in, client_tcp_protocol_version);

    ContextPtr context_to_resolve_table_names;
    if (is_interserver_mode)
    {
        /// In interserver mode session context does not exists, because authentication is done for each query.
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
}

void TCPHandler::receiveUnexpectedTablesStatusRequest()
{
    TablesStatusRequest skip_request;
    skip_request.read(*in, client_tcp_protocol_version);

    throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet TablesStatusRequest received from client");
}

void TCPHandler::sendPartUUIDs()
{
    auto uuids = query_context->getPartUUIDs()->get();
    if (!uuids.empty())
    {
        for (const auto & uuid : uuids)
            LOG_TRACE(log, "Sending UUID: {}", toString(uuid));

        writeVarUInt(Protocol::Server::PartUUIDs, *out);
        writeVectorBinary(uuids, *out);
        out->next();
    }
}


void TCPHandler::sendReadTaskRequestAssumeLocked()
{
    writeVarUInt(Protocol::Server::ReadTaskRequest, *out);
    out->next();
}


void TCPHandler::sendMergeTreeAllRangesAnnounecementAssumeLocked(InitialAllRangesAnnouncement announcement)
{
    writeVarUInt(Protocol::Server::MergeTreeAllRangesAnnounecement, *out);
    announcement.serialize(*out);
    out->next();
}


void TCPHandler::sendMergeTreeReadTaskRequestAssumeLocked(ParallelReadRequest request)
{
    writeVarUInt(Protocol::Server::MergeTreeReadTaskRequest, *out);
    request.serialize(*out);
    out->next();
}


void TCPHandler::sendProfileInfo(const ProfileInfo & info)
{
    writeVarUInt(Protocol::Server::ProfileInfo, *out);
    info.write(*out);
    out->next();
}


void TCPHandler::sendTotals(const Block & totals)
{
    if (totals)
    {
        initBlockOutput(totals);

        writeVarUInt(Protocol::Server::Totals, *out);
        writeStringBinary("", *out);

        state.block_out->write(totals);
        state.maybe_compressed_out->next();
        out->next();
    }
}


void TCPHandler::sendExtremes(const Block & extremes)
{
    if (extremes)
    {
        initBlockOutput(extremes);

        writeVarUInt(Protocol::Server::Extremes, *out);
        writeStringBinary("", *out);

        state.block_out->write(extremes);
        state.maybe_compressed_out->next();
        out->next();
    }
}

void TCPHandler::sendProfileEvents()
{
    Block block;
    ProfileEvents::getProfileEvents(server_display_name, state.profile_queue, block, last_sent_snapshots);
    if (block.rows() != 0)
    {
        initProfileEventsBlockOutput(block);

        writeVarUInt(Protocol::Server::ProfileEvents, *out);
        writeStringBinary("", *out);

        state.profile_events_block_out->write(block);
        out->next();
    }
}

void TCPHandler::sendSelectProfileEvents()
{
    if (client_tcp_protocol_version < DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS)
        return;

    sendProfileEvents();
}

void TCPHandler::sendInsertProfileEvents()
{
    if (client_tcp_protocol_version < DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT)
        return;
    if (query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        return;

    sendProfileEvents();
}

void TCPHandler::sendTimezone()
{
    if (client_tcp_protocol_version < DBMS_MIN_PROTOCOL_VERSION_WITH_TIMEZONE_UPDATES)
        return;

    const String & tz = query_context->getSettingsRef().session_timezone.value;

    LOG_DEBUG(log, "TCPHandler::sendTimezone(): {}", tz);
    writeVarUInt(Protocol::Server::TimezoneUpdate, *out);
    writeStringBinary(tz, *out);
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

    LimitReadBuffer limit_in(*in, 107, /* trow_exception */ true, /* exact_limit */ {}); /// Maximum length from the specs.

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

        ++limit_in.position();
        assertChar(' ', limit_in);

        /// Read the first field and ignore other.
        readStringUntilWhitespace(forwarded_address, limit_in);

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
            throw Exception(ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT, "Client has connected to wrong port");
        }
        else
            throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
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
        throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet from client (no user in Hello package)");

    LOG_DEBUG(log, "Connected {} version {}.{}.{}, revision: {}{}{}.",
        client_name,
        client_version_major, client_version_minor, client_version_patch,
        client_tcp_protocol_version,
        (!default_database.empty() ? ", database: " + default_database : ""),
        (!user.empty() ? ", user: " + user : "")
    );

    is_interserver_mode = (user == USER_INTERSERVER_MARKER) && password.empty();
    if (is_interserver_mode)
    {
        if (client_tcp_protocol_version < DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2)
            LOG_WARNING(LogFrequencyLimiter(log, 10),
                        "Using deprecated interserver protocol because the client is too old. Consider upgrading all nodes in cluster.");
        receiveClusterNameAndSalt();
        return;
    }

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
                    SSLCertificateCredentials{user, secure_socket.peerCertificate().commonName()},
                    getClientAddress(client_info));
                return;
            }
            catch (...)
            {
                tryLogCurrentException(log, "SSL authentication failed, falling back to password authentication");
            }
        }
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
}


void TCPHandler::receiveUnexpectedHello()
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

    throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Hello received from client");
}


void TCPHandler::sendHello()
{
    writeVarUInt(Protocol::Server::Hello, *out);
    writeStringBinary(VERSION_NAME, *out);
    writeVarUInt(VERSION_MAJOR, *out);
    writeVarUInt(VERSION_MINOR, *out);
    writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE)
        writeStringBinary(DateLUT::instance().getTimeZone(), *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME)
        writeStringBinary(server_display_name, *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
        writeVarUInt(VERSION_PATCH, *out);
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
    out->next();
}


bool TCPHandler::receivePacket()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);

    switch (packet_type)
    {
        case Protocol::Client::IgnoredPartUUIDs:
            /// Part uuids packet if any comes before query.
            if (!state.empty() || state.part_uuids_to_ignore)
                receiveUnexpectedIgnoredPartUUIDs();
            receiveIgnoredPartUUIDs();
            return true;

        case Protocol::Client::Query:
            if (!state.empty())
                receiveUnexpectedQuery();
            receiveQuery();
            return true;

        case Protocol::Client::Data:
        case Protocol::Client::Scalar:
            if (state.skipping_data)
                return receiveUnexpectedData(false);
            if (state.empty())
                receiveUnexpectedData(true);
            return receiveData(packet_type == Protocol::Client::Scalar);

        case Protocol::Client::Ping:
            writeVarUInt(Protocol::Server::Pong, *out);
            out->next();
            return false;

        case Protocol::Client::Cancel:
            decreaseCancellationStatus("Received 'Cancel' packet from the client, canceling the query.");
            return false;

        case Protocol::Client::Hello:
            receiveUnexpectedHello();

        case Protocol::Client::TablesStatusRequest:
            if (!state.empty())
                receiveUnexpectedTablesStatusRequest();
            processTablesStatusRequest();
            out->next();
            return false;

        default:
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown packet {} from client", toString(packet_type));
    }
}


void TCPHandler::receiveIgnoredPartUUIDs()
{
    readVectorBinary(state.part_uuids_to_ignore.emplace(), *in);
}


void TCPHandler::receiveUnexpectedIgnoredPartUUIDs()
{
    std::vector<UUID> skip_part_uuids;
    readVectorBinary(skip_part_uuids, *in);
    throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet IgnoredPartUUIDs received from client");
}


String TCPHandler::receiveReadTaskResponseAssumeLocked()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);
    if (packet_type != Protocol::Client::ReadTaskResponse)
    {
        if (packet_type == Protocol::Client::Cancel)
        {
            decreaseCancellationStatus("Received 'Cancel' packet from the client, canceling the read task.");
            return {};
        }
        else
        {
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Received {} packet after requesting read task",
                    Protocol::Client::toString(packet_type));
        }
    }
    UInt64 version;
    readVarUInt(version, *in);
    if (version != DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Protocol version for distributed processing mismatched");
    String response;
    readStringBinary(response, *in);
    return response;
}


std::optional<ParallelReadResponse> TCPHandler::receivePartitionMergeTreeReadTaskResponseAssumeLocked()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);
    if (packet_type != Protocol::Client::MergeTreeReadTaskResponse)
    {
        if (packet_type == Protocol::Client::Cancel)
        {
            decreaseCancellationStatus("Received 'Cancel' packet from the client, canceling the MergeTree read task.");
            return std::nullopt;
        }
        else
        {
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Received {} packet after requesting read task",
                    Protocol::Client::toString(packet_type));
        }
    }
    ParallelReadResponse response;
    response.deserialize(*in);
    return response;
}


void TCPHandler::receiveClusterNameAndSalt()
{
    readStringBinary(cluster, *in);
    readStringBinary(salt, *in, 32);
}

void TCPHandler::receiveQuery()
{
    UInt64 stage = 0;
    UInt64 compression = 0;

    state.is_empty = false;
    readStringBinary(state.query_id, *in);

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
        if (config_ref.validate_tcp_client_information)
            validateClientInfo(session->getClientInfo(), client_info);
    }

    /// Per query settings are also passed via TCP.
    /// We need to check them before applying due to they can violate the settings constraints.
    auto settings_format = (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS)
        ? SettingsWriteFormat::STRINGS_WITH_FLAGS
        : SettingsWriteFormat::BINARY;
    Settings passed_settings;
    passed_settings.read(*in, settings_format);

    /// Interserver secret.
    std::string received_hash;
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET)
    {
        readStringBinary(received_hash, *in, 32);
    }

    readVarUInt(stage, *in);
    state.stage = QueryProcessingStage::Enum(stage);

    readVarUInt(compression, *in);
    state.compression = static_cast<Protocol::Compression>(compression);
    last_block_in.compression = state.compression;

    readStringBinary(state.query, *in);

    Settings passed_params;
    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS)
        passed_params.read(*in, settings_format);

    if (is_interserver_mode)
    {
        client_info.interface = ClientInfo::Interface::TCP_INTERSERVER;
#if USE_SSL
        String cluster_secret = server.context()->getCluster(cluster)->getSecret();

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
        data += state.query;
        data += state.query_id;
        data += client_info.initial_user;

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
            LOG_DEBUG(log, "User (initial, interserver mode): {} (client: {})", client_info.initial_user, getClientAddress(client_info).toString());
            /// In case of inter-server mode authorization is done with the
            /// initial address of the client, not the real address from which
            /// the query was come, since the real address is the address of
            /// the initiator server, while we are interested in client's
            /// address.
            session->authenticate(AlwaysAllowCredentials{client_info.initial_user}, client_info.initial_address);
        }
#else
        auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "Inter-server secret support is disabled, because ClickHouse was built without SSL library");
        session->onAuthenticationFailure(/* user_name */ std::nullopt, socket().peerAddress(), exception);
        throw exception; /// NOLINT
#endif
    }

    query_context = session->makeQueryContext(std::move(client_info));

    /// Sets the default database if it wasn't set earlier for the session context.
    if (is_interserver_mode && !default_database.empty())
        query_context->setCurrentDatabase(default_database);

    if (state.part_uuids_to_ignore)
        query_context->getIgnoredPartUUIDs()->add(*state.part_uuids_to_ignore);

    query_context->setProgressCallback([this] (const Progress & value) { return this->updateProgress(value); });
    query_context->setFileProgressCallback([this](const FileProgress & value) { this->updateProgress(Progress(value)); });

    ///
    /// Settings
    ///
    auto settings_changes = passed_settings.changes();
    query_kind = query_context->getClientInfo().query_kind;
    if (query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        /// Throw an exception if the passed settings violate the constraints.
        query_context->checkSettingsConstraints(settings_changes, SettingSource::QUERY);
    }
    else
    {
        /// Quietly clamp to the constraints if it's not an initial query.
        query_context->clampToSettingsConstraints(settings_changes, SettingSource::QUERY);
    }
    query_context->applySettingsChanges(settings_changes);

    /// Use the received query id, or generate a random default. It is convenient
    /// to also generate the default OpenTelemetry trace id at the same time, and
    /// set the trace parent.
    /// Notes:
    /// 1) ClientInfo might contain upstream trace id, so we decide whether to use
    /// the default ids after we have received the ClientInfo.
    /// 2) There is the opentelemetry_start_trace_probability setting that
    /// controls when we start a new trace. It can be changed via Native protocol,
    /// so we have to apply the changes first.
    query_context->setCurrentQueryId(state.query_id);

    query_context->addQueryParameters(convertToQueryParameters(passed_params));

    /// For testing hedged requests
    if (unlikely(sleep_after_receiving_query.totalMilliseconds()))
    {
        std::chrono::milliseconds ms(sleep_after_receiving_query.totalMilliseconds());
        std::this_thread::sleep_for(ms);
    }
}

void TCPHandler::receiveUnexpectedQuery()
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

    throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Query received from client");
}

bool TCPHandler::receiveData(bool scalar)
{
    initBlockInput();

    /// The name of the temporary table for writing data, default to empty string
    auto temporary_id = StorageID::createEmpty();
    readStringBinary(temporary_id.table_name, *in);

    /// Read one block from the network and write it down
    Block block = state.block_in->read();

    if (!block)
    {
        state.read_all_data = true;
        return false;
    }

    if (scalar)
    {
        /// Scalar value
        query_context->addScalar(temporary_id.table_name, block);
    }
    else if (!state.need_receive_data_for_insert && !state.need_receive_data_for_input)
    {
        /// Data for external tables

        auto resolved = query_context->tryResolveStorageID(temporary_id, Context::ResolveExternal);
        StoragePtr storage;
        /// If such a table does not exist, create it.
        if (resolved)
        {
            storage = DatabaseCatalog::instance().getTable(resolved, query_context);
        }
        else
        {
            NamesAndTypesList columns = block.getNamesAndTypesList();
            auto temporary_table = TemporaryTableHolder(query_context, ColumnsDescription{columns}, {});
            storage = temporary_table.getTable();
            query_context->addExternalTable(temporary_id.table_name, std::move(temporary_table));
        }
        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        /// The data will be written directly to the table.
        QueryPipeline temporary_table_out(storage->write(ASTPtr(), metadata_snapshot, query_context, /*async_insert=*/false));
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


bool TCPHandler::receiveUnexpectedData(bool throw_exception)
{
    String skip_external_table_name;
    readStringBinary(skip_external_table_name, *in);

    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    if (last_block_in.compression == Protocol::Compression::Enable)
        maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in, /* allow_different_codecs */ true);
    else
        maybe_compressed_in = in;

    auto skip_block_in = std::make_shared<NativeReader>(*maybe_compressed_in, client_tcp_protocol_version);
    bool read_ok = !!skip_block_in->read();

    if (!read_ok)
        state.read_all_data = true;

    if (throw_exception)
        throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Data received from client");

    return read_ok;
}

void TCPHandler::initBlockInput()
{
    if (!state.block_in)
    {
        /// 'allow_different_codecs' is set to true, because some parts of compressed data can be precompressed in advance
        /// with another codec that the rest of the data. Example: data sent by Distributed tables.

        if (state.compression == Protocol::Compression::Enable)
            state.maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in, /* allow_different_codecs */ true);
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
            client_tcp_protocol_version);
    }
}


void TCPHandler::initBlockOutput(const Block & block)
{
    if (!state.block_out)
    {
        const Settings & query_settings = query_context->getSettingsRef();
        if (!state.maybe_compressed_out)
        {
            std::string method = Poco::toUpper(query_settings.network_compression_method.toString());
            std::optional<int> level;
            if (method == "ZSTD")
                level = query_settings.network_zstd_compression_level;

            if (state.compression == Protocol::Compression::Enable)
            {
                CompressionCodecFactory::instance().validateCodec(method, level, !query_settings.allow_suspicious_codecs, query_settings.allow_experimental_codecs, query_settings.enable_deflate_qpl_codec);

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
            !query_settings.low_cardinality_allow_in_native_format);
    }
}

void TCPHandler::initLogsBlockOutput(const Block & block)
{
    if (!state.logs_block_out)
    {
        /// Use uncompressed stream since log blocks usually contain only one row
        const Settings & query_settings = query_context->getSettingsRef();
        state.logs_block_out = std::make_unique<NativeWriter>(
            *out,
            client_tcp_protocol_version,
            block.cloneEmpty(),
            !query_settings.low_cardinality_allow_in_native_format);
    }
}


void TCPHandler::initProfileEventsBlockOutput(const Block & block)
{
    if (!state.profile_events_block_out)
    {
        const Settings & query_settings = query_context->getSettingsRef();
        state.profile_events_block_out = std::make_unique<NativeWriter>(
            *out,
            client_tcp_protocol_version,
            block.cloneEmpty(),
            !query_settings.low_cardinality_allow_in_native_format);
    }
}

void TCPHandler::decreaseCancellationStatus(const std::string & log_message)
{
    auto prev_status = magic_enum::enum_name(state.cancellation_status);

    bool partial_result_on_first_cancel = false;
    if (query_context)
    {
        const auto & settings = query_context->getSettingsRef();
        partial_result_on_first_cancel = settings.partial_result_on_first_cancel;
    }

    if (partial_result_on_first_cancel && state.cancellation_status == CancellationStatus::NOT_CANCELLED)
    {
        state.cancellation_status = CancellationStatus::READ_CANCELLED;
    }
    else
    {
        state.cancellation_status = CancellationStatus::FULLY_CANCELLED;
    }

    auto current_status = magic_enum::enum_name(state.cancellation_status);
    LOG_INFO(log, "Change cancellation status from {} to {}. Log message: {}", prev_status, current_status, log_message);
}

QueryState::CancellationStatus TCPHandler::getQueryCancellationStatus()
{
    if (state.cancellation_status == CancellationStatus::FULLY_CANCELLED || state.sent_all_data)
        return CancellationStatus::FULLY_CANCELLED;

    if (after_check_cancelled.elapsed() / 1000 < interactive_delay)
        return state.cancellation_status;

    after_check_cancelled.restart();

    /// During request execution the only packet that can come from the client is stopping the query.
    if (static_cast<ReadBufferFromPocoSocket &>(*in).poll(0))
    {
        if (in->eof())
        {
            LOG_INFO(log, "Client has dropped the connection, cancel the query.");
            state.cancellation_status = CancellationStatus::FULLY_CANCELLED;
            state.is_connection_closed = true;
            return CancellationStatus::FULLY_CANCELLED;
        }

        UInt64 packet_type = 0;
        readVarUInt(packet_type, *in);

        switch (packet_type)
        {
            case Protocol::Client::Cancel:
                if (state.empty())
                    throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Cancel received from client");

                decreaseCancellationStatus("Query was cancelled.");

                return state.cancellation_status;

            default:
                throw NetException(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown packet from client {}", toString(packet_type));
        }
    }

    return state.cancellation_status;
}


void TCPHandler::sendData(const Block & block)
{
    initBlockOutput(block);

    size_t prev_bytes_written_out = out->count();
    size_t prev_bytes_written_compressed_out = state.maybe_compressed_out->count();

    try
    {
        /// For testing hedged requests
        if (unknown_packet_in_send_data)
        {
            constexpr UInt64 marker = (1ULL<<63) - 1;
            --unknown_packet_in_send_data;
            if (unknown_packet_in_send_data == 0)
                writeVarUInt(marker, *out);
        }

        writeVarUInt(Protocol::Server::Data, *out);
        /// Send external table name (empty name is the main table)
        writeStringBinary("", *out);

        /// For testing hedged requests
        if (block.rows() > 0 && query_context->getSettingsRef().sleep_in_send_data_ms.totalMilliseconds())
        {
            out->next();
            std::chrono::milliseconds ms(query_context->getSettingsRef().sleep_in_send_data_ms.totalMilliseconds());
            std::this_thread::sleep_for(ms);
        }

        state.block_out->write(block);
        state.maybe_compressed_out->next();
        out->next();
    }
    catch (...)
    {
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


void TCPHandler::sendLogData(const Block & block)
{
    initLogsBlockOutput(block);

    writeVarUInt(Protocol::Server::Log, *out);
    /// Send log tag (empty tag is the default tag)
    writeStringBinary("", *out);

    state.logs_block_out->write(block);
    out->next();
}

void TCPHandler::sendTableColumns(const ColumnsDescription & columns)
{
    writeVarUInt(Protocol::Server::TableColumns, *out);

    /// Send external table name (empty name is the main table)
    writeStringBinary("", *out);
    writeStringBinary(columns.toString(), *out);

    out->next();
}

void TCPHandler::sendException(const Exception & e, bool with_stack_trace)
{
    state.io.setAllDataSent();

    writeVarUInt(Protocol::Server::Exception, *out);
    writeException(e, *out, with_stack_trace);
    out->next();
}


void TCPHandler::sendEndOfStream()
{
    state.sent_all_data = true;
    state.io.setAllDataSent();

    writeVarUInt(Protocol::Server::EndOfStream, *out);
    out->next();
}


void TCPHandler::updateProgress(const Progress & value)
{
    state.progress.incrementPiecewiseAtomically(value);
}


void TCPHandler::sendProgress()
{
    writeVarUInt(Protocol::Server::Progress, *out);
    auto increment = state.progress.fetchValuesAndResetPiecewiseAtomically();
    UInt64 current_elapsed_ns = state.watch.elapsedNanoseconds();
    increment.elapsed_ns = current_elapsed_ns - state.prev_elapsed_ns;
    state.prev_elapsed_ns = current_elapsed_ns;
    increment.write(*out, client_tcp_protocol_version);
    out->next();
}


void TCPHandler::sendLogs()
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
        sendLogData(block);
    }
}


void TCPHandler::run()
{
    try
    {
        runImpl();

        LOG_DEBUG(log, "Done processing connection.");
    }
    catch (Poco::Exception & e)
    {
        /// Timeout - not an error.
        if (e.what() == "Timeout"sv)
        {
            LOG_DEBUG(log, "Poco::Exception. Code: {}, e.code() = {}, e.displayText() = {}, e.what() = {}", ErrorCodes::POCO_EXCEPTION, e.code(), e.displayText(), e.what());
        }
        else
            throw;
    }
}

Poco::Net::SocketAddress TCPHandler::getClientAddress(const ClientInfo & client_info)
{
    /// Extract the last entry from comma separated list of forwarded_for addresses.
    /// Only the last proxy can be trusted (if any).
    String forwarded_address = client_info.getLastForwardedFor();
    if (!forwarded_address.empty() && server.config().getBool("auth_use_forwarded_address", false))
        return Poco::Net::SocketAddress(forwarded_address, socket().peerAddress().port());
    else
        return socket().peerAddress();
}

}
