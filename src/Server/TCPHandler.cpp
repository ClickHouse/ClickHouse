#include <iomanip>
#include <common/scope_guard.h>
#include <Poco/Net/NetException.h>
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
#include <IO/copyData.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/TablesStatus.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/StorageS3Cluster.h>
#include <Core/ExternalTable.h>
#include <Storages/ColumnDefault.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Compression/CompressionFactory.h>
#include <common/logger_useful.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include "Core/Protocol.h"
#include "TCPHandler.h"

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CLIENT_HAS_CONNECTED_TO_WRONG_PORT;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int POCO_EXCEPTION;
    extern const int SOCKET_TIMEOUT;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNKNOWN_PROTOCOL;
}

TCPHandler::TCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_, bool parse_proxy_protocol_, std::string server_display_name_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , parse_proxy_protocol(parse_proxy_protocol_)
    , log(&Poco::Logger::get("TCPHandler"))
    , connection_context(Context::createCopy(server.context()))
    , query_context(Context::createCopy(server.context()))
    , server_display_name(std::move(server_display_name_))
{
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

    connection_context = Context::createCopy(server.context());
    connection_context->makeSessionContext();

    /// These timeouts can be changed after receiving query.

    auto global_receive_timeout = connection_context->getSettingsRef().receive_timeout;
    auto global_send_timeout = connection_context->getSettingsRef().send_timeout;

    socket().setReceiveTimeout(global_receive_timeout);
    socket().setSendTimeout(global_send_timeout);
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
            sendException(e, connection_context->getSettingsRef().calculate_text_stack_trace);
        }
        catch (...) {}

        throw;
    }

    /// When connecting, the default database can be specified.
    if (!default_database.empty())
    {
        if (!DatabaseCatalog::instance().isDatabaseExist(default_database))
        {
            Exception e("Database " + backQuote(default_database) + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
            LOG_ERROR(log, "Code: {}, e.displayText() = {}, Stack trace:\n\n{}", e.code(), e.displayText(), e.getStackTraceString());
            sendException(e, connection_context->getSettingsRef().calculate_text_stack_trace);
            return;
        }

        connection_context->setCurrentDatabase(default_database);
    }

    Settings connection_settings = connection_context->getSettings();
    UInt64 idle_connection_timeout = connection_settings.idle_connection_timeout;
    UInt64 poll_interval = connection_settings.poll_interval;

    sendHello();

    connection_context->setProgressCallback([this] (const Progress & value) { return this->updateProgress(value); });

    while (true)
    {
        /// We are waiting for a packet from the client. Thus, every `poll_interval` seconds check whether we need to shut down.
        {
            Stopwatch idle_time;
            UInt64 timeout_ms = std::min(poll_interval, idle_connection_timeout) * 1000000;
            while (!server.isCancelled() && !static_cast<ReadBufferFromPocoSocket &>(*in).poll(timeout_ms))
            {
                if (idle_time.elapsedSeconds() > idle_connection_timeout)
                {
                    LOG_TRACE(log, "Closing idle connection");
                    return;
                }
            }
        }

        /// If we need to shut down, or client disconnects.
        if (server.isCancelled() || in->eof())
            break;

        /// Set context of request.
        query_context = Context::createCopy(connection_context);

        Stopwatch watch;
        state.reset();

        /// Initialized later.
        std::optional<CurrentThread::QueryScope> query_scope;

        /** An exception during the execution of request (it must be sent over the network to the client).
         *  The client will be able to accept it, if it did not happen while sending another packet and the client has not disconnected yet.
         */
        std::optional<DB::Exception> exception;
        bool network_error = false;

        bool send_exception_with_stack_trace = true;

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

            /** If Query received, then settings in query_context has been updated
             *  So, update some other connection settings, for flexibility.
             */
            {
                const Settings & settings = query_context->getSettingsRef();
                idle_connection_timeout = settings.idle_connection_timeout;
                poll_interval = settings.poll_interval;
            }

            /** If part_uuids got received in previous packet, trying to read again.
              */
            if (state.empty() && state.part_uuids && !receivePacket())
                continue;

            query_scope.emplace(query_context);

            send_exception_with_stack_trace = query_context->getSettingsRef().calculate_text_stack_trace;

            /// Should we send internal logs to client?
            const auto client_logs_level = query_context->getSettingsRef().send_logs_level;
            if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_LOGS
                && client_logs_level != LogsLevel::none)
            {
                state.logs_queue = std::make_shared<InternalTextLogsQueue>();
                state.logs_queue->max_priority = Poco::Logger::parseLevel(client_logs_level.toString());
                CurrentThread::attachInternalTextLogsQueue(state.logs_queue, client_logs_level);
                CurrentThread::setFatalErrorCallback([this]{ sendLogs(); });
            }

            query_context->setExternalTablesInitializer([&connection_settings, this] (ContextPtr context)
            {
                if (context != query_context)
                    throw Exception("Unexpected context in external tables initializer", ErrorCodes::LOGICAL_ERROR);

                /// Get blocks of temporary tables
                readData(connection_settings);

                /// Reset the input stream, as we received an empty block while receiving external table data.
                /// So, the stream has been marked as cancelled and we can't read from it anymore.
                state.block_in.reset();
                state.maybe_compressed_in.reset(); /// For more accurate accounting by MemoryTracker.

                state.temporary_tables_read = true;
            });

            /// Send structure of columns to client for function input()
            query_context->setInputInitializer([this] (ContextPtr context, const StoragePtr & input_storage)
            {
                if (context != query_context)
                    throw Exception("Unexpected context in Input initializer", ErrorCodes::LOGICAL_ERROR);

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
            });

            query_context->setInputBlocksReaderCallback([&connection_settings, this] (ContextPtr context) -> Block
            {
                if (context != query_context)
                    throw Exception("Unexpected context in InputBlocksReader", ErrorCodes::LOGICAL_ERROR);

                size_t poll_interval_ms;
                int receive_timeout;
                std::tie(poll_interval_ms, receive_timeout) = getReadTimeouts(connection_settings);
                if (!readDataNext(poll_interval_ms, receive_timeout))
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
                std::lock_guard lock(task_callback_mutex);
                sendReadTaskRequestAssumeLocked();
                return receiveReadTaskResponseAssumeLocked();
            });

            bool may_have_embedded_data = client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_CLIENT_SUPPORT_EMBEDDED_DATA;
            /// Processing Query
            state.io = executeQuery(state.query, query_context, false, state.stage, may_have_embedded_data);

            unknown_packet_in_send_data = query_context->getSettingsRef().unknown_packet_in_send_data;

            after_check_cancelled.restart();
            after_send_progress.restart();

            if (state.io.out)
            {
                state.need_receive_data_for_insert = true;
                processInsertQuery(connection_settings);
            }
            else if (state.need_receive_data_for_input) // It implies pipeline execution
            {
                /// It is special case for input(), all works for reading data from client will be done in callbacks.
                auto executor = state.io.pipeline.execute();
                executor->execute(state.io.pipeline.getNumThreads());
            }
            else if (state.io.pipeline.initialized())
                processOrdinaryQueryWithProcessors();
            else if (state.io.in)
                processOrdinaryQuery();

            state.io.onFinish();

            /// Do it before sending end of stream, to have a chance to show log message in client.
            query_scope->logPeakMemoryUsage();

            if (state.is_connection_closed)
                break;

            sendLogs();
            sendEndOfStream();

            /// QueryState should be cleared before QueryScope, since otherwise
            /// the MemoryTracker will be wrong for possible deallocations.
            /// (i.e. deallocations from the Aggregator with two-level aggregation)
            state.reset();
            query_scope.reset();
        }
        catch (const Exception & e)
        {
            state.io.onException();
            exception.emplace(e);

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
            exception.emplace(Exception::CreateFromPocoTag{}, e);
        }
        catch (const Poco::Exception & e)
        {
            state.io.onException();
            exception.emplace(Exception::CreateFromPocoTag{}, e);
        }
// Server should die on std logic errors in debug, like with assert()
// or ErrorCodes::LOGICAL_ERROR. This helps catch these errors in
// tests.
#ifndef NDEBUG
        catch (const std::logic_error & e)
        {
            state.io.onException();
            exception.emplace(Exception::CreateFromSTDTag{}, e);
            sendException(*exception, send_exception_with_stack_trace);
            std::abort();
        }
#endif
        catch (const std::exception & e)
        {
            state.io.onException();
            exception.emplace(Exception::CreateFromSTDTag{}, e);
        }
        catch (...)
        {
            state.io.onException();
            exception.emplace("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
        }

        try
        {
            if (exception)
            {
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
                LOG_ERROR(log, "Code: {}, e.displayText() = {}, Stack trace:\n\n{}", e.code(), e.displayText(), e.getStackTraceString());
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
            if (exception && !state.temporary_tables_read)
                query_context->initializeExternalTablesIfSet();
        }
        catch (...)
        {
            network_error = true;
            LOG_WARNING(log, "Can't read external tables after query failure.");
        }


        try
        {
            /// QueryState should be cleared before QueryScope, since otherwise
            /// the MemoryTracker will be wrong for possible deallocations.
            /// (i.e. deallocations from the Aggregator with two-level aggregation)
            state.reset();
            query_scope.reset();
        }
        catch (...)
        {
            /** During the processing of request, there was an exception that we caught and possibly sent to client.
             *  When destroying the request pipeline execution there was a second exception.
             *  For example, a pipeline could run in multiple threads, and an exception could occur in each of them.
             *  Ignore it.
             */
        }

        watch.stop();

        LOG_DEBUG(log, "Processed in {} sec.", watch.elapsedSeconds());

        /// It is important to destroy query context here. We do not want it to live arbitrarily longer than the query.
        query_context.reset();

        if (network_error)
            break;
    }
}


bool TCPHandler::readDataNext(size_t poll_interval, time_t receive_timeout)
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);

    /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
    while (true)
    {
        if (static_cast<ReadBufferFromPocoSocket &>(*in).poll(poll_interval))
            break;

        /// Do we need to shut down?
        if (server.isCancelled())
            return false;

        /** Have we waited for data for too long?
         *  If we periodically poll, the receive_timeout of the socket itself does not work.
         *  Therefore, an additional check is added.
         */
        Float64 elapsed = watch.elapsedSeconds();
        if (elapsed > static_cast<Float64>(receive_timeout))
        {
            throw Exception(ErrorCodes::SOCKET_TIMEOUT,
                            "Timeout exceeded while receiving data from client. Waited for {} seconds, timeout is {} seconds.",
                            static_cast<size_t>(elapsed), receive_timeout);
        }
    }

    /// If client disconnected.
    if (in->eof())
    {
        LOG_INFO(log, "Client has dropped the connection, cancel the query.");
        state.is_connection_closed = true;
        return false;
    }

    /// We accept and process data. And if they are over, then we leave.
    if (!receivePacket())
        return false;

    sendLogs();
    return true;
}


std::tuple<size_t, int> TCPHandler::getReadTimeouts(const Settings & connection_settings)
{
    const auto receive_timeout = query_context->getSettingsRef().receive_timeout.value;

    /// Poll interval should not be greater than receive_timeout
    const size_t default_poll_interval = connection_settings.poll_interval * 1000000;
    size_t current_poll_interval = static_cast<size_t>(receive_timeout.totalMicroseconds());
    constexpr size_t min_poll_interval = 5000; // 5 ms
    size_t poll_interval = std::max(min_poll_interval, std::min(default_poll_interval, current_poll_interval));

    return std::make_tuple(poll_interval, receive_timeout.totalSeconds());
}


void TCPHandler::readData(const Settings & connection_settings)
{
    auto [poll_interval, receive_timeout] = getReadTimeouts(connection_settings);
    sendLogs();

    while (readDataNext(poll_interval, receive_timeout))
        ;
}


void TCPHandler::processInsertQuery(const Settings & connection_settings)
{
    /** Made above the rest of the lines, so that in case of `writePrefix` function throws an exception,
      *  client receive exception before sending data.
      */
    state.io.out->writePrefix();

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
    sendData(state.io.out->getHeader());

    try
    {
        readData(connection_settings);
    }
    catch (...)
    {
        /// To avoid flushing from the destructor, that may lead to uncaught exception.
        state.io.out->writeSuffix();
        throw;
    }
    state.io.out->writeSuffix();
}


void TCPHandler::processOrdinaryQuery()
{
    OpenTelemetrySpanHolder span(__PRETTY_FUNCTION__);

    /// Pull query execution result, if exists, and send it to network.
    if (state.io.in)
    {

        if (query_context->getSettingsRef().allow_experimental_query_deduplication)
            sendPartUUIDs();

        /// This allows the client to prepare output format
        if (Block header = state.io.in->getHeader())
            sendData(header);

        /// Use of async mode here enables reporting progress and monitoring client cancelling the query
        AsynchronousBlockInputStream async_in(state.io.in);

        async_in.readPrefix();
        while (true)
        {
            if (isQueryCancelled())
            {
                async_in.cancel(false);
                break;
            }

            if (after_send_progress.elapsed() / 1000 >= query_context->getSettingsRef().interactive_delay)
            {
                /// Some time passed.
                after_send_progress.restart();
                sendProgress();
            }

            sendLogs();

            if (async_in.poll(query_context->getSettingsRef().interactive_delay / 1000))
            {
                const auto block = async_in.read();
                if (!block)
                    break;

                if (!state.io.null_format)
                    sendData(block);
            }
        }
        async_in.readSuffix();

        /** When the data has run out, we send the profiling data and totals up to the terminating empty block,
          * so that this information can be used in the suffix output of stream.
          * If the request has been interrupted, then sendTotals and other methods should not be called,
          * because we have not read all the data.
          */
        if (!isQueryCancelled())
        {
            sendTotals(state.io.in->getTotals());
            sendExtremes(state.io.in->getExtremes());
            sendProfileInfo(state.io.in->getProfileInfo());
            sendProgress();
        }

        if (state.is_connection_closed)
            return;

        sendData({});
    }

    sendProgress();
}


void TCPHandler::processOrdinaryQueryWithProcessors()
{
    auto & pipeline = state.io.pipeline;

    if (query_context->getSettingsRef().allow_experimental_query_deduplication)
        sendPartUUIDs();

    /// Send header-block, to allow client to prepare output format for data to send.
    {
        const auto & header = pipeline.getHeader();

        if (header)
            sendData(header);
    }

    {
        PullingAsyncPipelineExecutor executor(pipeline);
        CurrentMetrics::Increment query_thread_metric_increment{CurrentMetrics::QueryThread};

        Block block;
        while (executor.pull(block, query_context->getSettingsRef().interactive_delay / 1000))
        {
            std::lock_guard lock(task_callback_mutex);

            if (isQueryCancelled())
            {
                /// A packet was received requesting to stop execution of the request.
                executor.cancel();
                break;
            }

            if (after_send_progress.elapsed() / 1000 >= query_context->getSettingsRef().interactive_delay)
            {
                /// Some time passed and there is a progress.
                after_send_progress.restart();
                sendProgress();
            }

            sendLogs();

            if (block)
            {
                if (!state.io.null_format)
                    sendData(block);
            }
        }

        /** If data has run out, we will send the profiling data and total values to
          * the last zero block to be able to use
          * this information in the suffix output of stream.
          * If the request was interrupted, then `sendTotals` and other methods could not be called,
          *  because we have not read all the data yet,
          *  and there could be ongoing calculations in other threads at the same time.
          */
        if (!isQueryCancelled())
        {
            sendTotals(executor.getTotalsBlock());
            sendExtremes(executor.getExtremesBlock());
            sendProfileInfo(executor.getProfileInfo());
            sendProgress();
            sendLogs();
        }

        if (state.is_connection_closed)
            return;

        sendData({});
    }

    sendProgress();
}


void TCPHandler::processTablesStatusRequest()
{
    TablesStatusRequest request;
    request.read(*in, client_tcp_protocol_version);

    TablesStatusResponse response;
    for (const QualifiedTableName & table_name: request.tables)
    {
        auto resolved_id = connection_context->tryResolveStorageID({table_name.database, table_name.table});
        StoragePtr table = DatabaseCatalog::instance().tryGetTable(resolved_id, connection_context);
        if (!table)
            continue;

        TableStatus status;
        if (auto * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
        {
            status.is_replicated = true;
            status.absolute_delay = replicated_table->getAbsoluteDelay();
        }
        else
            status.is_replicated = false; //-V1048

        response.table_states_by_id.emplace(table_name, std::move(status));
    }


    writeVarUInt(Protocol::Server::TablesStatusResponse, *out);

    /// For testing hedged requests
    const Settings & settings = query_context->getSettingsRef();
    if (settings.sleep_in_send_tables_status_ms.totalMilliseconds())
    {
        out->next();
        std::chrono::milliseconds ms(settings.sleep_in_send_tables_status_ms.totalMilliseconds());
        std::this_thread::sleep_for(ms);
    }

    response.write(*out, client_tcp_protocol_version);
}

void TCPHandler::receiveUnexpectedTablesStatusRequest()
{
    TablesStatusRequest skip_request;
    skip_request.read(*in, client_tcp_protocol_version);

    throw NetException("Unexpected packet TablesStatusRequest received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
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

void TCPHandler::sendProfileInfo(const BlockStreamProfileInfo & info)
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

    LimitReadBuffer limit_in(*in, 107, true); /// Maximum length from the specs.

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
    connection_context->getClientInfo().forwarded_for = forwarded_address;
    return true;
}


void TCPHandler::receiveHello()
{
    /// Receive `hello` packet.
    UInt64 packet_type = 0;
    String user;
    String password;

    readVarUInt(packet_type, *in);
    if (packet_type != Protocol::Client::Hello)
    {
        /** If you accidentally accessed the HTTP protocol for a port destined for an internal TCP protocol,
          * Then instead of the packet type, there will be G (GET) or P (POST), in most cases.
          */
        if (packet_type == 'G' || packet_type == 'P')
        {
            writeString("HTTP/1.0 400 Bad Request\r\n\r\n"
                "Port " + server.config().getString("tcp_port") + " is for clickhouse-client program.\r\n"
                "You must use port " + server.config().getString("http_port") + " for HTTP.\r\n",
                *out);

            throw Exception("Client has connected to wrong port", ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT);
        }
        else
            throw NetException("Unexpected packet from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
    }

    readStringBinary(client_name, *in);
    readVarUInt(client_version_major, *in);
    readVarUInt(client_version_minor, *in);
    // NOTE For backward compatibility of the protocol, client cannot send its version_patch.
    readVarUInt(client_tcp_protocol_version, *in);
    readStringBinary(default_database, *in);
    readStringBinary(user, *in);
    readStringBinary(password, *in);

    if (user.empty())
        throw NetException("Unexpected packet from client (no user in Hello package)", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

    LOG_DEBUG(log, "Connected {} version {}.{}.{}, revision: {}{}{}.",
        client_name,
        client_version_major, client_version_minor, client_version_patch,
        client_tcp_protocol_version,
        (!default_database.empty() ? ", database: " + default_database : ""),
        (!user.empty() ? ", user: " + user : "")
    );

    if (user != USER_INTERSERVER_MARKER)
    {
        connection_context->setUser(user, password, socket().peerAddress());
    }
    else
    {
        receiveClusterNameAndSalt();
    }
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

    throw NetException("Unexpected packet Hello received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
}


void TCPHandler::sendHello()
{
    writeVarUInt(Protocol::Server::Hello, *out);
    writeStringBinary(DBMS_NAME, *out);
    writeVarUInt(DBMS_VERSION_MAJOR, *out);
    writeVarUInt(DBMS_VERSION_MINOR, *out);
    writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE)
        writeStringBinary(DateLUT::instance().getTimeZone(), *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME)
        writeStringBinary(server_display_name, *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
        writeVarUInt(DBMS_VERSION_PATCH, *out);
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
            receiveIgnoredPartUUIDs();
            return true;
        case Protocol::Client::Query:
            if (!state.empty())
                receiveUnexpectedQuery();
            receiveQuery();
            return true;

        case Protocol::Client::Data:
        case Protocol::Client::Scalar:
            if (state.empty())
                receiveUnexpectedData();
            return receiveData(packet_type == Protocol::Client::Scalar);

        case Protocol::Client::Ping:
            writeVarUInt(Protocol::Server::Pong, *out);
            out->next();
            return false;

        case Protocol::Client::Cancel:
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
            throw Exception("Unknown packet " + toString(packet_type) + " from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
    }
}

void TCPHandler::receiveIgnoredPartUUIDs()
{
    state.part_uuids = true;
    std::vector<UUID> uuids;
    readVectorBinary(uuids, *in);

    if (!uuids.empty())
        query_context->getIgnoredPartUUIDs()->add(uuids);
}


String TCPHandler::receiveReadTaskResponseAssumeLocked()
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);
    if (packet_type != Protocol::Client::ReadTaskResponse)
    {
        if (packet_type == Protocol::Client::Cancel)
        {
            state.is_cancelled = true;
            return {};
        }
        else
        {
            throw Exception(fmt::format("Received {} packet after requesting read task",
                    Protocol::Client::toString(packet_type)), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
        }
    }
    UInt64 version;
    readVarUInt(version, *in);
    if (version != DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION)
        throw Exception("Protocol version for distributed processing mismatched", ErrorCodes::UNKNOWN_PROTOCOL);
    String response;
    readStringBinary(response, *in);
    return response;
}


void TCPHandler::receiveClusterNameAndSalt()
{
    readStringBinary(cluster, *in);
    readStringBinary(salt, *in, 32);

    try
    {
        if (salt.empty())
            throw NetException("Empty salt is not allowed", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

        cluster_secret = query_context->getCluster(cluster)->getSecret();
    }
    catch (const Exception & e)
    {
        try
        {
            /// We try to send error information to the client.
            sendException(e, connection_context->getSettingsRef().calculate_text_stack_trace);
        }
        catch (...) {}

        throw;
    }
}

void TCPHandler::receiveQuery()
{
    UInt64 stage = 0;
    UInt64 compression = 0;

    state.is_empty = false;
    readStringBinary(state.query_id, *in);

    /// Client info
    ClientInfo & client_info = query_context->getClientInfo();
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        client_info.read(*in, client_tcp_protocol_version);

    /// For better support of old clients, that does not send ClientInfo.
    if (client_info.query_kind == ClientInfo::QueryKind::NO_QUERY)
    {
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.client_name = client_name;
        client_info.client_version_major = client_version_major;
        client_info.client_version_minor = client_version_minor;
        client_info.client_version_patch = client_version_patch;
        client_info.client_tcp_protocol_version = client_tcp_protocol_version;
    }

    /// Set fields, that are known apriori.
    client_info.interface = ClientInfo::Interface::TCP;

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

    readStringBinary(state.query, *in);

    /// It is OK to check only when query != INITIAL_QUERY,
    /// since only in that case the actions will be done.
    if (!cluster.empty() && client_info.query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
    {
#if USE_SSL
        std::string data(salt);
        data += cluster_secret;
        data += state.query;
        data += state.query_id;
        data += client_info.initial_user;

        if (received_hash.size() != 32)
            throw NetException("Unexpected hash received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

        std::string calculated_hash = encodeSHA256(data);

        if (calculated_hash != received_hash)
            throw NetException("Hash mismatch", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
        /// TODO: change error code?

        /// initial_user can be empty in case of Distributed INSERT via Buffer/Kafka,
        /// i.e. when the INSERT is done with the global context (w/o user).
        if (!client_info.initial_user.empty())
        {
            query_context->setUserWithoutCheckingPassword(client_info.initial_user, client_info.initial_address);
            LOG_DEBUG(log, "User (initial): {}", query_context->getUserName());
        }
        /// No need to update connection_context, since it does not requires user (it will not be used for query execution)
#else
        throw Exception(
            "Inter-server secret support is disabled, because ClickHouse was built without SSL library",
            ErrorCodes::SUPPORT_IS_DISABLED);
#endif
    }
    else
    {
        query_context->setInitialRowPolicy();
    }

    ///
    /// Settings
    ///
    auto settings_changes = passed_settings.changes();
    if (client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        /// Throw an exception if the passed settings violate the constraints.
        query_context->checkSettingsConstraints(settings_changes);
    }
    else
    {
        /// Quietly clamp to the constraints if it's not an initial query.
        query_context->clampToSettingsConstraints(settings_changes);
    }
    query_context->applySettingsChanges(settings_changes);

    /// Disable function name normalization when it's a secondary query, because queries are either
    /// already normalized on initiator node, or not normalized and should remain unnormalized for
    /// compatibility.
    if (client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        query_context->setSetting("normalize_function_names", Field(0));
    }

    // Use the received query id, or generate a random default. It is convenient
    // to also generate the default OpenTelemetry trace id at the same time, and
    // set the trace parent.
    // Why is this done here and not earlier:
    // 1) ClientInfo might contain upstream trace id, so we decide whether to use
    // the default ids after we have received the ClientInfo.
    // 2) There is the opentelemetry_start_trace_probability setting that
    // controls when we start a new trace. It can be changed via Native protocol,
    // so we have to apply the changes first.
    query_context->setCurrentQueryId(state.query_id);

    // Set parameters of initial query.
    if (client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        /// 'Current' fields was set at receiveHello.
        client_info.initial_user = client_info.current_user;
        client_info.initial_query_id = client_info.current_query_id;
        client_info.initial_address = client_info.current_address;
    }

    /// Sync timeouts on client and server during current query to avoid dangling queries on server
    /// NOTE: We use settings.send_timeout for the receive timeout and vice versa (change arguments ordering in TimeoutSetter),
    ///  because settings.send_timeout is client-side setting which has opposite meaning on the server side.
    /// NOTE: these settings are applied only for current connection (not for distributed tables' connections)
    const Settings & settings = query_context->getSettingsRef();
    state.timeout_setter = std::make_unique<TimeoutSetter>(socket(), settings.receive_timeout, settings.send_timeout);
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
    readStringBinary(skip_string, *in);

    throw NetException("Unexpected packet Query received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
}

bool TCPHandler::receiveData(bool scalar)
{
    initBlockInput();

    /// The name of the temporary table for writing data, default to empty string
    auto temporary_id = StorageID::createEmpty();
    readStringBinary(temporary_id.table_name, *in);

    /// Read one block from the network and write it down
    Block block = state.block_in->read();

    if (block)
    {
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
            auto temporary_table_out = storage->write(ASTPtr(), metadata_snapshot, query_context);
            temporary_table_out->write(block);
            temporary_table_out->writeSuffix();

        }
        else if (state.need_receive_data_for_input)
        {
            /// 'input' table function.
            state.block_for_input = block;
        }
        else
        {
            /// INSERT query.
            state.io.out->write(block);
        }
        return true;
    }
    else
        return false;
}

void TCPHandler::receiveUnexpectedData()
{
    String skip_external_table_name;
    readStringBinary(skip_external_table_name, *in);

    std::shared_ptr<ReadBuffer> maybe_compressed_in;

    if (last_block_in.compression == Protocol::Compression::Enable)
        maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in, /* allow_different_codecs */ true);
    else
        maybe_compressed_in = in;

    auto skip_block_in = std::make_shared<NativeBlockInputStream>(
            *maybe_compressed_in,
            last_block_in.header,
            client_tcp_protocol_version);

    skip_block_in->read();
    throw NetException("Unexpected packet Data received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
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
        if (state.io.out)
            header = state.io.out->getHeader();
        else if (state.need_receive_data_for_input)
            header = state.input_header;

        last_block_in.header = header;
        last_block_in.compression = state.compression;

        state.block_in = std::make_shared<NativeBlockInputStream>(
            *state.maybe_compressed_in,
            header,
            client_tcp_protocol_version);
    }
}


void TCPHandler::initBlockOutput(const Block & block)
{
    if (!state.block_out)
    {
        if (!state.maybe_compressed_out)
        {
            const Settings & query_settings = query_context->getSettingsRef();

            std::string method = Poco::toUpper(query_settings.network_compression_method.toString());
            std::optional<int> level;
            if (method == "ZSTD")
                level = query_settings.network_zstd_compression_level;

            if (state.compression == Protocol::Compression::Enable)
            {
                CompressionCodecFactory::instance().validateCodec(method, level, !query_settings.allow_suspicious_codecs, query_settings.allow_experimental_codecs);

                state.maybe_compressed_out = std::make_shared<CompressedWriteBuffer>(
                    *out, CompressionCodecFactory::instance().get(method, level));
            }
            else
                state.maybe_compressed_out = out;
        }

        state.block_out = std::make_shared<NativeBlockOutputStream>(
            *state.maybe_compressed_out,
            client_tcp_protocol_version,
            block.cloneEmpty(),
            !connection_context->getSettingsRef().low_cardinality_allow_in_native_format);
    }
}

void TCPHandler::initLogsBlockOutput(const Block & block)
{
    if (!state.logs_block_out)
    {
        /// Use uncompressed stream since log blocks usually contain only one row
        state.logs_block_out = std::make_shared<NativeBlockOutputStream>(
            *out,
            client_tcp_protocol_version,
            block.cloneEmpty(),
            !connection_context->getSettingsRef().low_cardinality_allow_in_native_format);
    }
}


bool TCPHandler::isQueryCancelled()
{
    if (state.is_cancelled || state.sent_all_data)
        return true;

    if (after_check_cancelled.elapsed() / 1000 < query_context->getSettingsRef().interactive_delay)
        return false;

    after_check_cancelled.restart();

    /// During request execution the only packet that can come from the client is stopping the query.
    if (static_cast<ReadBufferFromPocoSocket &>(*in).poll(0))
    {
        if (in->eof())
        {
            LOG_INFO(log, "Client has dropped the connection, cancel the query.");
            state.is_cancelled = true;
            state.is_connection_closed = true;
            return true;
        }

        UInt64 packet_type = 0;
        readVarUInt(packet_type, *in);

        switch (packet_type)
        {
            case Protocol::Client::Cancel:
                if (state.empty())
                    throw NetException("Unexpected packet Cancel received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
                LOG_INFO(log, "Query was cancelled.");
                state.is_cancelled = true;
                return true;

            default:
                throw NetException("Unknown packet from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        }
    }

    return false;
}


void TCPHandler::sendData(const Block & block)
{
    initBlockOutput(block);

    auto prev_bytes_written_out = out->count();
    auto prev_bytes_written_compressed_out = state.maybe_compressed_out->count();

    try
    {
        /// For testing hedged requests
        if (unknown_packet_in_send_data)
        {
            --unknown_packet_in_send_data;
            if (unknown_packet_in_send_data == 0)
                writeVarUInt(UInt64(-1), *out);
        }

        writeVarUInt(Protocol::Server::Data, *out);
        /// Send external table name (empty name is the main table)
        writeStringBinary("", *out);

        /// For testing hedged requests
        const Settings & settings = query_context->getSettingsRef();
        if (block.rows() > 0 && settings.sleep_in_send_data_ms.totalMilliseconds())
        {
            out->next();
            std::chrono::milliseconds ms(settings.sleep_in_send_data_ms.totalMilliseconds());
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
    writeVarUInt(Protocol::Server::Exception, *out);
    writeException(e, *out, with_stack_trace);
    out->next();
}


void TCPHandler::sendEndOfStream()
{
    state.sent_all_data = true;
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
    auto increment = state.progress.fetchAndResetPiecewiseAtomically();
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
        if (!strcmp(e.what(), "Timeout"))
        {
            LOG_DEBUG(log, "Poco::Exception. Code: {}, e.code() = {}, e.displayText() = {}, e.what() = {}", ErrorCodes::POCO_EXCEPTION, e.code(), e.displayText(), e.what());
        }
        else
            throw;
    }
}

}
