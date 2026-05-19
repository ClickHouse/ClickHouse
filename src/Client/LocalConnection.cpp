#include <Client/LocalConnection.h>
#include <future>
#include <memory>
#include <Client/ClientBase.h>
#include <Client/ClientApplicationBase.h>
#include <Core/Protocol.h>
#include <Core/Settings.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/Pipe.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/IStorage.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Common/config_version.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/setThreadName.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/PRQL/ParserPRQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/parseKQLQuery.h>
#include <Parsers/Prometheus/ParserPrometheusQuery.h>
#include <Common/CurrentThread.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_settings_after_format_in_insert;
    extern const SettingsBool async_insert;
    extern const SettingsBool allow_experimental_detach_non_readonly_queries;
    extern const SettingsDialect dialect;
    extern const SettingsBool input_format_defaults_for_omitted_fields;
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsUInt64 max_insert_block_size_bytes;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 min_insert_block_size_bytes;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsBool implicit_select;
    extern const SettingsLogsLevel send_logs_level;
    extern const SettingsString send_logs_source_regexp;
    extern const SettingsString promql_database;
    extern const SettingsString promql_table;
    extern const SettingsFloatAuto promql_evaluation_time;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int UNKNOWN_EXCEPTION;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}


LocalConnection::LocalConnection(ContextPtr context_, ReadBuffer * in_, bool send_progress_, bool send_profile_events_, const String & server_display_name_, bool is_interactive_)
    : WithContext(context_)
    , session(std::make_unique<Session>(getContext(), ClientInfo::Interface::LOCAL))
    , send_progress(send_progress_)
    , send_profile_events(send_profile_events_)
    , server_display_name(server_display_name_)
    , in(in_)
    , is_interactive(is_interactive_)
{
    /// Authenticate and create a context to execute queries.
    session->authenticate("default", "", Poco::Net::SocketAddress{});
    ContextMutablePtr session_context = session->makeSessionContext();
    /// Re-apply settings from the command line arguments
    session_context->applySettingsChanges(getContext()->getSettingsRef().changes());
}

LocalConnection::LocalConnection(
    std::unique_ptr<Session> && session_, ReadBuffer * in_, bool send_progress_, bool send_profile_events_, const String & server_display_name_, bool is_interactive_)
    : WithContext(session_->sessionContext())
    , session(std::move(session_))
    , send_progress(send_progress_)
    , send_profile_events(send_profile_events_)
    , server_display_name(server_display_name_)
    , in(in_)
    , is_interactive(is_interactive_)
{
}

LocalConnection::~LocalConnection()
{
    /// Wait for any detached non-readonly query to complete so data is persisted before process exit.
    if (detached_query_thread && detached_query_thread->joinable())
        detached_query_thread->join();
    /// Last query may not have been finished or cancelled due to exception on client side.
    if (state && !state->is_finished && !state->is_cancelled)
    {
        try
        {
            LocalConnection::sendCancel();
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Just ignore any exception.
        }
    }
    state.reset();
}

bool LocalConnection::hasReadPendingData() const
{
    return !state->is_finished;
}

std::optional<UInt64> LocalConnection::checkPacket(size_t)
{
    return next_packet_type;
}

void LocalConnection::updateProgress(const Progress & value)
{
    state->progress.incrementPiecewiseAtomically(value);
}

void LocalConnection::sendProfileEvents()
{
    Block profile_block;
    state->after_send_profile_events.restart();
    next_packet_type = Protocol::Server::ProfileEvents;
    state->block.emplace(ProfileEvents::getProfileEvents(server_display_name, state->profile_queue, last_sent_snapshots));
}

void LocalConnection::sendQuery(
    const ConnectionTimeouts &,
    const String & query,
    const NameToNameMap & query_parameters,
    const String & query_id,
    UInt64 stage,
    const Settings *,
    const ClientInfo * client_info,
    bool,
    const std::vector<String> & /*external_roles*/,
    std::function<void(const Progress &)> process_progress_callback)
{
    /// Last query may not have been finished or cancelled due to exception on client side.
    if (state && !state->is_finished && !state->is_cancelled)
        sendCancel();

    /// Suggestion comes without client_info.
    if (client_info)
        query_context = session->makeQueryContext(*client_info);
    else
        query_context = session->makeQueryContext();

    query_context->setCurrentQueryId(query_id);
    query_context->setClientInterface(ClientInfo::Interface::LOCAL);

    /// Always track progress so that output formats (e.g. JSON) can report accurate statistics.
    /// The send_progress flag only controls the client-side progress bar, not progress tracking.
    query_context->setProgressCallback([this](const Progress & value) { this->updateProgress(value); });
    query_context->setFileProgressCallback([this](const FileProgress & value) { this->updateProgress(Progress(value)); });

    if (is_cancelled_callback)
    {
        query_context->setInteractiveCancelCallback(
            [this, check_cancelled = is_cancelled_callback, progress_callback = process_progress_callback]() -> bool
        {
            /// Send accumulated progress to the client so the progress bar updates during analysis.
            if (progress_callback)
            {
                auto progress = state->progress.fetchAndResetPiecewiseAtomically();
                if (progress.read_rows || progress.read_bytes)
                    progress_callback(progress);
            }

            if (!check_cancelled())
                return false;

            state->is_cancelled = true;
            if (auto elem = query_context->getProcessListElement())
                elem->cancelQuery(CancelReason::CANCELLED_BY_USER);
            return true;
        });
    }

    /// Switch the database to the desired one (set by the USE query)
    /// but don't attempt to do it if we are already in that database.
    /// (there is a rare case when it matters - if we deleted the current database,
    // we can still do some queries, but we cannot switch to the same database)
    if (!current_database.empty() && current_database != query_context->getCurrentDatabase())
        query_context->setCurrentDatabase(current_database);

    query_context->addQueryParameters(query_parameters);

    state.reset();
    state.emplace();

    state->query_id = query_id;
    state->query = query;
    state->query_scope_holder = QueryScope::create(query_context);
    state->stage = QueryProcessingStage::Enum(stage);
    state->profile_queue = std::make_shared<InternalProfileEventsQueue>(std::numeric_limits<int>::max());
    CurrentThread::attachInternalProfileEventsQueue(state->profile_queue);
    state->logs_queue = std::make_shared<InternalTextLogsQueue>();
    const auto client_logs_level = getContext()->getSettingsRef()[Setting::send_logs_level];
    state->logs_queue->max_priority = Poco::Logger::parseLevel(client_logs_level.toString());
    state->logs_queue->setSourceRegexp(getContext()->getSettingsRef()[Setting::send_logs_source_regexp]);
    CurrentThread::attachInternalTextLogsQueue(state->logs_queue, client_logs_level);

    if (send_progress)
        state->after_send_progress.restart();

    if (send_profile_events)
        state->after_send_profile_events.restart();

    next_packet_type.reset();

    /// Wait for any previous detached query to complete before starting a new one.
    if (detached_query_thread && detached_query_thread->joinable())
    {
        detached_query_thread->join();
        if (detached_query_exception && *detached_query_exception)
        {
            std::exception_ptr e = *detached_query_exception;
            detached_query_exception.reset();
            std::rethrow_exception(e);
        }
    }

    /// Detach path (interactive mode only): when allow_experimental_detach_non_readonly_queries is on and query is non-readonly
    /// (and does not need client data), run query in a background thread and return query_id immediately.
    if (is_interactive)
    {
        /// Set by the background thread once the query is registered in ProcessList and quotas are checked.
        /// Kept outside the try/catch below so that query-start failures (quota, duplicate query_id,
        /// permissions) propagate to the client rather than silently falling back to sync execution.
        std::optional<std::future<void>> detach_started;
        String detach_query_id;

        try
        {
            const auto & settings_ref = query_context->getSettingsRef();
            const size_t max_query_size_val = settings_ref[Setting::max_query_size] ? settings_ref[Setting::max_query_size] : std::numeric_limits<size_t>::max();
            const char * begin = state->query.data();
            const char * end = begin + state->query.size();
            ParserQuery parser(end, settings_ref[Setting::allow_settings_after_format_in_insert], settings_ref[Setting::implicit_select]);
            ASTPtr ast = parseQuery(parser, begin, end, "", max_query_size_val, settings_ref[Setting::max_parser_depth], settings_ref[Setting::max_parser_backtracks]);

            /// Apply inline SETTINGS before the detach check so they can trigger the detach path.
            /// settings_ref is a reference to query_context, so it reflects the changes immediately.
            InterpreterSetQuery::applySettingsFromQuery(ast, query_context);
            if (settings_ref[Setting::allow_experimental_detach_non_readonly_queries] && !settings_ref[Setting::async_insert]
                && ast && IAST::isNonReadOnlyQuery(ast.get()))
            {
                const auto * insert_ast = ast->as<ASTInsertQuery>();
                bool insert_needs_client_data = insert_ast && !insert_ast->select && !insert_ast->hasInlinedData();
                if (!insert_needs_client_data)
                {
                    ContextMutablePtr async_context = Context::createCopy(query_context);
                    async_context->setProgressCallback(nullptr);
                    /// Ensure the background thread uses the same path and current database as this session.
                    String path = query_context->getPath();
                    if (!path.empty())
                        async_context->setPath(path);
                    String db = query_context->getCurrentDatabase();
                    if (!db.empty())
                        async_context->setCurrentDatabase(db);
                    String query_copy = state->query;
                    QueryProcessingStage::Enum stage_copy = state->stage;
                    detached_query_exception = std::make_shared<std::exception_ptr>();

                    auto started_promise = std::make_shared<std::promise<void>>();
                    auto started_future = started_promise->get_future();

                    detached_query_thread = std::make_unique<std::thread>([async_context, query_copy, stage_copy, exc_ptr = detached_query_exception, started_promise]() mutable
                    {
                        setThreadName(ThreadName::QUERY_ASYNC_EXECUTOR);
                        ThreadStatus thread_status;
                        QueryScope async_query_scope = QueryScope::create(async_context);

                        bool query_started = false;

                        /// Called by executeQuery after ProcessList::insert and quota checks pass —
                        /// the earliest point where ExceptionBeforeStart can no longer occur.
                        auto on_started = [&]()
                        {
                            if (!query_started)
                            {
                                query_started = true;
                                started_promise->set_value();
                            }
                        };

                        try
                        {
                            BlockIO io = executeQuery(query_copy, async_context, QueryFlags{}, stage_copy, std::move(on_started)).second;
                            /// Actually run the pipeline (executeQuery only builds it). Without this, no data is written.
                            if (io.pipeline.pushing())
                            {
                                PushingPipelineExecutor executor(io.pipeline);
                                executor.start();
                                executor.finish();
                            }
                            else if (io.pipeline.pulling())
                            {
                                PullingPipelineExecutor executor(io.pipeline);
                                Block block;
                                while (executor.pull(block)) { }
                            }
                            else if (io.pipeline.completed())
                            {
                                CompletedPipelineExecutor executor(io.pipeline);
                                executor.execute();
                            }
                            io.onFinish();
                        }
                        catch (...)
                        {
                            if (!query_started)
                            {
                                /// Pre-start failure: propagate to the main thread via the promise.
                                /// Do not store in exc_ptr — the exception already reaches the caller
                                /// through detach_started->get(), so storing it again would cause a
                                /// double-throw on the next sendQuery call.
                                started_promise->set_exception(std::current_exception());
                            }
                            else
                            {
                                /// Post-start failure: query_id was already returned to the client.
                                /// Store for later retrieval (rethrown at the next sendQuery).
                                *exc_ptr = std::current_exception();
                                tryLogCurrentException("LocalConnection", "Detached local non-readonly query failed after start");
                            }
                        }
                    });

                    detach_started = std::move(started_future);
                    detach_query_id = query_context->getClientInfo().current_query_id;
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException("LocalConnection", "Cannot run local query in detach mode, falling back to sync");
        }

        if (detach_started.has_value())
        {
            /// Block until the background thread signals that the query has passed
            /// ProcessList::insert and quota checks. Re-throw on failure so the
            /// caller receives the exception (e.g. quota exceeded, duplicate query_id).
            detach_started->get();

            auto col = ColumnString::create();
            col->insertData(detach_query_id.data(), detach_query_id.size());
            state->block = Block({{std::move(col), std::make_shared<DataTypeString>(), "query_id"}});
            next_packet_type = Protocol::Server::Data;
            state->is_finished = true;
            state->sent_totals = true;
            state->sent_extremes = true;
            state->sent_profile_info = true;
            state->sent_profile_events = true;
            return;
        }
    }

    /// Prepare input() function
    query_context->setInputInitializer([this] (ContextPtr context, const StoragePtr & input_storage)
    {
        if (context != query_context)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in Input initializer");

        auto metadata_snapshot = input_storage->getInMemoryMetadataPtr(context, false);
        Block sample = metadata_snapshot->getSampleBlock();

        next_packet_type = Protocol::Server::Data;
        state->block = sample;

        String current_format = "Values";

        const auto & settings = context->getSettingsRef();
        const char * begin = state->query.data();

        const char * end = begin + state->query.size();
        const Dialect & dialect = settings[Setting::dialect];

        std::unique_ptr<IParserBase> parser;
        if (dialect == Dialect::kusto)
            parser = std::make_unique<ParserKQLStatement>(end, settings[Setting::allow_settings_after_format_in_insert]);
        else if (dialect == Dialect::prql)
            parser = std::make_unique<ParserPRQLQuery>(settings[Setting::max_query_size], settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
        else if (dialect == Dialect::promql)
            parser = std::make_unique<ParserPrometheusQuery>(settings[Setting::promql_database], settings[Setting::promql_table], Field{settings[Setting::promql_evaluation_time]});
        else
            parser = std::make_unique<ParserQuery>(end, settings[Setting::allow_settings_after_format_in_insert], settings[Setting::implicit_select]);

        ASTPtr parsed_query;
        if (dialect == Dialect::kusto)
            parsed_query = parseKQLQueryAndMovePosition(
                *parser,
                begin,
                end,
                "",
                /*allow_multi_statements*/ false,
                settings[Setting::max_query_size],
                settings[Setting::max_parser_depth],
                settings[Setting::max_parser_backtracks]);
        else
            parsed_query = parseQueryAndMovePosition(
                *parser,
                begin,
                end,
                "",
                /*allow_multi_statements*/ false,
                settings[Setting::max_query_size],
                settings[Setting::max_parser_depth],
                settings[Setting::max_parser_backtracks]);

        if (const auto * insert = parsed_query->as<ASTInsertQuery>())
        {
            if (!insert->format.empty())
                current_format = insert->format;
        }

        chassert(in, "ReadBuffer should be initialized");

        auto source = context->getInputFormat(
            current_format,
            *in,
            sample,
            settings[Setting::max_insert_block_size],
            std::nullopt,
            settings[Setting::max_insert_block_size_bytes],
            settings[Setting::min_insert_block_size_rows],
            settings[Setting::min_insert_block_size_bytes]);
        Pipe pipe(source);

        auto columns_description = metadata_snapshot->getColumns();
        if (columns_description.hasDefaults())
        {
            pipe.addSimpleTransform([&](const SharedHeader & header)
            {
                return std::make_shared<AddingDefaultsTransform>(header, columns_description, *source, context);
            });
        }

        state->input_pipeline = std::make_unique<QueryPipeline>(std::move(pipe));
        state->input_pipeline_executor = std::make_unique<PullingAsyncPipelineExecutor>(*state->input_pipeline);

    });
    query_context->setInputBlocksReaderCallback([this] (ContextPtr context) -> Block
    {
        if (context != query_context)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in InputBlocksReader");

        Block block;
        state->input_pipeline_executor->pull(block);
        return block;
    });

    try
    {
        query_context->setSetting("serialize_query_plan", false);
        state->io = executeQuery(state->query, query_context, QueryFlags{}, state->stage).second;

        if (state->io.pipeline.pushing())
        {
            size_t num_threads = state->io.pipeline.getNumThreads();
            if (num_threads > 1)
            {
                state->pushing_async_executor = std::make_unique<PushingAsyncPipelineExecutor>(state->io.pipeline);
                state->pushing_async_executor->start();
                state->block = state->pushing_async_executor->getHeader();
            }
            else
            {
                state->pushing_executor = std::make_unique<PushingPipelineExecutor>(state->io.pipeline);
                state->pushing_executor->start();
                state->block = state->pushing_executor->getHeader();
            }

            if (query_context->getSettingsRef()[Setting::input_format_defaults_for_omitted_fields])
            {
                if (query_context->hasInsertionTableColumnsDescription())
                    state->columns_description = query_context->getInsertionTableColumnsDescription();
            }
        }
        else if (state->io.pipeline.pulling())
        {
            state->block = state->io.pipeline.getHeader();
            state->executor = std::make_unique<PullingAsyncPipelineExecutor>(state->io.pipeline);
            state->io.pipeline.setConcurrencyControl(false);
        }
        else if (state->io.pipeline.completed())
        {
            CompletedPipelineExecutor executor(state->io.pipeline);
            if (process_progress_callback)
            {
                auto callback = [this, &process_progress_callback]()
                {
                    if (state->is_cancelled)
                        return true;

                    process_progress_callback(state->progress.fetchAndResetPiecewiseAtomically());
                    return false;
                };

                executor.setCancelCallback(callback, query_context->getSettingsRef()[Setting::interactive_delay] / 1000);
            }
            executor.execute();
        }

        if (state->columns_description)
            next_packet_type = Protocol::Server::TableColumns;
        else if (state->block)
            next_packet_type = Protocol::Server::Data;
    }
    catch (const Exception & e)
    {
        state->io.onException();
        state->exception.reset(e.clone());
    }
    catch (const std::exception & e)
    {
        state->io.onException();
        state->exception = std::make_unique<Exception>(Exception::CreateFromSTDTag{}, e);
    }
    catch (...) // Ok: wrap unknown exception for the client
    {
        state->io.onException();
        state->exception = std::make_unique<Exception>(Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Unknown exception"));
    }
}

void LocalConnection::sendQueryPlan(const QueryPlan &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

void LocalConnection::sendData(const Block & block, const String &, bool)
{
    if (block.empty())
        return;

    if (state->pushing_async_executor)
        state->pushing_async_executor->push(block);
    else if (state->pushing_executor)
        state->pushing_executor->push(block);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown executor");

    if (send_profile_events)
        sendProfileEvents();
}

bool LocalConnection::isSendDataNeeded() const
{
    return !state || state->input_pipeline == nullptr;
}

void LocalConnection::sendCancel()
{
    state->is_cancelled = true;
    if (state->executor)
        state->executor->cancel();
    if (state->pushing_executor)
        state->pushing_executor->cancel();
    if (state->pushing_async_executor)
        state->pushing_async_executor->cancel();
}

bool LocalConnection::pullBlock(Block & block)
{
    if (state->executor)
        return state->executor->pull(block, query_context->getSettingsRef()[Setting::interactive_delay] / 1000);

    return false;
}

void LocalConnection::finishQuery()
{
    next_packet_type = Protocol::Server::EndOfStream;

    if (state->executor)
    {
        state->executor.reset();
    }
    else if (state->pushing_async_executor)
    {
        state->pushing_async_executor->finish();
        state->pushing_async_executor.reset();
    }
    else if (state->pushing_executor)
    {
        state->pushing_executor->finish();
        state->pushing_executor.reset();
    }

    state->io.onFinish();
    state.reset();
    last_sent_snapshots.clear();
}

bool LocalConnection::poll(size_t)
{
    if (!state)
        return false;

    /// Wait for next poll to collect current packet.
    if (next_packet_type)
        return true;

    if (state->exception)
    {
        /// Flush any buffered logs before delivering the exception, otherwise
        /// the user would not see log messages produced before the failure.
        if (needSendLogs())
            return true;

        next_packet_type = Protocol::Server::Exception;
        return true;
    }

    if (!state->is_finished)
    {
        if (needSendProgressOrMetrics())
            return true;

        if (needSendLogs())
            return true;

        try
        {
            while (pollImpl())
            {
                LOG_TEST(&Poco::Logger::get("LocalConnection"), "Executor timeout encountered, will retry");

                if (needSendProgressOrMetrics())
                    return true;

                if (needSendLogs())
                    return true;
            }
        }
        catch (const Exception & e)
        {
            state->io.onException();
            state->exception.reset(e.clone());
        }
        catch (const std::exception & e)
        {
            state->io.onException();
            state->exception = std::make_unique<Exception>(Exception::CreateFromSTDTag{}, e);
        }
        catch (...) // Ok: wrap unknown exception for the client
        {
            state->io.onException();
            state->exception = std::make_unique<Exception>(Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Unknown exception"));
        }
    }

    if (state->exception)
    {
        if (needSendLogs())
            return true;

        next_packet_type = Protocol::Server::Exception;
        return true;
    }

    // pushing executors have to be finished before the final stats are sent
    if (state->is_finished)
    {
        if (state->executor)
        {
            // no op
        }
        else if (state->pushing_async_executor)
        {
            state->pushing_async_executor->finish();
        }
        else if (state->pushing_executor)
        {
            state->pushing_executor->finish();
        }
    }

    if (state->is_finished && !state->sent_totals)
    {
        state->sent_totals = true;
        Block totals;

        if (state->executor)
            totals = state->executor->getTotalsBlock();

        if (!totals.empty())
        {
            next_packet_type = Protocol::Server::Totals;
            state->block.emplace(totals);
            return true;
        }
    }

    if (state->is_finished && !state->sent_extremes)
    {
        state->sent_extremes = true;
        Block extremes;

        if (state->executor)
            extremes = state->executor->getExtremesBlock();

        if (!extremes.empty())
        {
            next_packet_type = Protocol::Server::Extremes;
            state->block.emplace(extremes);
            return true;
        }
    }

    if (state->is_finished && !state->sent_profile_info)
    {
        state->sent_profile_info = true;

        if (state->executor)
        {
            next_packet_type = Protocol::Server::ProfileInfo;
            state->profile_info = state->executor->getProfileInfo();
            return true;
        }
    }

    if (state->is_finished && !state->sent_profile_events)
    {
        state->sent_profile_events = true;

        if (send_profile_events && (state->executor || state->pushing_async_executor || state->pushing_executor))
        {
            sendProfileEvents();
            return true;
        }
    }

    if (state->is_finished && !state->sent_progress)
    {
        state->sent_progress = true;
        next_packet_type = Protocol::Server::Progress;
        return true;
    }

    if (state->is_finished)
    {
        if (needSendLogs())
            return true;

        finishQuery();
        return true;
    }

    if (state->block && !state->block.value().empty())
    {
        next_packet_type = Protocol::Server::Data;
        return true;
    }

    return false;
}

bool LocalConnection::needSendProgressOrMetrics()
{
    if (state->after_send_progress.elapsedMicroseconds() >= query_context->getSettingsRef()[Setting::interactive_delay])
    {
        state->after_send_progress.restart();
        next_packet_type = Protocol::Server::Progress;
        return true;
    }

    if (send_profile_events
        && (state->after_send_profile_events.elapsedMicroseconds() >= query_context->getSettingsRef()[Setting::interactive_delay]))
    {
        sendProfileEvents();
        return true;
    }

    return false;
}

bool LocalConnection::needSendLogs()
{
    if (!state->logs_queue)
        return false;

    MutableColumns logs_columns;
    MutableColumns curr_logs_columns;
    size_t rows = 0;

    for (; state->logs_queue->tryPop(curr_logs_columns); ++rows)
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

        next_packet_type = Protocol::Server::Log;
        state->block = std::move(block);
        return true;
    }

    return false;
}

bool LocalConnection::pollImpl()
{
    Block block;
    auto next_read = pullBlock(block);

    if (block.empty() && next_read)
    {
        return true;
    }
    if (!block.empty() && !state->io.null_format)
    {
        state->block.emplace(block);
    }
    else if (!next_read)
    {
        state->is_finished = true;
    }

    return false;
}

UInt64 LocalConnection::receivePacketType()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "receivePacketType is not implemented for LocalConnection");
}

Packet LocalConnection::receivePacket()
{
    Packet packet;
    if (!state)
    {
        packet.type = Protocol::Server::EndOfStream;
        return packet;
    }

    if (!next_packet_type)
        poll(0);

    if (!next_packet_type)
    {
        packet.type = Protocol::Server::EndOfStream;
        return packet;
    }

    packet.type = next_packet_type.value();
    switch (next_packet_type.value())
    {
        case Protocol::Server::Totals:
        case Protocol::Server::Extremes:
        case Protocol::Server::Log:
        case Protocol::Server::Data:
        case Protocol::Server::ProfileEvents:
        {
            if (state->block && !state->block.value().empty())
            {
                packet.block = std::move(state->block.value());
                state->block.reset();
            }
            next_packet_type.reset();
            break;
        }
        case Protocol::Server::ProfileInfo:
        {
            if (state->profile_info)
            {
                packet.profile_info = *state->profile_info;
                state->profile_info.reset();
            }
            next_packet_type.reset();
            break;
        }
        case Protocol::Server::TableColumns:
        {
            if (state->columns_description)
            {
                packet.columns_description = state->columns_description->toString(/* include_comments = */ false);
            }

            if (state->block)
            {
                next_packet_type = Protocol::Server::Data;
            }

            break;
        }
        case Protocol::Server::Exception:
        {
            packet.exception.reset(state->exception->clone());
            next_packet_type.reset();
            break;
        }
        case Protocol::Server::Progress:
        {
            packet.progress = state->progress.fetchAndResetPiecewiseAtomically();
            state->progress.reset();
            next_packet_type.reset();
            break;
        }
        case Protocol::Server::EndOfStream:
        {
            next_packet_type.reset();
            break;
        }
        default:
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER,
                            "Unknown packet {} for {}", toString(packet.type), getDescription());
    }

    return packet;
}

void LocalConnection::getServerVersion(
    const ConnectionTimeouts & /* timeouts */, String & name,
    UInt64 & version_major, UInt64 & version_minor,
    UInt64 & version_patch, UInt64 & revision)
{
    name = std::string(VERSION_NAME);
    version_major = VERSION_MAJOR;
    version_minor = VERSION_MINOR;
    version_patch = VERSION_PATCH;
    revision = DBMS_TCP_PROTOCOL_VERSION;
}

void LocalConnection::setDefaultDatabase(const String & database)
{
    current_database = database;
}

UInt64 LocalConnection::getServerRevision(const ConnectionTimeouts &)
{
    return DBMS_TCP_PROTOCOL_VERSION;
}

const String & LocalConnection::getServerTimezone(const ConnectionTimeouts &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

const String & LocalConnection::getServerDisplayName(const ConnectionTimeouts &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

void LocalConnection::sendExternalTablesData(ExternalTablesData &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

void LocalConnection::sendMergeTreeReadTaskResponse(const ParallelReadResponse &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

ServerConnectionPtr LocalConnection::createConnection(
    const ConnectionParameters &,
    ContextPtr current_context,
    ReadBuffer * in_buffer,
    bool send_progress_val,
    bool send_profile_events_val,
    const String & server_display_name_val,
    bool is_interactive_val)
{
    return std::make_unique<LocalConnection>(current_context, in_buffer, send_progress_val, send_profile_events_val, server_display_name_val, is_interactive_val);
}

ServerConnectionPtr LocalConnection::createConnection(
    const ConnectionParameters &,
    std::unique_ptr<Session> && session_ptr,
    ReadBuffer * in_buffer,
    bool send_progress_val,
    bool send_profile_events_val,
    const String & server_display_name_val,
    bool is_interactive_val)
{
    return std::make_unique<LocalConnection>(std::move(session_ptr), in_buffer, send_progress_val, send_profile_events_val, server_display_name_val, is_interactive_val);
}


}
