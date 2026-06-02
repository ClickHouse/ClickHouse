#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MessageQueueSink.h>
#include <Storages/NATS/NATSJetStreamConsumer.h>
#include <Storages/NATS/NATSCoreConsumer.h>
#include <Storages/NATS/NATSJetStreamProducer.h>
#include <Storages/NATS/NATSCoreProducer.h>
#include <Storages/NATS/NATSSettings.h>
#include <Storages/NATS/NATSSource.h>
#include <Storages/NATS/StorageNATS.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace Setting
{
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsMilliseconds stream_flush_interval_ms;
    extern const SettingsBool stream_like_engine_allow_direct_select;
    extern const SettingsString stream_like_engine_insert_queue;
    extern const SettingsUInt64 output_format_avro_rows_in_file;
}

namespace NATSSetting
{
    extern const NATSSettingsString nats_credential_file;
    extern const NATSSettingsMilliseconds nats_flush_interval_ms;
    extern const NATSSettingsString nats_format;
    extern const NATSSettingsStreamingHandleErrorMode nats_handle_error_mode;
    extern const NATSSettingsUInt64 nats_max_block_size;
    extern const NATSSettingsUInt64 nats_max_rows_per_message;
    extern const NATSSettingsString nats_consumer_name;
    extern const NATSSettingsUInt64 nats_num_consumers;
    extern const NATSSettingsString nats_password;
    extern const NATSSettingsString nats_queue_group;
    extern const NATSSettingsUInt64 nats_reconnect_wait;
    extern const NATSSettingsString nats_schema;
    extern const NATSSettingsBool nats_secure;
    extern const NATSSettingsString nats_server_list;
    extern const NATSSettingsUInt64 nats_skip_broken_messages;
    extern const NATSSettingsUInt64 nats_startup_connect_tries;
    extern const NATSSettingsString nats_subjects;
    extern const NATSSettingsString nats_token;
    extern const NATSSettingsString nats_url;
    extern const NATSSettingsString nats_stream;
    extern const NATSSettingsString nats_username;
}

static const uint32_t QUEUE_SIZE = 100000;
static const auto RESCHEDULE_MS = 500;
static const auto MAX_THREAD_WORK_DURATION_MS = 60000;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_CONNECT_NATS;
    extern const int QUERY_NOT_ALLOWED;
}


StorageNATS::StorageNATS(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & comment,
    std::unique_ptr<NATSSettings> nats_settings_,
    LoadingStrictnessLevel mode)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , nats_settings(std::move(nats_settings_))
    , subjects(parseList(getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_subjects]), ','))
    , format_name(getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_format]))
    , schema_name(getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_schema]))
    , num_consumers((*nats_settings)[NATSSetting::nats_num_consumers].value)
    , max_rows_per_message((*nats_settings)[NATSSetting::nats_max_rows_per_message])
    , log(getLogger("StorageNATS (" + table_id_.getFullTableName() + ")"))
    , event_handler(log)
    , semaphore(0, static_cast<int>(num_consumers))
    , queue_size(std::max(QUEUE_SIZE, static_cast<uint32_t>(getMaxBlockSize())))
    , throw_on_startup_failure(mode <= LoadingStrictnessLevel::CREATE)
{
    auto nats_username = getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_username]);
    auto nats_password = getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_password]);
    auto nats_token = getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_token]);
    auto nats_credential_file = getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_credential_file]);

    configuration =
    {
        .url = getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_url]),
        .servers = parseList(getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_server_list]), ','),
        .username = nats_username.empty() ? getContext()->getConfigRef().getString("nats.user", "") : nats_username,
        .password = nats_password.empty() ? getContext()->getConfigRef().getString("nats.password", "") : nats_password,
        .token = nats_token.empty() ? getContext()->getConfigRef().getString("nats.token", "") : nats_token,
        .credential_file = nats_credential_file.empty() ? getContext()->getConfigRef().getString("nats.credential_file", "") : nats_credential_file,
        .max_connect_tries = static_cast<UInt64>((*nats_settings)[NATSSetting::nats_startup_connect_tries].value),
        .reconnect_wait = static_cast<int>((*nats_settings)[NATSSetting::nats_reconnect_wait].value),
        .secure = (*nats_settings)[NATSSetting::nats_secure].value
    };

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals((*nats_settings)[NATSSetting::nats_handle_error_mode]));
    setInMemoryMetadata(storage_metadata);

    nats_context = addSettings(getContext());
    nats_context->makeQueryContext();

    event_loop_thread = std::make_unique<ThreadFromGlobalPool>([this] { event_handler.runLoop(); });

    try
    {
        createConsumersConnection();
    }
    catch (...)
    {
        if (throw_on_startup_failure)
        {
            stopEventLoop();
            throw;
        }

        tryLogCurrentException(log);
    }

    streaming_task = getContext()->getMessageBrokerSchedulePool().createTask(getStorageID(), "NATSStreamingTask", [this] { streamingToViewsFunc(); });
    streaming_task->deactivate();

    initialize_consumers_task = getContext()->getMessageBrokerSchedulePool().createTask(getStorageID(), "NATSInitializeConsumersTask", [this] { initializeConsumersFunc(); });
    initialize_consumers_task->deactivate();
}
StorageNATS::~StorageNATS()
{
    stopEventLoop();
}

VirtualColumnsDescription StorageNATS::createVirtuals(StreamingHandleErrorMode handle_error_mode)
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_subject", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);

    if (handle_error_mode == StreamingHandleErrorMode::STREAM)
    {
        desc.addEphemeral("_raw_message", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
        desc.addEphemeral("_error", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    }

    return desc;
}

Names StorageNATS::parseList(const String & list, char delim)
{
    Names result;
    if (list.empty())
        return result;
    boost::split(result, list, [delim](char c) { return c == delim; });
    for (String & key : result)
        boost::trim(key);

    return result;
}


String StorageNATS::getTableBasedName(String name, const StorageID & table_id)
{
    if (name.empty())
        return fmt::format("{}_{}", table_id.database_name, table_id.table_name);
    return fmt::format("{}_{}_{}", name, table_id.database_name, table_id.table_name);
}


ContextMutablePtr StorageNATS::addSettings(ContextPtr local_context) const
{
    auto modified_context = Context::createCopy(local_context);
    modified_context->setSetting("input_format_skip_unknown_fields", true);
    modified_context->setSetting("input_format_allow_errors_ratio", 0.);
    if ((*nats_settings)[NATSSetting::nats_handle_error_mode] == StreamingHandleErrorMode::DEFAULT)
        modified_context->setSetting("input_format_allow_errors_num", (*nats_settings)[NATSSetting::nats_skip_broken_messages].value);
    else if ((*nats_settings)[NATSSetting::nats_handle_error_mode] == StreamingHandleErrorMode::DEAD_LETTER_QUEUE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DEAD_LETTER_QUEUE is not supported by the table engine");
    else
        modified_context->setSetting("input_format_allow_errors_num", Field{0});

    /// Since we are reusing the same context for all queries executed simultaneously, we don't want to used shared `analyze_count`
    modified_context->setSetting("max_analyze_depth", Field{0});

    if (!schema_name.empty())
        modified_context->setSetting("format_schema", schema_name);

    /// check for non-nats-related settings
    modified_context->applySettingsChanges(nats_settings->getFormatSettings());

    /// It does not make sense to use auto detection here, since the format
    /// will be reset for each message, plus, auto detection takes CPU
    /// time.
    modified_context->setSetting("input_format_csv_detect_header", false);
    modified_context->setSetting("input_format_tsv_detect_header", false);
    modified_context->setSetting("input_format_custom_detect_header", false);

    return modified_context;
}


void StorageNATS::stopEventLoop()
{
    event_handler.stopLoop();

    LOG_TRACE(log, "Waiting for event loop thread");
    Stopwatch watch;
    if (event_loop_thread)
    {
        if (event_loop_thread->joinable())
            event_loop_thread->join();
        event_loop_thread.reset();
    }
    LOG_TRACE(log, "Event loop thread finished in {} ms.", watch.elapsedMilliseconds());
}

void StorageNATS::initializeConsumersFunc()
{
    if (consumers_ready)
        return;

    try
    {
        createConsumersConnection();
        createConsumers();
    }
    catch (...)
    {
        LOG_WARNING(log, "Cannot initialize consumers: {}", getCurrentExceptionMessage(false));
        initialize_consumers_task->scheduleAfter(RESCHEDULE_MS);
        return;
    }

    size_t num_views = DatabaseCatalog::instance().getDependentViews(getStorageID()).size();
    if (num_views == 0)
    {
        initialize_consumers_task->scheduleAfter(RESCHEDULE_MS);
        return;
    }
    mv_attached.store(true);

    if (!subscribeConsumers())
    {
        initialize_consumers_task->scheduleAfter(RESCHEDULE_MS);
        return;
    }

    streaming_task->activateAndSchedule();
}

void StorageNATS::createConsumersConnection()
{
    if (consumers_connection)
        return;

    auto connect_future = event_handler.createConnection(configuration);
    consumers_connection = connect_future.get();
}

void StorageNATS::createConsumers()
{
    if (num_created_consumers != 0)
        return;

    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            pushConsumer(createConsumer());
            ++num_created_consumers;
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}

bool StorageNATS::subscribeConsumers()
{
    std::lock_guard lock(consumers_mutex);
    size_t num_initialized = 0;
    for (auto & consumer : consumers)
    {
        try
        {
            consumer->subscribe();
            ++num_initialized;
        }
        catch (...)
        {
            tryLogCurrentException(log);
            break;
        }
    }

    const bool are_consumers_initialized = num_initialized == num_created_consumers;
    if (are_consumers_initialized)
        consumers_ready.store(true);

    return are_consumers_initialized;
}

void StorageNATS::unsubscribeConsumers()
{
    std::lock_guard lock(consumers_mutex);
    for (auto & consumer : consumers)
        consumer->unsubscribe();

    consumers_ready.store(false);
}


/* Need to deactivate this way because otherwise might get a deadlock when first deactivate streaming task in shutdown and then
 * inside streaming task try to deactivate any other task
 */
void StorageNATS::deactivateTask(BackgroundSchedulePool::TaskHolder & task)
{
    std::unique_lock<std::mutex> lock(task_mutex, std::defer_lock);
    lock.lock();
    task->deactivate();
}


size_t StorageNATS::getMaxBlockSize() const
{
    return (*nats_settings)[NATSSetting::nats_max_block_size].changed ? (*nats_settings)[NATSSetting::nats_max_block_size].value
                                                      : (getContext()->getSettingsRef()[Setting::max_insert_block_size].value / num_consumers);
}


void StorageNATS::read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        size_t /* num_streams */)
{
    if (!consumers_connection || num_created_consumers == 0)
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "NATS consumers setup not finished. Connection might be not established");

    if (!local_context->getSettingsRef()[Setting::stream_like_engine_allow_direct_select])
        throw Exception(
            ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`. Be aware that usually the read data is removed from the queue.");

    if (mv_attached)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageNATS with attached materialized views");

    if (!getStreamName().empty() && getConsumerName().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "To read from NATS jet stream, you must specify `nats_consumer_name` setting");

    auto sample_block = storage_snapshot->getSampleBlockForColumns(column_names);
    auto modified_context = addSettings(local_context);

    if (!consumers_connection->isConnected())
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "No connection to {}", consumers_connection->connectionInfoForLog());

    Pipes pipes;
    pipes.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto nats_source = std::make_shared<NATSSource>(*this, storage_snapshot, modified_context, column_names, 1, (*nats_settings)[NATSSetting::nats_handle_error_mode]);

        auto converting_dag = ActionsDAG::makeConvertingActions(
            nats_source->getPort().getHeader().getColumnsWithTypeAndName(),
            sample_block.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            local_context);

        auto converting = std::make_shared<ExpressionActions>(std::move(converting_dag));
        auto converting_transform = std::make_shared<ExpressionTransform>(nats_source->getPort().getSharedHeader(), std::move(converting));

        pipes.emplace_back(std::move(nats_source));
        pipes.back().addTransform(std::move(converting_transform));
    }

    LOG_DEBUG(log, "Starting reading {} streams", pipes.size());
    auto pipe = Pipe::unitePipes(std::move(pipes));

    if (pipe.empty())
    {
        auto header = storage_snapshot->getSampleBlockForColumns(column_names);
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info);
    }
    else
    {
        auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), shared_from_this(), local_context, query_info);
        query_plan.addStep(std::move(read_step));
        query_plan.addInterpreterContext(modified_context);
    }
}


SinkToStoragePtr StorageNATS::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    auto modified_context = addSettings(local_context);
    std::string subject = modified_context->getSettingsRef()[Setting::stream_like_engine_insert_queue].changed
        ? modified_context->getSettingsRef()[Setting::stream_like_engine_insert_queue].value
        : "";
    if (subject.empty())
    {
        if (subjects.size() > 1)
        {
            throw Exception(
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "This NATS engine reads from multiple subjects. "
                            "You must specify `stream_like_engine_insert_queue` to choose the subject to write to");
        }

        subject = subjects[0];
    }

    auto pos = subject.find('*');
    if (pos != std::string::npos || subject.back() == '>')
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not publish to wildcard subject");

    if (!isSubjectInSubscriptions(subject))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Selected subject is not among engine subjects");

    size_t max_rows = max_rows_per_message;
    /// Need for backward compatibility.
    if (format_name == "Avro" && local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].changed)
        max_rows = local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].value;
    return std::make_shared<MessageQueueSink>(
        std::make_shared<const Block>(metadata_snapshot->getSampleBlockNonMaterialized()), getFormatName(), max_rows, createProducer(std::move(subject)), getName(), modified_context);}


void StorageNATS::startup()
{
    initialize_consumers_task->activateAndSchedule();
}


void StorageNATS::shutdown(bool /* is_drop */)
{
    shutdown_called = true;

    /// The order of deactivating tasks is important: wait for streamingToViews() func to finish and
    /// then wait for background event loop to finish.
    deactivateTask(streaming_task);

    /// In case it has not yet been able to setup connection;
    deactivateTask(initialize_consumers_task);

    /// Just a paranoid try catch, it is not actually needed.
    try
    {
        if (drop_table)
            unsubscribeConsumers();

        if (consumers_connection)
        {
            if (consumers_connection->isConnected())
                natsConnection_Flush(consumers_connection->getConnection());

            consumers_connection->disconnect();
        }

        for (size_t i = 0; i < num_created_consumers; ++i)
            popConsumer();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    stopEventLoop();
}

void StorageNATS::pushConsumer(INATSConsumerPtr consumer)
{
    std::lock_guard lock(consumers_mutex);
    consumers.push_back(consumer);
    semaphore.set();
}

INATSConsumerPtr StorageNATS::popConsumer()
{
    return popConsumer(std::chrono::milliseconds::zero());
}


INATSConsumerPtr StorageNATS::popConsumer(std::chrono::milliseconds timeout)
{
    // Wait for the first free consumer
    if (timeout == std::chrono::milliseconds::zero())
        semaphore.wait();
    else
    {
        if (!semaphore.tryWait(timeout.count()))
            return nullptr;
    }

    // Take the first available consumer from the list
    std::lock_guard lock(consumers_mutex);
    auto consumer = consumers.back();
    consumers.pop_back();

    return consumer;
}


INATSConsumerPtr StorageNATS::createConsumer()
{
    auto stream_name = getStreamName();
    if (stream_name.empty())
    {
        auto queue_name = (*nats_settings)[NATSSetting::nats_queue_group].changed ? (*nats_settings)[NATSSetting::nats_queue_group].value : getStorageID().getFullTableName();
        return std::make_shared<NATSCoreConsumer>(consumers_connection, subjects, queue_name, log, queue_size, shutdown_called);
    }

    auto queue_name = (*nats_settings)[NATSSetting::nats_queue_group];

    return std::make_shared<NATSJetStreamConsumer>(consumers_connection, std::move(stream_name), getConsumerName(), subjects, queue_name, log, queue_size, shutdown_called);
}

INATSProducerPtr StorageNATS::createProducer(String subject)
{
    auto connection_future = event_handler.createConnection(configuration);

    if (!getStreamName().empty())
        return std::make_unique<NATSJetStreamProducer>(connection_future.get(), std::move(subject), shutdown_called, log);
    else
        return std::make_unique<NATSCoreProducer>(connection_future.get(), std::move(subject), shutdown_called, log);
}

bool StorageNATS::isSubjectInSubscriptions(const std::string & subject)
{
    auto subject_levels = parseList(subject, '.');

    for (const auto & nats_subject : subjects)
    {
        auto nats_subject_levels = parseList(nats_subject, '.');
        size_t levels_to_check = 0;
        if (!nats_subject_levels.empty() && nats_subject_levels.back() == ">")
            levels_to_check = nats_subject_levels.size() - 1;
        if (levels_to_check)
        {
            if (subject_levels.size() < levels_to_check)
                continue;
        }
        else
        {
            if (subject_levels.size() != nats_subject_levels.size())
                continue;
            levels_to_check = nats_subject_levels.size();
        }

        bool is_same = true;
        for (size_t i = 0; i < levels_to_check; ++i)
        {
            if (nats_subject_levels[i] == "*")
                continue;

            if (subject_levels[i] != nats_subject_levels[i])
            {
                is_same = false;
                break;
            }
        }
        if (is_same)
            return true;
    }

    return false;
}

bool StorageNATS::checkDependencies(const StorageID & table_id)
{
    return !DatabaseCatalog::instance().getReadyDependentViews(table_id, getContext()).empty();
}

void StorageNATS::streamingToViewsFunc()
{
    auto table_id = getStorageID();

    bool consumers_queues_are_empty = false;

    try
    {
        if (consumers_connection && consumers_connection->isConnected())
        {
            auto start_time = std::chrono::steady_clock::now();

            mv_attached.store(true);

            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!shutdown_called && num_created_consumers > 0)
            {
                if (!checkDependencies(table_id))
                {
                    consumers_queues_are_empty = true;
                    break;
                }

                LOG_DEBUG(log, "Started streaming to attached views");

                if (streamToViews())
                {
                    /// Reschedule with backoff.
                    consumers_queues_are_empty = true;
                    break;
                }

                auto end_time = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
                if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                {
                    LOG_TRACE(log, "Reschedule streaming. Thread work duration limit exceeded");
                    consumers_queues_are_empty = false;
                    break;
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (shutdown_called)
        return;

    size_t num_views = DatabaseCatalog::instance().getDependentViews(table_id).size();

    if (num_views != 0)
    {
        if (consumers_queues_are_empty)
            streaming_task->scheduleAfter(RESCHEDULE_MS);
        else
            streaming_task->schedule();

        return;
    }
    else if (consumers_ready)
        unsubscribeConsumers();

    if (!consumers_queues_are_empty)
    {
        streaming_task->schedule();
        return;
    }

    initialize_consumers_task->schedule();
    mv_attached.store(false);
}


bool StorageNATS::streamToViews()
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist", table_id.getNameForLogs());

    // Create an INSERT query for streaming data
    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = table_id;

    auto new_context = Context::createCopy(nats_context);

    /// Create a fresh query context from nats_context, discarding any caches attached to the previous context to
    /// ensure no stale state is reused.
    new_context->makeQueryContext();

    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(
        insert,
        new_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_isnert */ false);
    auto block_io = interpreter.execute();

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(getContext(), false), getContext());
    auto column_names = block_io.pipeline.getHeader().getNames();
    auto sample_block = storage_snapshot->getSampleBlockForColumns(column_names);

    auto block_size = getMaxBlockSize();

    // Create a stream for each consumer and join them in a union stream
    std::vector<std::shared_ptr<NATSSource>> sources;
    Pipes pipes;
    sources.reserve(num_created_consumers);
    pipes.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto source = std::make_shared<NATSSource>(*this, storage_snapshot, new_context, column_names, block_size, (*nats_settings)[NATSSetting::nats_handle_error_mode]);
        sources.emplace_back(source);
        pipes.emplace_back(source);

        Poco::Timespan max_execution_time = (*nats_settings)[NATSSetting::nats_flush_interval_ms].changed
            ? (*nats_settings)[NATSSetting::nats_flush_interval_ms]
            : getContext()->getSettingsRef()[Setting::stream_flush_interval_ms];

        source->setTimeLimit(max_execution_time);
    }

    block_io.pipeline.complete(Pipe::unitePipes(std::move(pipes)));

    {
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }

    if (!consumers_connection || !consumers_connection->isConnected())
    {
        LOG_TRACE(log, "Reschedule streaming. Unable to restore connection");
        return true;
    }

    size_t queue_empty = 0;
    for (auto & source : sources)
    {
        if (source->queueEmpty())
            ++queue_empty;
    }

    if (queue_empty == num_created_consumers)
    {
        LOG_TRACE(log, "Reschedule streaming. Queues are empty");
        return true;
    }

    LOG_TRACE(log, "Reschedule streaming. Queues are not empty");

    return false;
}

String StorageNATS::getStreamName() const
{
    return getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_stream]);
}
String StorageNATS::getConsumerName() const
{
    return getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_consumer_name]);
}

void registerStorageNATS(StorageFactory & factory);
void registerStorageNATS(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        auto nats_settings = std::make_unique<NATSSettings>();
        if (auto named_collection = tryGetNamedCollectionWithOverrides(args.engine_args, args.getLocalContext(), true, nullptr, &args.table_id))
        {
            nats_settings->loadFromNamedCollection(named_collection);
        }
        else if (!args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NATS engine must have settings");

        nats_settings->loadFromQuery(*args.storage_def);

        if (!(*nats_settings)[NATSSetting::nats_url].changed && !(*nats_settings)[NATSSetting::nats_server_list].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify either `nats_url` or `nats_server_list` settings");

        if (!(*nats_settings)[NATSSetting::nats_format].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `nats_format` setting");

        if (!(*nats_settings)[NATSSetting::nats_subjects].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `nats_subjects` setting");

        if ((*nats_settings)[NATSSetting::nats_consumer_name].changed && !(*nats_settings)[NATSSetting::nats_stream].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "To use NATS jet stream, you must specify `nats_stream` setting");

        return std::make_shared<StorageNATS>(args.table_id, args.getContext(), args.columns, args.comment, std::move(nats_settings), args.mode);
    };

    factory.registerStorage(
        "NATS",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .source_access_type = AccessTypeObjects::Source::NATS,
            .has_builtin_setting_fn = NATSSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
This engine allows integrating ClickHouse with [NATS](https://nats.io/).

`NATS` lets you:

- Publish or subscribe to message subjects.
- Process new messages as they become available.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = NATS SETTINGS
    nats_url = 'host:port',
    nats_subjects = 'subject1,subject2,...',
    nats_format = 'data_format'[,]
    [nats_schema = '',]
    [nats_num_consumers = N,]
    [nats_queue_group = 'group_name',]
    [nats_secure = false,]
    [nats_max_reconnect = N,]
    [nats_reconnect_wait = N,]
    [nats_server_list = 'host1:port1,host2:port2,...',]
    [nats_skip_broken_messages = N,]
    [nats_max_block_size = N,]
    [nats_flush_interval_ms = N,]
    [nats_username = 'user',]
    [nats_password = 'password',]
    [nats_token = 'clickhouse',]
    [nats_credential_file = '/var/nats_credentials',]
    [nats_startup_connect_tries = '5']
    [nats_max_rows_per_message = 1,]
    [nats_handle_error_mode = 'default']
```

Required parameters:

- `nats_url` – host:port (for example, `localhost:5672`)..
- `nats_subjects` – List of subject for NATS table to subscribe/publish to. Supports wildcard subjects like `foo.*.bar` or `baz.>`
- `nats_format` – Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](../../../interfaces/formats.md) section.

Optional parameters:

- `nats_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/) requires the path to the schema file and the name of the root `schema.capnp:Message` object.
- `nats_stream` – The name of an existing stream in NATS JetStream.
- `nats_consumer` – The name of an existing durable  pull consumer in NATS JetStream.
- `nats_num_consumers` – The number of consumers per table. Default: `1`. Specify more consumers if the throughput of one consumer is insufficient for NATS core only.
- `nats_queue_group` – Name for queue group of NATS subscribers. Default is the table name.
- `nats_max_reconnect` – Deprecated and has no effect, reconnect is performed permanently with nats_reconnect_wait timeout.
- `nats_reconnect_wait` – Amount of time in milliseconds to sleep between each reconnect attempt. Default: `5000`.
- `nats_server_list` - Server list for connection. Can be specified to connect to NATS cluster.
- `nats_skip_broken_messages` - NATS message parser tolerance to schema-incompatible messages per block. Default: `0`. If `nats_skip_broken_messages = N` then the engine skips *N* NATS messages that cannot be parsed (a message equals a row of data).
- `nats_max_block_size` - Number of row collected by poll(s) for flushing data from NATS. Default: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size).
- `nats_flush_interval_ms` - Timeout for flushing data read from NATS. Default: [stream_flush_interval_ms](/operations/settings/settings#stream_flush_interval_ms).
- `nats_username` - NATS username.
- `nats_password` - NATS password.
- `nats_token` - NATS auth token.
- `nats_credential_file` - Path to a NATS credentials file.
- `nats_startup_connect_tries` - Number of connect tries at startup. Default: `5`.
- `nats_max_rows_per_message` — The maximum number of rows written in one NATS message for row-based formats. (default : `1`).
- `nats_handle_error_mode` — How to handle errors for NATS engine. Possible values: default (the exception will be thrown if we fail to parse a message), stream (the exception message and raw message will be saved in virtual columns `_error` and `_raw_message`).

SSL connection:

For secure connection use `nats_secure = 1`.
Certificate verification is controlled by the `CLICKHOUSE_NATS_TLS_SECURE` environment variable;
If the certificate is expired, self-signed, missing, or otherwise invalid, disable verification by setting `CLICKHOUSE_NATS_TLS_SECURE=0`.

Writing to NATS table:

If table reads only from one subject, any insert will publish to the same subject.
However, if table reads from multiple subjects, we need to specify which subject we want to publish to.
That is why whenever inserting into table with multiple subjects, setting `stream_like_engine_insert_queue` is needed.
You can select one of the subjects the table reads from and publish your data there. For example:

```sql
CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1,subject2',
             nats_format = 'JSONEachRow';

INSERT INTO queue
SETTINGS stream_like_engine_insert_queue = 'subject2'
VALUES (1, 1);
```

Also format settings can be added along with nats-related settings.

Example:

```sql
CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';
```

The NATS server configuration can be added using the ClickHouse config file.
More specifically you can add your password for the NATS engine:

```xml
<nats>
    <user>click</user>
    <password>house</password>
    <token>clickhouse</token>
</nats>
```

## Description {#description}

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1.  Use the engine to create a NATS consumer and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from NATS and convert them to the required format using `SELECT`.
One NATS table can have as many materialized views as you like, they do not read data from the table directly, but receive new records (in blocks), this way you can write to several tables with different detail level (with grouping - aggregation and without).

Example:

```sql
CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';

CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

SELECT key, value FROM daily ORDER BY key;
```

To stop receiving streams data or to change the conversion logic, detach the materialized view:

```sql
DETACH TABLE consumer;
ATTACH TABLE consumer;
```

If you want to change the target table by using `ALTER`, we recommend disabling the material view to avoid discrepancies between the target table and the data from the view.

## Virtual columns {#virtual-columns}

- `_subject` - NATS message subject. Data type: `String`.

Additional virtual columns when `nats_handle_error_mode='stream'`:

- `_raw_message` - Raw message that couldn't be parsed successfully. Data type: `Nullable(String)`.
- `_error` - Exception message happened during failed parsing. Data type: `Nullable(String)`.

Note: `_raw_message` and `_error` virtual columns are filled only in case of exception during parsing, they are always `NULL` when message was parsed successfully.

## Data formats support {#data-formats-support}

NATS engine supports all [formats](../../../interfaces/formats.md) supported in ClickHouse.
The number of rows in one NATS message depends on whether the format is row-based or block-based:

- For row-based formats the number of rows in one NATS message can be controlled by setting `nats_max_rows_per_message`.
- For block-based formats we cannot divide block into smaller parts, but the number of rows in one block can be controlled by general setting [max_block_size](/operations/settings/settings#max_block_size).

## Using JetStream {#using-jetstream}

Before using NATS engine with NATS JetStream, you must create a NATS stream and a durable pull consumer. For this, you can use, for example, the nats utility from the [NATS CLI](https://github.com/nats-io/natscli) package:
<details>
<summary>creating stream</summary>

```bash
$ nats stream add
? Stream Name stream_name
? Subjects stream_subject
? Storage file
? Replication 1
? Retention Policy Limits
? Discard Policy Old
? Stream Messages Limit -1
? Per Subject Messages Limit -1
? Total Stream Size -1
? Message TTL -1
? Max Message Size -1
? Duplicate tracking time window 2m0s
? Allow message Roll-ups No
? Allow message deletion Yes
? Allow purging subjects or the entire stream Yes
Stream stream_name was created

Information for Stream stream_name created 2025-10-03 14:12:51

                Subjects: stream_subject
                Replicas: 1
                 Storage: File

Options:

               Retention: Limits
         Acknowledgments: true
          Discard Policy: Old
        Duplicate Window: 2m0s
              Direct Get: true
       Allows Msg Delete: true
            Allows Purge: true
Allows Per-Message TTL: false
          Allows Rollups: false

Limits:

        Maximum Messages: unlimited
     Maximum Per Subject: unlimited
           Maximum Bytes: unlimited
             Maximum Age: unlimited
    Maximum Message Size: unlimited
       Maximum Consumers: unlimited

State:

                Messages: 0
                   Bytes: 0 B
          First Sequence: 0
           Last Sequence: 0
        Active Consumers: 0
```
</details>

<details>
<summary>creating durable pull consumer</summary>

```bash
$ nats consumer add
? Select a Stream stream_name
? Consumer name consumer_name
? Delivery target (empty for Pull Consumers)
? Start policy (all, new, last, subject, 1h, msg sequence) all
? Acknowledgment policy explicit
? Replay policy instant
? Filter Stream by subjects (blank for all)
? Maximum Allowed Deliveries -1
? Maximum Acknowledgments Pending 0
? Deliver headers only without bodies No
? Add a Retry Backoff Policy No
Information for Consumer stream_name > consumer_name created 2025-10-03T14:13:51+03:00

Configuration:

                    Name: consumer_name
               Pull Mode: true
          Deliver Policy: All
              Ack Policy: Explicit
                Ack Wait: 30.00s
           Replay Policy: Instant
         Max Ack Pending: 1,000
       Max Waiting Pulls: 512

State:

Last Delivered Message: Consumer sequence: 0 Stream sequence: 0
    Acknowledgment Floor: Consumer sequence: 0 Stream sequence: 0
        Outstanding Acks: 0 out of maximum 1,000
    Redelivered Messages: 0
    Unprocessed Messages: 0
           Waiting Pulls: 0 of maximum 512
```
</details>

After creating stream and durable pull consumer, we can create a table with NATS engine. To do this, you need to initialize: nats_stream, nats_consumer_name, and nats_subjects:

```SQL
CREATE TABLE nats_jet_stream (
    key UInt64,
    value UInt64
  ) ENGINE NATS
    SETTINGS  nats_url = 'localhost:4222',
              nats_stream = 'stream_name',
              nats_consumer_name = 'consumer_name',
              nats_subjects = 'stream_subject',
              nats_format = 'JSONEachRow';
```
)DOCS_MD",
            .syntax = "ENGINE = NATS() SETTINGS nats_url = 'host:port', nats_subjects = 'subject', nats_format = 'format', ...",
            .related = {"Kafka", "RabbitMQ", "FileLog"}});
}

}
