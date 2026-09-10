#include <algorithm>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MessageQueueSink.h>
#include <Storages/NATS/NATSCoreConsumer.h>
#include <Storages/NATS/NATSCoreProducer.h>
#include <Storages/NATS/NATSJetStreamConsumer.h>
#include <Storages/NATS/NATSJetStreamProducer.h>
#include <Storages/NATS/NATSSettings.h>
#include <Storages/NATS/NATSSource.h>
#include <Storages/NATS/StorageNATS.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Macros.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool allow_named_collection_override_by_default;
extern const SettingsNonZeroUInt64 max_insert_block_size;
extern const SettingsMilliseconds rabbitmq_max_wait_ms;
extern const SettingsMilliseconds stream_flush_interval_ms;
extern const SettingsBool stream_like_engine_allow_direct_select;
extern const SettingsString stream_like_engine_insert_queue;
extern const SettingsUInt64 output_format_avro_rows_in_file;
}

namespace NATSSetting
{
extern const NATSSettingsString nats_credentials;
extern const NATSSettingsString nats_credential_file;
extern const NATSSettingsMilliseconds nats_flush_interval_ms;
extern const NATSSettingsBool nats_wait_for_flush_interval;
extern const NATSSettingsBool nats_commit_on_select;
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

namespace FailPoints
{
extern const char nats_pause_before_building_insert_pipeline[];
}

StorageNATS::StorageNATS(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & comment,
    std::unique_ptr<NATSSettings> nats_settings_,
    LoadingStrictnessLevel mode,
    bool authentication_determined_by_table_)
    : IStreamingStorage(table_id_)
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
    auto nats_credentials = getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_credentials]);
    /// `libnats` sends every configured authentication method in the `CONNECT` frame, so a table
    /// authentication method must not be combined with the server-global fallback: inline
    /// credentials can be used with a query-supplied destination.
    /// The decision is made on provenance rather than on the resulting values. A query which
    /// clears the authentication a named collection carries - `nats_username = ''` - would
    /// otherwise resurrect the global fallback and send the global account to the destination it
    /// selected. `authentication_determined_by_table` says whether the table definition decided
    /// its authentication at all, in either direction, including clearing it.
    const bool has_table_authentication = authentication_determined_by_table_ || !nats_username.empty() || !nats_password.empty()
        || !nats_token.empty() || !nats_credential_file.empty() || !nats_credentials.empty();

    const String global_username = has_table_authentication ? "" : getContext()->getConfigRef().getString("nats.user", "");
    const String global_password = has_table_authentication ? "" : getContext()->getConfigRef().getString("nats.password", "");
    const String global_token = has_table_authentication ? "" : getContext()->getConfigRef().getString("nats.token", "");
    /// A path in the server configuration file is an operator-selected source, like the other
    /// global fallbacks, so it keeps the established behavior of authenticating tables which
    /// define no authentication of their own.
    const String global_credential_file
        = has_table_authentication ? "" : getContext()->getConfigRef().getString("nats.credential_file", "");

    configuration
        = {.url = getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_url]),
           .servers = parseList(getContext()->getMacros()->expand((*nats_settings)[NATSSetting::nats_server_list]), ','),
           .username = nats_username.empty() ? global_username : nats_username,
           .password = nats_password.empty() ? global_password : nats_password,
           .token = nats_token.empty() ? global_token : nats_token,
           .credential_file = nats_credential_file.empty() ? global_credential_file : nats_credential_file,
           .credentials = nats_credentials,
           .max_connect_tries = static_cast<UInt64>((*nats_settings)[NATSSetting::nats_startup_connect_tries].value),
           .reconnect_wait = static_cast<int>((*nats_settings)[NATSSetting::nats_reconnect_wait].value),
           .secure = (*nats_settings)[NATSSetting::nats_secure].value};

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
        if (!getContext()->getMessageQueueDisableInsertion())
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

    streaming_task
        = getContext()->getMessageBrokerSchedulePool()->createTask(getStorageID(), "NATSStreamingTask", [this] { threadFunc(); });
    streaming_task->deactivate();

    initialize_consumers_task = getContext()->getMessageBrokerSchedulePool()->createTask(
        getStorageID(), "NATSInitializeConsumersTask", [this] { initializeConsumersFunc(); });
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
    desc.addEphemeral(
        "_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);

    if (handle_error_mode == StreamingHandleErrorMode::STREAM)
    {
        desc.addEphemeral(
            "_raw_message",
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            "",
            VirtualsMaterializationPlace::Reader);
        desc.addEphemeral(
            "_error", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
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
        stream_control.claimCycle(last_seen_refresh_epoch);
        initialize_consumers_task->scheduleAfter(RESCHEDULE_MS);
        return;
    }

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
            /// A consumer can reach this point still subscribed, because `unsubscribeConsumers`
            /// only reaches the consumers that are in the pool at that moment: a direct `SELECT`
            /// holds one out of the pool and hands it back subscribed whenever it found it that
            /// way. What such a consumer buffered belongs to the window when the table was not
            /// streaming - messages published while the materialized view was detached - and must
            /// not be inserted into the views by the cycles that follow. Clearing the queue alone
            /// does not guarantee that: `onMsg` keeps appending from the NATS client thread, and
            /// `subscribe` does nothing for a consumer that is subscribed already, so the live
            /// subscription is replaced the way `unsubscribeConsumers` would have replaced it. The
            /// queue is finished before the drain inside `unsubscribe` delivers what the
            /// subscription still holds, so nothing from that window can land behind the cleanup,
            /// and a JetStream message goes back to the broker while the subscription it arrived
            /// on is still alive, so it is redelivered at once rather than after the ACK deadline.
            if (consumer->isSubscribed())
            {
                consumer->finishAndReturnUnprocessed(INATSConsumer::SkippedMessages::Acknowledge);
                consumer->unsubscribe();
            }

            /// What an unsubscribed consumer still holds are leftovers of a subscription that is
            /// already gone, which can only be thrown away.
            consumer->dropBuffered();
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
    {
        consumers_ready.store(true);
        subscription_stale.store(false);
    }

    return are_consumers_initialized;
}

void StorageNATS::resubscribeStaleConsumers()
{
    std::lock_guard lock(consumers_mutex);
    for (auto & consumer : consumers)
    {
        if (!consumer->needsResubscribe())
            continue;

        /// Nothing the consumer holds locally can outlive its subscription: a `natsMsg` keeps a
        /// plain pointer to the `natsSubscription` it arrived on, and `natsMsg_Ack` follows it to
        /// reach the JetStream context and the connection, so acknowledging a message whose
        /// subscription has been destroyed reads freed memory. Recovery therefore prefers a
        /// consumer that holds nothing: the streaming cycles insert and acknowledge what the
        /// broker delivered before it went stale, and a stale subscription delivers nothing more,
        /// so the queue does drain and the reconnect this keys on is still reported once it has.
        if (!consumer->queueEmpty())
        {
            LOG_DEBUG(log, "A subscription stopped consuming from the NATS server, resubscribing once the buffered messages are drained");
            continue;
        }

        LOG_INFO(log, "A subscription stopped consuming from the NATS server, resubscribing");

        /// The check above is only a snapshot: `onMsg` runs on the NATS client thread and appends
        /// to the queue without `consumers_mutex`, and the drain inside `unsubscribe` delivers
        /// whatever the subscription still has. So the messages are returned to the broker rather
        /// than destroyed, while the subscription they arrived on is still alive, and the queue is
        /// finished first so that nothing can be appended behind that.
        consumer->finishAndReturnUnprocessed(INATSConsumer::SkippedMessages::Acknowledge);
        consumer->unsubscribe();

        try
        {
            consumer->subscribe();
        }
        catch (...)
        {
            tryLogCurrentException(log);
            /// A consumer left unsubscribed no longer reports that it needs to be recovered, so it
            /// would stay silent. Hand it to `subscribeConsumers`, which only runs while the
            /// consumers are not ready.
            consumers_ready.store(false);
            break;
        }
    }
}

void StorageNATS::unsubscribeConsumers()
{
    std::lock_guard lock(consumers_mutex);
    for (auto & consumer : consumers)
    {
        consumer->finishAndReturnUnprocessed(INATSConsumer::SkippedMessages::Acknowledge);
        consumer->unsubscribe();
    }

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
    return (*nats_settings)[NATSSetting::nats_max_block_size].changed
        ? (*nats_settings)[NATSSetting::nats_max_block_size].value
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
            ErrorCodes::QUERY_NOT_ALLOWED,
            "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`. Be aware that usually the read "
            "data is removed from the queue.");

    if (!DatabaseCatalog::instance().getDependentViews(getStorageID()).empty())
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
        auto nats_source = std::make_shared<NATSSource>(
            *this, storage_snapshot, modified_context, column_names, 1, (*nats_settings)[NATSSetting::nats_handle_error_mode]);
        nats_source->setCommitOnSelect((*nats_settings)[NATSSetting::nats_commit_on_select]);
        nats_source->setTimeLimit(modified_context->getSettingsRef()[Setting::rabbitmq_max_wait_ms]);
        nats_source->setWaitForFlushInterval(true);

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


SinkToStoragePtr
StorageNATS::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
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
        std::make_shared<const Block>(metadata_snapshot->getSampleBlockNonMaterialized()),
        getFormatName(),
        max_rows,
        createProducer(std::move(subject)),
        getName(),
        modified_context);
}


void StorageNATS::startup()
{
    if (getContext()->getMessageQueueDisableInsertion())
    {
        LOG_INFO(log, "Streaming to views is disabled");
        return;
    }

    initialize_consumers_task->activateAndSchedule();
}

void StorageNATS::scheduleStreamingTasksImpl()
{
    streaming_task->schedule();
}

ActionLock StorageNATS::getActionLock(StorageActionBlockType action_type)
{
    auto lock = IStreamingStorage::getActionLock(action_type);
    if (action_type == ActionLocks::StreamConsume)
        subscription_stale.store(true);
    return lock;
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
        auto queue_name = (*nats_settings)[NATSSetting::nats_queue_group].changed ? (*nats_settings)[NATSSetting::nats_queue_group].value
                                                                                  : getStorageID().getFullTableName();
        return std::make_shared<NATSCoreConsumer>(consumers_connection, subjects, queue_name, log, queue_size, shutdown_called);
    }

    auto queue_name = (*nats_settings)[NATSSetting::nats_queue_group];

    return std::make_shared<NATSJetStreamConsumer>(
        consumers_connection, std::move(stream_name), getConsumerName(), subjects, queue_name, log, queue_size, shutdown_called);
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

void StorageNATS::threadFunc()
{
    auto table_id = getStorageID();

    bool consumers_queues_are_empty = false;

    if (consumers_ready && subscription_stale.exchange(false))
        unsubscribeConsumers();

    /// A subscription the NATS client has closed, or one that outlived a reconnect, never receives
    /// another message, so replace it here, keeping everything the consumer already holds locally.
    if (consumers_ready)
        resubscribeStaleConsumers();

    const size_t num_views = DatabaseCatalog::instance().getDependentViews(table_id).size();
    const bool is_connected = consumers_connection && consumers_connection->isConnected();
    const UInt64 cycle_epoch = stream_control.currentCancelEpoch();
    const UInt64 refresh_epoch = last_seen_refresh_epoch.load();
    const bool deps_ready = num_views == 0 || checkDependencies(table_id);
    const bool run_cycle = is_connected && deps_ready && stream_control.claimCycle(last_seen_refresh_epoch);

    try
    {
        if (num_views && run_cycle)
        {
            if (!consumers_ready && !subscribeConsumers())
            {
                /// Give back a REFRESH permit consumed by `claimCycle`, so the refresh still
                /// runs once subscribing succeeds, even if the table is stopped by then.
                last_seen_refresh_epoch.store(refresh_epoch);
                unsubscribeConsumers();
                streaming_task->scheduleAfter(RESCHEDULE_MS);
                return;
            }

            auto start_time = std::chrono::steady_clock::now();

            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (consumers_ready && !shutdown_called && num_created_consumers > 0)
            {
                if (!checkDependencies(table_id))
                {
                    consumers_queues_are_empty = true;
                    break;
                }

                LOG_DEBUG(log, "Started streaming to {} attached views", num_views);

                if (streamToViews(cycle_epoch))
                {
                    /// Reschedule with backoff.
                    consumers_queues_are_empty = true;
                    break;
                }

                if (stream_control.isBlocked() || stream_control.isCancelRequested(cycle_epoch))
                    break;

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

    if (num_views != 0)
    {
        if (stream_control.isBlocked() && consumers_ready)
            unsubscribeConsumers();

        /// While paused/stopped/views unready the loop above does no work, so reschedule with a delay to avoid
        /// busy-looping; SYSTEM START reschedules it promptly via `scheduleStreamingTasks`.
        if (consumers_queues_are_empty || stream_control.isBlocked() || !deps_ready)
            streaming_task->scheduleAfter(RESCHEDULE_MS);
        else
            streaming_task->schedule();

        return;
    }
    else if (consumers_ready)
        unsubscribeConsumers();

    initialize_consumers_task->schedule();
}


bool StorageNATS::streamToViews(UInt64 cycle_epoch)
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

    FailPointInjection::pauseFailPoint(FailPoints::nats_pause_before_building_insert_pipeline);

    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(
        insert,
        new_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_isnert */ false);
    auto block_io = interpreter.execute();

    /// `threadFunc` streams only while the table has ready dependent views, but the interpreter looks
    /// them up again, and a `DROP VIEW` or `DETACH TABLE` landing in between leaves it with nowhere
    /// to insert into: the pipeline it builds then discards whatever the sources consume (see
    /// `InsertDependenciesBuilder::createChainWithDependencies`), and the acknowledgement below
    /// would confirm to the broker messages that were never inserted anywhere. Such a cycle must not
    /// consume anything. What the consumers hold goes back to the broker when the next cycle finds
    /// the last view gone and unsubscribes them, or stays with them until the view is attached again.
    /// This repeats the readiness check of `threadFunc`, not a check of the dependency metadata or of
    /// the shape of the pipeline: a plain `DETACH TABLE` keeps the dependency registered while the
    /// view is gone, and a materialized view whose target is `Null` legitimately ends in the same
    /// discarding sink while being a view the table streams to. The check can only err towards
    /// skipping a cycle whose pipeline would have inserted somewhere, which consumes nothing.
    if (!checkDependencies(table_id))
    {
        LOG_DEBUG(log, "The last materialized view was dropped or detached while the streaming cycle was being prepared, nothing to stream to");
        return true;
    }

    const auto metadata_snapshot = getInMemoryMetadataPtr(getContext(), false);
    auto storage_snapshot = getStorageSnapshot(metadata_snapshot, getContext());
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
        auto source = std::make_shared<NATSSource>(
            *this,
            storage_snapshot,
            new_context,
            column_names,
            block_size,
            (*nats_settings)[NATSSetting::nats_handle_error_mode],
            cycle_epoch);
        sources.emplace_back(source);
        pipes.emplace_back(source);

        const bool flush_interval_set = (*nats_settings)[NATSSetting::nats_flush_interval_ms].changed;
        Poco::Timespan max_execution_time = flush_interval_set ? (*nats_settings)[NATSSetting::nats_flush_interval_ms]
                                                               : getContext()->getSettingsRef()[Setting::stream_flush_interval_ms];

        source->setTimeLimit(max_execution_time);
        /// Only hold blocks open for the whole flush interval when `nats_wait_for_flush_interval` is set.
        source->setWaitForFlushInterval(
            (*nats_settings)[NATSSetting::nats_wait_for_flush_interval] && max_execution_time.totalMicroseconds() > 0);
        source->setBackgroundStreaming(true);
    }

    block_io.pipeline.complete(Pipe::unitePipes(std::move(pipes)));

    {
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }

    for (auto & source : sources)
    {
        if (source->wasConsumptionAborted())
            continue;
        if (auto source_consumer = source->getConsumer())
            source_consumer->ackConsumed();
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

namespace
{

constexpr std::string_view CREDENTIAL_FILE_ONLY_FROM_CONFIG_MESSAGE
    = "`nats_credential_file` can only be specified in a named collection defined in the server configuration file. "
      "Pass the contents of the file in `nats_credentials` instead";

/// `nats_credential_file` and `nats_credentials` are two ways to provide the same credentials - a path to a
/// file on the server filesystem, and the contents of that file - so only one of them may be specified.
/// A source specified in the query (in the `SETTINGS` clause or as a named-collection override) is more
/// specific than one stored in the named collection, so it replaces it instead of conflicting with it -
/// otherwise a named collection with one of the sources could not be reused by a table which provides the
/// other one.
///
/// `credential_file_assigned_by_query` and `credentials_assigned_by_query` say whether the query assigned
/// the key, in either spelling: as a named-collection override, or in the `SETTINGS` clause, which is
/// applied on top of the collection values. They are about provenance rather than about the resulting
/// value: an assignment of the empty string is an assignment too, and dropping the credentials the
/// operator configured is exactly what has to be refused.
/// Returns whether the table definition decided its authentication itself, so the server-global
/// fallback must not be applied. This is provenance, not value: a query which clears the
/// authentication a named collection carries has decided it too, and resurrecting the global
/// account for the destination that query selected is exactly what has to be prevented.
bool resolveCredentialSource(
    NATSSettings & nats_settings,
    const NamedCollection * named_collection,
    bool collection_defined_in_config,
    bool credential_file_assigned_by_query,
    bool credentials_assigned_by_query,
    bool username_assigned_by_query,
    bool password_assigned_by_query,
    bool token_assigned_by_query,
    bool destination_assigned_by_query,
    bool allow_named_collection_override_by_default,
    bool loading_from_existing_metadata)
{
    /// The value the named collection defines itself, before a query override of the same key.
    auto value_in_collection = [&](const std::string & key) -> String
    {
        if (!named_collection)
            return {};
        if (named_collection->isQueryOverridden(key))
            return named_collection->getValueBeforeQueryOverride(key).value_or("");
        return named_collection->getOrDefault<String>(key, "");
    };

    const String credential_file_in_collection = value_in_collection("nats_credential_file");
    const String credentials_in_collection = value_in_collection("nats_credentials");
    const String username_in_collection = value_in_collection("nats_username");
    const String password_in_collection = value_in_collection("nats_password");
    const String token_in_collection = value_in_collection("nats_token");

    const bool credentials_set = !nats_settings[NATSSetting::nats_credentials].value.empty();

    const bool credential_file_from_collection = !credential_file_in_collection.empty() && !credential_file_assigned_by_query;
    const bool credentials_from_collection = !credentials_in_collection.empty() && !credentials_assigned_by_query;
    const bool username_from_collection = !username_in_collection.empty() && !username_assigned_by_query;
    const bool password_from_collection = !password_in_collection.empty() && !password_assigned_by_query;
    const bool token_from_collection = !token_in_collection.empty() && !token_assigned_by_query;
    /// Query-supplied inline credentials replace a configured credentials file before `libnats`
    /// opens the connection. In that case the file is no longer a credential source that needs
    /// to stay bound to the collection's destination.
    const bool credential_file_remains_from_collection = credential_file_from_collection && !credentials_assigned_by_query;
    const bool trusted_credentials_from_collection = collection_defined_in_config
        && (credential_file_remains_from_collection || credentials_from_collection || username_from_collection || password_from_collection
            || token_from_collection);

    const bool credential_file_from_query
        = !nats_settings[NATSSetting::nats_credential_file].value.empty() && !credential_file_from_collection;
    const bool credentials_from_query = credentials_set && !credentials_from_collection;

    if (credential_file_from_query && credentials_from_query)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "You can specify only one of `nats_credential_file` and `nats_credentials`");

    if (!credential_file_in_collection.empty() && !credentials_in_collection.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "The named collection can specify only one of `nats_credential_file` and `nats_credentials`");

    /// `libnats` sends all configured authentication fields in its `CONNECT` frame. The final
    /// effective settings must therefore select exactly one authentication family. This is checked
    /// independently of provenance: query overrides can otherwise combine methods on an auth-empty
    /// named collection, and a named collection itself can store the ambiguous configuration.
    /// A credentials file and inline credentials are the two representations of one family; their
    /// query-side replacement is handled below.
    const bool has_inline_credentials = !nats_settings[NATSSetting::nats_credential_file].value.empty() || credentials_set;
    const bool has_user_info = !nats_settings[NATSSetting::nats_username].value.empty()
        || !nats_settings[NATSSetting::nats_password].value.empty();
    const bool has_token = !nats_settings[NATSSetting::nats_token].value.empty();

    if ((has_inline_credentials && (has_user_info || has_token)) || (has_user_info && has_token))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Credentials from different authentication methods cannot be combined");

    /// Credentials read from a named collection in the server configuration are secrets selected
    /// by the operator. They must not be sent to an endpoint selected by the query. The global
    /// `nats.user`, `nats.password`, and `nats.token` fallbacks intentionally retain their
    /// established behavior: they authenticate tables whose destination is defined in SQL.
    /// Loading an existing table definition is exempt so an upgrade does not make previously valid
    /// metadata unloadable.
    if (!loading_from_existing_metadata && trusted_credentials_from_collection && destination_assigned_by_query)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "`nats_url` and `nats_server_list` cannot be overridden when credentials come from the server configuration file");

    /// Credentials the operator explicitly locked (`<nats_credential_file overridable="false">`) cannot be
    /// replaced from a query. `tryGetNamedCollectionWithOverrides` checks this for the engine-argument
    /// spelling - `nats_credentials` inherits the permission of the path key it replaces, see
    /// `findOverrideForbiddingKey` - but the `SETTINGS` clause is applied on top of the collection values
    /// without passing through that check, so the permission is enforced here for both spellings. It is
    /// enforced when loading from metadata as well, for the same reason it is enforced there for the
    /// engine-argument spelling: the lock is a policy on a named collection that is still in use, and the
    /// alternative is to authenticate with credentials the operator forbade. This must be based on key
    /// existence, not the value: `tryGetNamedCollectionWithOverrides` also refuses a new key when
    /// `allow_named_collection_override_by_default` is disabled. When the collection already stores
    /// `nats_credentials`, this is a same-key override and uses `allow_named_collection_override_by_default`.
    /// Replacing `nats_credential_file` uses `true`: passing the contents is the only way to supply these
    /// credentials from SQL, so the operator states the permission with the attribute.
    if (named_collection)
    {
        /// This exactly mirrors `findOverrideForbiddingKey`: inline credentials replace a configured
        /// file path, but otherwise they are either a same-key override or a new key. The collection
        /// has already been mutated by the engine-argument override, so use its pre-override state.
        const auto is_defined_in_collection = [&](const std::string & key)
        {
            return named_collection->isQueryOverridden(key) ? named_collection->getValueBeforeQueryOverride(key).has_value()
                                                            : named_collection->has(key);
        };
        const auto check_override_allowed = [&](const char * key, bool default_value)
        {
            if (!named_collection->isOverridable(key, default_value))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Override not allowed for '{}'", key);
        };

        if (credentials_assigned_by_query)
        {
            const auto * key = is_defined_in_collection("nats_credentials") || !is_defined_in_collection("nats_credential_file")
                ? "nats_credentials"
                : "nats_credential_file";
            check_override_allowed(key, std::string_view{key} == "nats_credential_file" || allow_named_collection_override_by_default);
        }

        /// `nats_username`, `nats_password`, and `nats_token` do not have an alternative spelling,
        /// so their query overrides follow the regular named-collection policy. In particular, this
        /// prevents a `SETTINGS` clause from clearing operator-provided credentials and bypassing the
        /// destination-binding check above.
        if (username_assigned_by_query)
            check_override_allowed("nats_username", allow_named_collection_override_by_default);
        if (password_assigned_by_query)
            check_override_allowed("nats_password", allow_named_collection_override_by_default);
        if (token_assigned_by_query)
            check_override_allowed("nats_token", allow_named_collection_override_by_default);
    }

    /// A path to a credentials file is only accepted from the server configuration file: the server opens
    /// the file with its own privileges, and during authentication the credentials are sent to `nats_url`,
    /// which comes from the same query. So taking a path from SQL would let anyone who can define a `NATS`
    /// source probe the local filesystem and exfiltrate files the server can read to a NATS server they
    /// control. Loading previously-validated metadata without a named collection, or with a collection
    /// defined in the server configuration, is exempt so those tables keep working after an upgrade.
    /// SQL named collections remain mutable, so their current contents must be validated on every reload.
    if (!loading_from_existing_metadata)
    {
        /// Checked on provenance, before the resulting value below: an override of a configured path with
        /// the empty string carries no path of its own, so it would pass a check on the value while
        /// silently dropping the credentials the operator configured.
        if (credential_file_assigned_by_query
            && (!nats_settings[NATSSetting::nats_credential_file].value.empty() || !credential_file_in_collection.empty()))
        {
            /// Outside the server configuration file the path key is not accepted at all, overridden or
            /// not, so the override rejection would name the wrong remedy.
            if (!collection_defined_in_config)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}", CREDENTIAL_FILE_ONLY_FROM_CONFIG_MESSAGE);

            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`nats_credential_file` cannot be overridden in a query. "
                "Pass the contents of the file in `nats_credentials` instead");
        }

        /// An empty `nats_credentials` never replaces the credentials the collection carries with other
        /// ones - it can only silently drop them, whichever form they are stored in. When the collection
        /// carries no credentials at all there is nothing to drop, and the empty assignment stays the
        /// no-op it is for a table which uses no named collection.
        if (credentials_assigned_by_query && !credentials_set
            && (!credential_file_in_collection.empty() || !credentials_in_collection.empty()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The credentials of the named collection cannot be dropped by an empty `nats_credentials`");
    }

    if (credential_file_from_query && credentials_from_collection)
        nats_settings[NATSSetting::nats_credentials] = String{};
    else if (credentials_from_query && credential_file_from_collection)
        nats_settings[NATSSetting::nats_credential_file] = String{};

    /// Whatever path is left is the one the collection defines, and it is accepted only when the
    /// collection itself comes from the server configuration file.
    if (!loading_from_existing_metadata && !nats_settings[NATSSetting::nats_credential_file].value.empty() && !collection_defined_in_config)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}", CREDENTIAL_FILE_ONLY_FROM_CONFIG_MESSAGE);

    const bool authentication_in_collection = !credential_file_in_collection.empty() || !credentials_in_collection.empty()
        || !username_in_collection.empty() || !password_in_collection.empty() || !token_in_collection.empty();
    const bool authentication_assigned_by_query = credential_file_assigned_by_query || credentials_assigned_by_query
        || username_assigned_by_query || password_assigned_by_query || token_assigned_by_query;

    return authentication_in_collection || authentication_assigned_by_query;
}

}

void registerStorageNATS(StorageFactory & factory);
void registerStorageNATS(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        auto nats_settings = std::make_unique<NATSSettings>();
        /// Whether the query assigned a credential source, in either spelling.
        bool credential_file_assigned_by_query = false;
        bool credentials_assigned_by_query = false;
        bool username_assigned_by_query = false;
        bool password_assigned_by_query = false;
        bool token_assigned_by_query = false;
        bool destination_assigned_by_query = false;
        /// Whether the named collection is defined in the server configuration file rather than created by SQL.
        bool collection_defined_in_config = false;
        auto named_collection = tryGetNamedCollectionWithOverrides(args.engine_args, args.getLocalContext(), true, nullptr, &args.table_id);
        if (named_collection)
        {
            nats_settings->loadFromNamedCollection(named_collection);

            credential_file_assigned_by_query = named_collection->isQueryOverridden("nats_credential_file");
            credentials_assigned_by_query = named_collection->isQueryOverridden("nats_credentials");
            username_assigned_by_query = named_collection->isQueryOverridden("nats_username");
            password_assigned_by_query = named_collection->isQueryOverridden("nats_password");
            token_assigned_by_query = named_collection->isQueryOverridden("nats_token");
            destination_assigned_by_query
                = named_collection->isQueryOverridden("nats_url") || named_collection->isQueryOverridden("nats_server_list");
            collection_defined_in_config = named_collection->getSourceId() == NamedCollection::SourceId::CONFIG;
        }
        else if (!args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NATS engine must have settings");

        nats_settings->loadFromQuery(*args.storage_def);

        /// A credential source assigned in the `SETTINGS` clause is query-level even when the named
        /// collection provides the same key: the clause is applied on top of the collection values,
        /// so the final value no longer comes from the collection.
        if (args.storage_def->settings)
        {
            for (const auto & change : args.storage_def->settings->changes)
            {
                if (change.name == "nats_credential_file")
                    credential_file_assigned_by_query = true;
                else if (change.name == "nats_credentials")
                    credentials_assigned_by_query = true;
                else if (change.name == "nats_username")
                    username_assigned_by_query = true;
                else if (change.name == "nats_password")
                    password_assigned_by_query = true;
                else if (change.name == "nats_token")
                    token_assigned_by_query = true;
                else if (change.name == "nats_url" || change.name == "nats_server_list")
                    destination_assigned_by_query = true;
            }
        }

        if (!(*nats_settings)[NATSSetting::nats_url].changed && !(*nats_settings)[NATSSetting::nats_server_list].changed)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify either `nats_url` or `nats_server_list` settings");

        if (!(*nats_settings)[NATSSetting::nats_format].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `nats_format` setting");

        if (!(*nats_settings)[NATSSetting::nats_subjects].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `nats_subjects` setting");

        /// Credential validation makes decisions from the effective settings, so it must see the
        /// same macro-expanded values that `StorageNATS` passes to `libnats`. In particular, an
        /// empty macro must not turn a non-empty query credential into an empty replacement after
        /// the check that prevents dropping credentials from a named collection.
        auto macros = args.getContext()->getMacros();
        (*nats_settings)[NATSSetting::nats_username] = macros->expand((*nats_settings)[NATSSetting::nats_username]);
        (*nats_settings)[NATSSetting::nats_password] = macros->expand((*nats_settings)[NATSSetting::nats_password]);
        (*nats_settings)[NATSSetting::nats_token] = macros->expand((*nats_settings)[NATSSetting::nats_token]);
        (*nats_settings)[NATSSetting::nats_credential_file] = macros->expand((*nats_settings)[NATSSetting::nats_credential_file]);
        (*nats_settings)[NATSSetting::nats_credentials] = macros->expand((*nats_settings)[NATSSetting::nats_credentials]);

        const bool authentication_determined_by_table = resolveCredentialSource(
            *nats_settings,
            named_collection.get(),
            collection_defined_in_config,
            credential_file_assigned_by_query,
            credentials_assigned_by_query,
            username_assigned_by_query,
            password_assigned_by_query,
            token_assigned_by_query,
            destination_assigned_by_query,
            args.getLocalContext()->getSettingsRef()[Setting::allow_named_collection_override_by_default],
            (isLoadingFromExistingMetadata(args.mode) || args.query.attach_short_syntax)
                && (!named_collection || collection_defined_in_config));

        if ((*nats_settings)[NATSSetting::nats_consumer_name].changed && !(*nats_settings)[NATSSetting::nats_stream].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "To use NATS jet stream, you must specify `nats_stream` setting");

        return std::make_shared<StorageNATS>(
            args.table_id,
            args.getContext(),
            args.columns,
            args.comment,
            std::move(nats_settings),
            args.mode,
            authentication_determined_by_table);
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
    [nats_credentials = '-----BEGIN NATS USER JWT----- ...',]
    [nats_startup_connect_tries = 5,]
    [nats_max_rows_per_message = 1,]
    [nats_commit_on_select = false,]
    [nats_handle_error_mode = 'default']
```

Required parameters:

- `nats_url` – host:port (for example, `localhost:4222`)..
- `nats_subjects` – List of subject for NATS table to subscribe/publish to. Supports wildcard subjects like `foo.*.bar` or `baz.>`
- `nats_format` – Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](/reference/formats/index) section.

Optional parameters:

- `nats_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/) requires the path to the schema file and the name of the root `schema.capnp:Message` object.
- `nats_stream` – The name of an existing stream in NATS JetStream.
- `nats_consumer_name` – The name of an existing durable pull consumer in NATS JetStream.
- `nats_num_consumers` – The number of consumers per table. Default: `1`. Specify more consumers if the throughput of one consumer is insufficient for NATS core only.
- `nats_queue_group` – Name for queue group of NATS subscribers. Default is the table name.
- `nats_max_reconnect` – Deprecated and has no effect, reconnect is performed permanently with nats_reconnect_wait timeout.
- `nats_reconnect_wait` – Amount of time in milliseconds to sleep between each reconnect attempt. Default: `2000`.
- `nats_server_list` - Server list for connection. Can be specified to connect to NATS cluster.
- `nats_skip_broken_messages` - NATS message parser tolerance to schema-incompatible messages per block. Default: `0`. If `nats_skip_broken_messages = N` then the engine skips *N* NATS messages that cannot be parsed (a message equals a row of data).
- `nats_max_block_size` - Number of row collected by poll(s) for flushing data from NATS. Default: [max_insert_block_size](/reference/settings/session-settings/max-insert#max_insert_block_size).
- `nats_flush_interval_ms` - Timeout for flushing data read from NATS. Default: [stream_flush_interval_ms](/reference/settings/session-settings/stream#stream_flush_interval_ms).
- `nats_wait_for_flush_interval` - If `true`, a background streaming cycle stays open for the whole flush interval (`nats_flush_interval_ms`, or `stream_flush_interval_ms` otherwise) instead of finishing as soon as the consumer queue drains, letting more messages accumulate into a single block at the cost of up to one flush interval of extra ingestion latency. Default: `false` (low-latency drain-and-go behaviour).
- `nats_username` - NATS username. When it is stored in a named collection defined in the server configuration file, the query cannot override the collection's `nats_url` or `nats_server_list`.
- `nats_password` - NATS password. When it is stored in a named collection defined in the server configuration file, the query cannot override the collection's `nats_url` or `nats_server_list`.
- `nats_token` - NATS auth token. When it is stored in a named collection defined in the server configuration file, the query cannot override the collection's `nats_url` or `nats_server_list`.
- `nats_credential_file` - Path to a NATS credentials file. It is accepted only from a named collection defined in the server configuration file whose `nats_url` and `nats_server_list` are not overridden by the query, because the server opens the path with its own privileges. In a query, pass the contents of the file in `nats_credentials` instead.
- `nats_credentials` - NATS credentials content (the same payload as in a `.creds` file with user JWT and seed). Because it is the only spelling a query can use, it replaces a `nats_credential_file` inherited from a named collection instead of conflicting with it - unless the operator locked that path with `<nats_credential_file overridable="false">`. It cannot be assigned the empty string to drop the credentials a named collection carries.
- `nats_startup_connect_tries` - Number of connect tries at startup. Default: `5`.
- `nats_max_rows_per_message` — The maximum number of rows written in one NATS message for row-based formats. (default : `1`).
- `nats_commit_on_select` - Commit messages when query is made. Applies to JetStream only; core NATS has no acknowledgements. Default: `0`.
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

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using [materialized views](/reference/statements/create/view). To do this:

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

NATS engine supports all [formats](/reference/formats/index) supported in ClickHouse.
The number of rows in one NATS message depends on whether the format is row-based or block-based:

- For row-based formats the number of rows in one NATS message can be controlled by setting `nats_max_rows_per_message`.
- For block-based formats we cannot divide block into smaller parts, but the number of rows in one block can be controlled by general setting [max_block_size](/reference/settings/session-settings/max#max_block_size).

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

JetStream tables give at-least-once delivery: a message is acknowledged only after it has been inserted into the dependent materialized views, so a message whose insert fails or is interrupted stays unacknowledged and is redelivered. Core NATS (without JetStream) has no acknowledgement or replay, so it is at-most-once and an interrupted message is lost.

## Data durability {#data-durability}

This section applies to JetStream only. Core NATS has no acknowledgements and is at-most-once, as described above, so it has no window in which an acknowledged message can be lost.

A JetStream table can silently lose already-consumed rows if the OS page cache is discarded before the inserted data is written to disk. After a batch is pushed to the dependent materialized views, the consumer acknowledges those messages, which lets the stream advance past them. The inserted rows, however, are only durable once the target part is fsynced, which does not happen synchronously by default (`fsync_after_insert = 0`). If the page cache is lost after the acknowledgement but before the target part is fsynced, the messages are no longer redelivered, so the rows are lost with no error and `count()` is simply smaller. A plain process kill does not expose this, because the kernel keeps the page cache and eventually writes it back. A loss of the page cache does expose it; examples are a device-level power loss and an unclean host or kernel reset.

For the recommended materialized-view consumption path (the acknowledgement is sent only after the whole insert pipeline finishes), setting `fsync_after_insert = 1` (and `fsync_part_directory = 1`) on the target `MergeTree` tables makes the inserted parts durable before the acknowledgement is sent, which narrows this window substantially. The setting must be enabled on every `MergeTree` table the batch is inserted into, including cascaded materialized-view targets; any such table left at the default can still lose its part. Asynchronous intermediaries do not gain durability from this setting alone: for example a `Distributed` target inserts in the background when `distributed_foreground_insert = 0`, which is the default outside ClickHouse Cloud, so it needs its own durability settings or synchronous insertion. This mitigation also does not apply to a direct `INSERT ... SELECT ... FROM <nats_table>` with `nats_commit_on_select = 1`, where messages are acknowledged when the read reaches its end rather than after the destination has written a durable part.
)DOCS_MD",
            .syntax = "ENGINE = NATS() SETTINGS nats_url = 'host:port', nats_subjects = 'subject', nats_format = 'format', ...",
            .related = {"Kafka", "RabbitMQ", "FileLog"}});
}

}
