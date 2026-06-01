#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MessageQueueSink.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Storages/RabbitMQ/RabbitMQProducer.h>
#include <Storages/RabbitMQ/RabbitMQSettings.h>
#include <Storages/RabbitMQ/RabbitMQSource.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StreamingStorageRegistry.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/RemoteHostFilter.h>
#include <Common/logger_useful.h>
#include <Common/parseAddress.h>

#include <base/range.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace Setting
{
    extern const SettingsNonZeroUInt64 max_insert_block_size;
    extern const SettingsUInt64 output_format_avro_rows_in_file;
    extern const SettingsMilliseconds stream_flush_interval_ms;
    extern const SettingsBool stream_like_engine_allow_direct_select;
}

namespace RabbitMQSetting
{
    extern const RabbitMQSettingsString rabbitmq_address;
    extern const RabbitMQSettingsBool rabbitmq_commit_on_select;
    extern const RabbitMQSettingsUInt64 rabbitmq_empty_queue_backoff_end_ms;
    extern const RabbitMQSettingsUInt64 rabbitmq_empty_queue_backoff_start_ms;
    extern const RabbitMQSettingsUInt64 rabbitmq_empty_queue_backoff_step_ms;
    extern const RabbitMQSettingsString rabbitmq_exchange_name;
    extern const RabbitMQSettingsString rabbitmq_exchange_type;
    extern const RabbitMQSettingsUInt64 rabbitmq_flush_interval_ms;
    extern const RabbitMQSettingsString rabbitmq_format;
    extern const RabbitMQSettingsStreamingHandleErrorMode rabbitmq_handle_error_mode;
    extern const RabbitMQSettingsString rabbitmq_host_port;
    extern const RabbitMQSettingsUInt64 rabbitmq_max_block_size;
    extern const RabbitMQSettingsUInt64 rabbitmq_max_rows_per_message;
    extern const RabbitMQSettingsUInt64 rabbitmq_num_consumers;
    extern const RabbitMQSettingsUInt64 rabbitmq_num_queues;
    extern const RabbitMQSettingsString rabbitmq_password;
    extern const RabbitMQSettingsBool rabbitmq_persistent;
    extern const RabbitMQSettingsString rabbitmq_queue_base;
    extern const RabbitMQSettingsBool rabbitmq_queue_consume;
    extern const RabbitMQSettingsString rabbitmq_queue_settings_list;
    extern const RabbitMQSettingsString rabbitmq_routing_key_list;
    extern const RabbitMQSettingsString rabbitmq_schema;
    extern const RabbitMQSettingsBool rabbitmq_secure;
    extern const RabbitMQSettingsUInt64 rabbitmq_skip_broken_messages;
    extern const RabbitMQSettingsString rabbitmq_username;
    extern const RabbitMQSettingsString rabbitmq_vhost;
    extern const RabbitMQSettingsBool reject_unhandled_messages;
}

static const uint32_t QUEUE_SIZE = 100000;
static const auto MAX_FAILED_READ_ATTEMPTS = 10;
static const auto RESCHEDULE_MS = 500;
static const auto MAX_THREAD_WORK_DURATION_MS = 60000;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_CONNECT_RABBITMQ;
    extern const int CANNOT_BIND_RABBITMQ_EXCHANGE;
    extern const int CANNOT_DECLARE_RABBITMQ_EXCHANGE;
    extern const int CANNOT_REMOVE_RABBITMQ_EXCHANGE;
    extern const int CANNOT_CREATE_RABBITMQ_QUEUE_BINDING;
    extern const int QUERY_NOT_ALLOWED;
}

namespace ExchangeType
{
    /// Note that default here means default by implementation and not by rabbitmq settings
    static const String DEFAULT = "default";
    static const String FANOUT = "fanout";
    static const String DIRECT = "direct";
    static const String TOPIC = "topic";
    static const String HASH = "consistent_hash";
    static const String HEADERS = "headers";
}

static const auto deadletter_exchange_setting = "x-dead-letter-exchange";

StorageRabbitMQ::StorageRabbitMQ(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & comment,
        std::unique_ptr<RabbitMQSettings> rabbitmq_settings_,
        LoadingStrictnessLevel mode)
        : IStorage(table_id_)
        , WithContext(context_->getGlobalContext())
        , rabbitmq_settings(std::move(rabbitmq_settings_))
        , exchange_name(getContext()->getMacros()->expand((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_exchange_name]))
        , format_name(getContext()->getMacros()->expand((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_format]))
        , exchange_type(defineExchangeType(getContext()->getMacros()->expand((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_exchange_type])))
        , routing_keys(parseSettings(getContext()->getMacros()->expand((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_routing_key_list])))
        , schema_name(getContext()->getMacros()->expand((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_schema]))
        , num_consumers((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_num_consumers].value)
        , num_queues((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_num_queues].value)
        , queue_base(getContext()->getMacros()->expand((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_queue_base]))
        , queue_settings_list(parseSettings(getContext()->getMacros()->expand((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_queue_settings_list])))
        , max_rows_per_message((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_max_rows_per_message])
        , log(getLogger("StorageRabbitMQ (" + table_id_.getFullTableName() + ")"))
        , persistent((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_persistent].value)
        , use_user_setup((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_queue_consume].value)
        , hash_exchange(num_consumers > 1 || num_queues > 1)
        , semaphore(0, static_cast<int>(num_consumers))
        , unique_strbase(getRandomName())
        , queue_size(std::max(QUEUE_SIZE, static_cast<uint32_t>(getMaxBlockSize())))
        , milliseconds_to_wait((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_empty_queue_backoff_start_ms])
{
    reject_unhandled_messages = (*rabbitmq_settings)[RabbitMQSetting::reject_unhandled_messages]
        || queue_settings_list.end() !=
        std::find_if(queue_settings_list.begin(), queue_settings_list.end(),
                     [](const String & name) { return name.starts_with(deadletter_exchange_setting); });

    const auto & config = getContext()->getConfigRef();

    std::pair<String, UInt16> parsed_address;
    auto setting_rabbitmq_username = (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_username].value;
    auto setting_rabbitmq_password = (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_password].value;
    String username;
    String password;

    if ((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_host_port].changed)
    {
        username = setting_rabbitmq_username.empty() ? config.getString("rabbitmq.username", "") : setting_rabbitmq_username;
        password = setting_rabbitmq_password.empty() ? config.getString("rabbitmq.password", "") : setting_rabbitmq_password;
        if (username.empty() || password.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "No username or password. They can be specified either in config or in storage settings");

        parsed_address = parseAddress(getContext()->getMacros()->expand((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_host_port]), 5672);
        if (parsed_address.first.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Host or port is incorrect (host: {}, port: {})", parsed_address.first, parsed_address.second);

        context_->getRemoteHostFilter().checkHostAndPort(parsed_address.first, toString(parsed_address.second));
    }
    else if (!(*rabbitmq_settings)[RabbitMQSetting::rabbitmq_address].changed)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "RabbitMQ requires either `rabbitmq_host_port` or `rabbitmq_address` setting");

    configuration =
    {
        .host = parsed_address.first,
        .port = parsed_address.second,
        .username = username,
        .password = password,
        .vhost = config.getString("rabbitmq.vhost", getContext()->getMacros()->expand((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_vhost])),
        .secure = (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_secure].value,
        .connection_string = getContext()->getMacros()->expand((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_address])
    };

    if (configuration.secure)
        SSL_library_init();

    if (!columns_.getMaterialized().empty() || !columns_.getAliases().empty() || !columns_.getDefaults().empty() || !columns_.getEphemeral().empty())
    {
        context_->addOrUpdateWarningMessage(
            Context::WarningType::RABBITMQ_UNSUPPORTED_COLUMNS,
            PreformattedMessage::create("RabbitMQ table engine doesn't support ALIAS, DEFAULT or MATERIALIZED columns. They will be ignored and filled with default values"));
    }
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_handle_error_mode]));
    setInMemoryMetadata(storage_metadata);

    rabbitmq_context = addSettings(getContext());
    rabbitmq_context->makeQueryContext();

    if (queue_base.empty())
    {
        /* Make sure that local exchange name is unique for each table and is not the same as client's exchange name. It also needs to
         * be table-based and not just a random string, because local exchanges should be declared the same for same tables
         */
        sharding_exchange = getTableBasedName(exchange_name, table_id_);

        /* By default without a specified queue name in queue's declaration - its name will be generated by the library, but its better
         * to specify it unique for each table to reuse them once the table is recreated. So it means that queues remain the same for every
         * table unless queue_base table setting is specified (which allows to register consumers to specific queues). Now this is a base
         * for the names of later declared queues
         */
        queue_base = getTableBasedName("", table_id_);
    }
    else
    {
        /* In case different tables are used to register multiple consumers to the same queues (so queues are shared between tables) and
         * at the same time sharding exchange is needed (if there are multiple shared queues), then those tables also need to share
         * sharding exchange and bridge exchange
         */
        sharding_exchange = exchange_name + "_" + queue_base;
    }

    bridge_exchange = sharding_exchange + "_bridge";

    try
    {
        connection = std::make_unique<RabbitMQConnection>(configuration, log);
        if (connection->connect())
            initRabbitMQ();
        else if (mode <= LoadingStrictnessLevel::CREATE)
            throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot connect to {}", connection->connectionInfoForLog());
    }
    catch (...)
    {
        tryLogCurrentException(log);
        if (mode <= LoadingStrictnessLevel::CREATE)
            throw;
    }

    /// One looping task for all consumers as they share the same connection == the same handler == the same event loop
    looping_task = getContext()->getMessageBrokerSchedulePool().createTask(getStorageID(), "RabbitMQLoopingTask", [this]{ loopingFunc(); });
    looping_task->deactivate();

    streaming_task = getContext()->getMessageBrokerSchedulePool().createTask(getStorageID(), "RabbitMQStreamingTask", [this]{ streamingToViewsFunc(); });
    streaming_task->deactivate();

    init_task = getContext()->getMessageBrokerSchedulePool().createTask(getStorageID(), "RabbitMQConnectionTask", [this]{ connectionFunc(); });
    init_task->deactivate();
}

StorageRabbitMQ::~StorageRabbitMQ() = default;

VirtualColumnsDescription StorageRabbitMQ::createVirtuals(StreamingHandleErrorMode handle_error_mode)
{
    VirtualColumnsDescription desc;

    desc.addEphemeral("_exchange_name", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_channel_id", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_delivery_tag", std::make_shared<DataTypeUInt64>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_redelivered", std::make_shared<DataTypeUInt8>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_message_id", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_timestamp", std::make_shared<DataTypeUInt64>(), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);

    if (handle_error_mode == StreamingHandleErrorMode::STREAM)
    {
        desc.addEphemeral("_raw_message", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
        desc.addEphemeral("_error", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    }

    return desc;
}

Names StorageRabbitMQ::parseSettings(String settings_list)
{
    Names result;
    if (settings_list.empty())
        return result;
    boost::split(result, settings_list, [](char c){ return c == ','; });
    for (String & key : result)
        boost::trim(key);

    return result;
}


AMQP::ExchangeType StorageRabbitMQ::defineExchangeType(String exchange_type_)
{
    AMQP::ExchangeType type;
    if (exchange_type_ != ExchangeType::DEFAULT)
    {
        if (exchange_type_ == ExchangeType::FANOUT)              type = AMQP::ExchangeType::fanout;
        else if (exchange_type_ == ExchangeType::DIRECT)         type = AMQP::ExchangeType::direct;
        else if (exchange_type_ == ExchangeType::TOPIC)          type = AMQP::ExchangeType::topic;
        else if (exchange_type_ == ExchangeType::HASH)           type = AMQP::ExchangeType::consistent_hash;
        else if (exchange_type_ == ExchangeType::HEADERS)        type = AMQP::ExchangeType::headers;
        else throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid exchange type");
    }
    else
    {
        type = AMQP::ExchangeType::fanout;
    }

    return type;
}


String StorageRabbitMQ::getTableBasedName(String name, const StorageID & table_id)
{
    if (name.empty())
        return fmt::format("{}_{}", table_id.database_name, table_id.table_name);
    return fmt::format("{}_{}_{}", name, table_id.database_name, table_id.table_name);
}


ContextMutablePtr StorageRabbitMQ::addSettings(ContextPtr local_context) const
{
    auto modified_context = Context::createCopy(local_context);
    modified_context->setSetting("input_format_skip_unknown_fields", true);
    modified_context->setSetting("input_format_allow_errors_ratio", 0.);
    if ((*rabbitmq_settings)[RabbitMQSetting::rabbitmq_handle_error_mode] == StreamingHandleErrorMode::DEFAULT)
        modified_context->setSetting("input_format_allow_errors_num", (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_skip_broken_messages].value);
    else
        modified_context->setSetting("input_format_allow_errors_num", Field(0));

    /// Since we are reusing the same context for all queries executed simultaneously, we don't want to used shared `analyze_count`
    modified_context->setSetting("max_analyze_depth", Field{0});

    if (!schema_name.empty())
        modified_context->setSetting("format_schema", schema_name);

    /// check for non-rabbitmq-related settings
    modified_context->applySettingsChanges(rabbitmq_settings->getFormatSettings());

    /// It does not make sense to use auto detection here, since the format
    /// will be reset for each message, plus, auto detection takes CPU
    /// time.
    modified_context->setSetting("input_format_csv_detect_header", false);
    modified_context->setSetting("input_format_tsv_detect_header", false);
    modified_context->setSetting("input_format_custom_detect_header", false);

    return modified_context;
}


void StorageRabbitMQ::loopingFunc()
{
    connection->getHandler().startLoop();
}


void StorageRabbitMQ::stopLoop()
{
    connection->getHandler().stopLoop();
}

void StorageRabbitMQ::stopLoopIfNoReaders()
{
    /// Stop the loop if no select was started.
    /// There can be a case that selects are finished
    /// but not all sources decremented the counter, then
    /// it is ok that the loop is not stopped, because
    /// there is a background task (streaming_task), which
    /// also checks whether there is an idle loop.
    std::lock_guard lock(loop_mutex);
    if (readers_count)
        return;
    connection->getHandler().stopLoop();
}

void StorageRabbitMQ::startLoop()
{
    chassert(initialized);
    connection->getHandler().updateLoopState(Loop::RUN);
    looping_task->activateAndSchedule();
}


void StorageRabbitMQ::incrementReader()
{
    ++readers_count;
}


void StorageRabbitMQ::decrementReader()
{
    --readers_count;
}


void StorageRabbitMQ::connectionFunc()
{
    if (initialized)
        return;

    try
    {
        if (connection->reconnect())
        {
            initRabbitMQ();
            streaming_task->scheduleAfter(RESCHEDULE_MS);
            return;
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    init_task->scheduleAfter(RESCHEDULE_MS);
}


/* Need to deactivate this way because otherwise might get a deadlock when first deactivate streaming task in shutdown and then
 * inside streaming task try to deactivate any other task
 */
void StorageRabbitMQ::deactivateTask(BackgroundSchedulePool::TaskHolder & task, bool wait, bool stop_loop)
{
    if (stop_loop)
        stopLoop();

    std::unique_lock<std::mutex> lock(task_mutex, std::defer_lock);
    if (lock.try_lock())
    {
        task->deactivate();
        lock.unlock();
    }
    else if (wait) /// Wait only if deactivating from shutdown
    {
        lock.lock();
        task->deactivate();
    }
}


size_t StorageRabbitMQ::getMaxBlockSize() const
{
    return (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_max_block_size].changed
        ? (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_max_block_size].value
        : (getContext()->getSettingsRef()[Setting::max_insert_block_size].value / num_consumers);
}


void StorageRabbitMQ::initRabbitMQ()
{
    if (shutdown_called || initialized)
        return;

    if (use_user_setup)
    {
        queues.emplace_back(queue_base);
    }
    else
    {
        auto rabbit_channel = connection->createChannel();

        /// Main exchange -> Bridge exchange -> ( Sharding exchange ) -> Queues -> Consumers

        bindExchange(*rabbit_channel);
        for (const auto i : collections::range(0, num_queues))
            bindQueue(i + 1, *rabbit_channel);

        if (queues.size() != num_queues)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Expected all queues to be initialized (but having {}/{})",
                queues.size(), num_queues);
        }

        LOG_TRACE(log, "RabbitMQ setup completed");
        rabbit_channel->close();
    }

    LOG_TRACE(log, "Registering {} conumers", num_consumers);

    for (size_t i = 0; i < num_consumers; ++i)
    {
        auto consumer = createConsumer();
        consumer->updateChannel(*connection);
        consumers_ref.push_back(consumer);
        pushConsumer(consumer);
        ++num_created_consumers;
    }

    LOG_TRACE(log, "Registered {}/{} conumers", num_created_consumers, num_consumers);
    initialized = true;
}


void StorageRabbitMQ::bindExchange(AMQP::TcpChannel & rabbit_channel)
{
    /// Exchange hierarchy:
    /// 1. Main exchange (defined with table settings - rabbitmq_exchange_name, rabbitmq_exchange_type).
    /// 2. Bridge exchange (fanout). Used to easily disconnect main exchange and to simplify queue bindings.
    /// 3. Sharding (or hash) exchange. Used in case of multiple queues.
    /// 4. Consumer exchange. Just an alias for bridge_exchange or sharding exchange to know to what exchange
    ///    queues will be bound.

    /// All exchanges are declared with options:
    /// 1. `durable` (survive RabbitMQ server restart)
    /// 2. `autodelete` (auto delete in case of queue bindings are dropped).

    std::string error;
    int error_code;
    rabbit_channel.declareExchange(exchange_name, exchange_type, AMQP::durable)
    .onError([&](const char * message)
    {
        connection->getHandler().stopBlockingLoop();
        /// This error can be a result of attempt to declare exchange if it was already declared but
        /// 1) with different exchange type.
        /// 2) with different exchange settings.
        error = "Unable to declare exchange. "
            "Make sure specified exchange is not already declared. Error: " + std::string(message);
        error_code = ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE;
    });

    rabbit_channel.declareExchange(bridge_exchange, AMQP::fanout, AMQP::durable | AMQP::autodelete)
    .onError([&](const char * message)
    {
        connection->getHandler().stopBlockingLoop();
        /// This error is not supposed to happen as this exchange name is always unique to type and its settings.
        if (error.empty())
        {
            error = fmt::format("Unable to declare bridge exchange ({}). Reason: {}",
                                bridge_exchange, std::string(message));
            error_code = ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE;
        }
    });

    if (hash_exchange)
    {
        AMQP::Table binding_arguments;

        /// Default routing key property in case of hash exchange is a routing key, which is required to be an integer.
        /// Support for arbitrary exchange type (i.e. arbitrary pattern of routing keys) requires to eliminate this dependency.
        /// This settings changes hash property to message_id.
        binding_arguments["hash-property"] = "message_id";

        /// Declare hash exchange for sharding.
        rabbit_channel.declareExchange(sharding_exchange, AMQP::consistent_hash, AMQP::durable | AMQP::autodelete, binding_arguments)
        .onError([&](const char * message)
        {
            connection->getHandler().stopBlockingLoop();
            /// This error can be a result of same reasons as above for exchange_name, i.e. it will mean that sharding exchange name appeared
            /// to be the same as some other exchange (which purpose is not for sharding). So probably actual error reason: queue_base parameter
            /// is bad.
            if (error.empty())
            {
                error = fmt::format("Unable to declare sharding exchange ({}). Reason: {}",
                                    sharding_exchange, std::string(message));
                error_code = ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE;
            }
        });

        rabbit_channel.bindExchange(bridge_exchange, sharding_exchange, routing_keys[0])
        .onError([&](const char * message)
        {
            connection->getHandler().stopBlockingLoop();
            if (error.empty())
            {
                error = fmt::format(
                    "Unable to bind bridge exchange ({}) to sharding exchange ({}). Reason: {}",
                    bridge_exchange, sharding_exchange, std::string(message));
                error_code = ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE;
            }
        });

        consumer_exchange = sharding_exchange;
    }
    else
    {
        consumer_exchange = bridge_exchange;
    }

    size_t bound_keys = 0;

    if (exchange_type == AMQP::ExchangeType::headers)
    {
        AMQP::Table bind_headers;
        for (const auto & header : routing_keys)
        {
            std::vector<String> matching;
            boost::split(matching, header, [](char c){ return c == '='; });
            bind_headers[matching[0]] = matching[1];
        }

        rabbit_channel.bindExchange(exchange_name, bridge_exchange, routing_keys[0], bind_headers)
        .onSuccess([&]() { connection->getHandler().stopBlockingLoop(); })
        .onError([&](const char * message)
        {
            connection->getHandler().stopBlockingLoop();
            error = fmt::format("Unable to bind exchange {} to bridge exchange ({}). Reason: {}",
                                exchange_name, bridge_exchange, std::string(message));
            error_code = ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE;
        });
    }
    else if (exchange_type == AMQP::ExchangeType::fanout || exchange_type == AMQP::ExchangeType::consistent_hash)
    {
        rabbit_channel.bindExchange(exchange_name, bridge_exchange, routing_keys[0])
        .onSuccess([&]() { connection->getHandler().stopBlockingLoop(); })
        .onError([&](const char * message)
        {
            connection->getHandler().stopBlockingLoop();
            if (error.empty())
            {
                error = fmt::format("Unable to bind exchange {} to bridge exchange ({}). Reason: {}",
                                    exchange_name, bridge_exchange, std::string(message));
                error_code = ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE;
            }
        });
    }
    else
    {
        for (const auto & routing_key : routing_keys)
        {
            rabbit_channel.bindExchange(exchange_name, bridge_exchange, routing_key)
            .onSuccess([&]()
            {
                ++bound_keys;
                if (bound_keys == routing_keys.size())
                    connection->getHandler().stopBlockingLoop();
            })
            .onError([&](const char * message)
            {
                connection->getHandler().stopBlockingLoop();
                if (error.empty())
                {
                    error = fmt::format("Unable to bind exchange {} to bridge exchange ({}). Reason: {}",
                                        exchange_name, bridge_exchange, std::string(message));
                    error_code = ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE;
                }
            });
        }
    }

    connection->getHandler().startBlockingLoop();
    if (!error.empty())
        throw Exception(error_code, "{}", error);
}


void StorageRabbitMQ::bindQueue(size_t queue_id, AMQP::TcpChannel & rabbit_channel)
{
    std::string error;
    auto success_callback = [&](const std::string &  queue_name, int msgcount, int /* consumercount */)
    {
        queues.emplace_back(queue_name);
        LOG_DEBUG(log, "Queue {} is declared", queue_name);

        if (msgcount)
            LOG_INFO(log, "Queue {} is non-empty. Non-consumed messaged will also be delivered", queue_name);

       /* Here we bind either to sharding exchange (consistent-hash) or to bridge exchange (fanout). All bindings to routing keys are
        * done between client's exchange and local bridge exchange. Binding key must be a string integer in case of hash exchange, for
        * fanout exchange it can be arbitrary
        */
        rabbit_channel.bindQueue(consumer_exchange, queue_name, std::to_string(queue_id))
        .onSuccess([&] { connection->getHandler().stopBlockingLoop(); })
        .onError([&](const char * message)
        {
            connection->getHandler().stopBlockingLoop();
            error = fmt::format("Failed to create queue binding for exchange {}. Reason: {}",
                                exchange_name, std::string(message));
        });
    };

    auto error_callback([&](const char * message)
    {
        connection->getHandler().stopBlockingLoop();
        /* This error is most likely a result of an attempt to declare queue with different settings if it was declared before. So for a
         * given queue name either deadletter_exchange parameter changed or queue_size changed, i.e. table was declared with different
         * max_block_size parameter. Solution: client should specify a different queue_base parameter or manually delete previously
         * declared queues via any of the various cli tools.
         */
         if (error.empty())
             error = fmt::format(
                 "Failed to declare queue. Probably queue settings are conflicting: "
                 "max_block_size, deadletter_exchange. Attempt specifying differently those settings "
                 "or use a different queue_base or manually delete previously declared queues, "
                 "which  were declared with the same names. ERROR reason: {}", std::string(message));
    });

    AMQP::Table queue_settings;

    std::unordered_set<String> integer_settings = {"x-max-length", "x-max-length-bytes", "x-message-ttl", "x-expires", "x-priority", "x-max-priority"};
    std::unordered_set<String> string_settings = {"x-overflow", "x-dead-letter-exchange", "x-queue-type"};

    /// Check user-defined settings.
    if (!queue_settings_list.empty())
    {
        for (const auto & setting : queue_settings_list)
        {
            Strings setting_values;
            splitInto<'='>(setting_values, setting);
            if (setting_values.size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid settings string: {}", setting);

            String key = setting_values[0];
            String value = setting_values[1];

            if (integer_settings.contains(key))
                queue_settings[key] = parse<uint64_t>(value);
            else if (string_settings.contains(key))
                queue_settings[key] = value;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported queue setting: {}", value);
        }
    }

    /// If queue_base - a single name, then it can be used as one specific queue, from which to read.
    /// Otherwise it is used as a generator (unique for current table) of queue names, because it allows to
    /// maximize performance - via setting `rabbitmq_num_queues`.
    const String queue_name = !hash_exchange ? queue_base : std::to_string(queue_id) + "_" + queue_base;

    /// AMQP::autodelete setting is not allowed, because in case of server restart there will be no consumers
    /// and deleting queues should not take place.
    rabbit_channel.declareQueue(queue_name, AMQP::durable, queue_settings).onSuccess(success_callback).onError(error_callback);
    connection->getHandler().startBlockingLoop();
    if (!error.empty())
        throw Exception(ErrorCodes::CANNOT_CREATE_RABBITMQ_QUEUE_BINDING, "{}", error);
}


void StorageRabbitMQ::unbindExchange()
{
    /* This is needed because with RabbitMQ (without special adjustments) can't, for example, properly make mv if there was insert query
     * on the same table before, and in another direction it will make redundant copies, but most likely nobody will do that.
     * As publishing is done to exchange, publisher never knows to which queues the message will go, every application interested in
     * consuming from certain exchange - declares its owns exchange-bound queues, messages go to all such exchange-bound queues, and as
     * input streams are always created at startup, then they will also declare its own exchange bound queues, but they will not be visible
     * externally - client declares its own exchange-bound queues, from which to consume, so this means that if not disconnecting this local
     * queues, then messages will go both ways and in one of them they will remain not consumed. So need to disconnect local exchange
     * bindings to remove redunadant message copies, but after that mv cannot work unless those bindings are recreated. Recreating them is
     * not difficult but very ugly and as probably nobody will do such thing - bindings will not be recreated.
     */
    if (!exchange_removed.exchange(true))
    {
        try
        {
            streaming_task->deactivate();

            stopLoop();
            looping_task->deactivate();
            std::string error;

            auto rabbit_channel = connection->createChannel();
            rabbit_channel->removeExchange(bridge_exchange)
            .onSuccess([&]()
            {
                connection->getHandler().stopBlockingLoop();
            })
            .onError([&](const char * message)
            {
                connection->getHandler().stopBlockingLoop();
                error = fmt::format("Unable to remove exchange. Reason: {}", std::string(message));
            });

            connection->getHandler().startBlockingLoop();
            rabbit_channel->close();
            if (!error.empty())
                throw Exception(ErrorCodes::CANNOT_REMOVE_RABBITMQ_EXCHANGE, "{}", error);
        }
        catch (...)
        {
            exchange_removed = false;
            throw;
        }
    }
}


void StorageRabbitMQ::read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        size_t /* num_streams */)
{
    if (!initialized)
        throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "RabbitMQ setup not finished. Connection might be lost");

    if (num_created_consumers == 0)
    {
        auto header = storage_snapshot->getSampleBlockForColumns(column_names);
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info);
        return;
    }

    if (!local_context->getSettingsRef()[Setting::stream_like_engine_allow_direct_select])
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED,
                        "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`. Be aware that usually the read data is removed from the queue.");

    if (mv_attached)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageRabbitMQ with attached materialized views");

    std::lock_guard lock(loop_mutex);

    auto sample_block = storage_snapshot->getSampleBlockForColumns(column_names);
    auto modified_context = addSettings(local_context);

    if (!connection->isConnected())
    {
        if (connection->getHandler().loopRunning())
            deactivateTask(looping_task, false, true);
        if (!connection->reconnect())
            throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "No connection to {}", connection->connectionInfoForLog());
    }

    Pipes pipes;
    pipes.reserve(num_created_consumers);

    uint64_t max_execution_time_ms = (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_flush_interval_ms].changed
        ? (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_flush_interval_ms]
        : static_cast<UInt64>(getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds());

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto rabbit_source = std::make_shared<RabbitMQSource>(
            *this, storage_snapshot, modified_context, column_names, /* max_block_size */1,
            max_execution_time_ms, (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_handle_error_mode], reject_unhandled_messages,
            /* ack_in_suffix */(*rabbitmq_settings)[RabbitMQSetting::rabbitmq_commit_on_select], log);

        auto converting_dag = ActionsDAG::makeConvertingActions(
            rabbit_source->getPort().getHeader().getColumnsWithTypeAndName(),
            sample_block.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            local_context);

        auto converting = std::make_shared<ExpressionActions>(std::move(converting_dag));
        auto converting_transform = std::make_shared<ExpressionTransform>(rabbit_source->getPort().getSharedHeader(), std::move(converting));

        pipes.emplace_back(std::move(rabbit_source));
        pipes.back().addTransform(std::move(converting_transform));
    }

    if (!connection->getHandler().loopRunning() && connection->isConnected())
        startLoop();

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


SinkToStoragePtr StorageRabbitMQ::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    auto producer = std::make_unique<RabbitMQProducer>(
        configuration, routing_keys, exchange_name, exchange_type, producer_id.fetch_add(1), persistent, shutdown_called, log);
    size_t max_rows = max_rows_per_message;
    /// Need for backward compatibility.
    if (format_name == "Avro" && local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].changed)
        max_rows = local_context->getSettingsRef()[Setting::output_format_avro_rows_in_file].value;
    return std::make_shared<MessageQueueSink>(
        std::make_shared<const Block>(metadata_snapshot->getSampleBlockNonMaterialized()),
        getFormatName(),
        max_rows,
        std::move(producer),
        getName(),
        local_context);
}


void StorageRabbitMQ::startup()
{
    if (initialized)
    {
        streaming_task->activateAndSchedule();
    }
    else
    {
        streaming_task->activate();
        init_task->activateAndSchedule();
    }
    StreamingStorageRegistry::instance().registerTable(getStorageID());
}


void StorageRabbitMQ::shutdown(bool)
{
    /// Needed to make this method idempotent
    if (shutdown_called.exchange(true))
        return;

    LOG_TRACE(log, "Deactivating init task");
    deactivateTask(init_task, true, false);

    for (auto & consumer_weak : consumers_ref)
    {
        auto consumer = consumer_weak.lock();
        if (!consumer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "RabbitMQ consumer has been unexpectedly destroyed");
        consumer->stop();
    }

    /// The order of deactivating tasks is important: wait for streamingToViews() func to finish and
    /// then wait for background event loop to finish.
    LOG_TRACE(log, "Deactivating streaming task");
    deactivateTask(streaming_task, true, false);
    LOG_TRACE(log, "Deactivating looping task");
    deactivateTask(looping_task, true, true);

    LOG_TRACE(log, "Cleaning up RabbitMQ after table usage");

    /// Just a paranoid try catch, it is not actually needed.
    try
    {
        for (auto & consumer_weak : consumers_ref)
        {
            auto consumer = consumer_weak.lock();
            if (!consumer)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "RabbitMQ consumer has been unexpectedly destroyed");
            consumer->closeConnections();
        }

        if (drop_table)
            cleanupRabbitMQ();

        /// It is important to close connection here - before removing consumers, because
        /// it will finish and clean callbacks, which might use those consumers data.
        connection->disconnect();

        for (size_t i = 0; i < num_created_consumers; ++i)
            popConsumer();

        consumers_ref.clear();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    StreamingStorageRegistry::instance().unregisterTable(getStorageID(), /* if_exists */ true);
    LOG_TRACE(log, "Shutdown finished");
}

void StorageRabbitMQ::renameInMemory(const StorageID & new_table_id)
{
    const auto prev_storage_id = getStorageID();
    IStorage::renameInMemory(new_table_id);
    StreamingStorageRegistry::instance().renameTable(prev_storage_id, getStorageID());
}


/// The only thing publishers are supposed to be aware of is _exchanges_ and queues are a responsibility of a consumer.
/// Therefore, if a table is dropped, a clean up is needed.
void StorageRabbitMQ::cleanupRabbitMQ() const
{
    if (use_user_setup)
        return;

    connection->heartbeat();
    if (!connection->isConnected())
    {
        String queue_names;
        for (const auto & queue : queues)
        {
            if (!queue_names.empty())
                queue_names += ", ";
            queue_names += queue;
        }
        LOG_WARNING(log,
                    "RabbitMQ clean up not done, because there is no connection in table's shutdown."
                    "There are {} queues ({}), which might need to be deleted manually. Exchanges will be auto-deleted",
                    queues.size(), queue_names);
        return;
    }

    auto rabbit_channel = connection->createChannel();
    for (const auto & queue : queues)
    {
        /// AMQP::ifunused is needed, because it is possible to share queues between multiple tables and dropping
        /// one of them should not affect others.
        /// AMQP::ifempty is not used on purpose.

        rabbit_channel->removeQueue(queue, AMQP::ifunused)
        .onSuccess([&](uint32_t num_messages)
        {
            LOG_TRACE(log, "Successfully deleted queue {}, messages contained {}", queue, num_messages);
            connection->getHandler().stopBlockingLoop();
        })
        .onError([&](const char * message)
        {
            LOG_ERROR(log, "Failed to delete queue {}. Error message: {}", queue, message);
            connection->getHandler().stopBlockingLoop();
        });
    }
    connection->getHandler().startBlockingLoop();
    rabbit_channel->close();

    /// Also there is no need to cleanup exchanges as they were created with AMQP::autodelete option. Once queues
    /// are removed, exchanges will also be cleaned.
}


void StorageRabbitMQ::pushConsumer(RabbitMQConsumerPtr consumer)
{
    std::lock_guard lock(consumers_mutex);
    consumers.push_back(consumer);
    semaphore.set();
}


RabbitMQConsumerPtr StorageRabbitMQ::popConsumer()
{
    return popConsumer(std::chrono::milliseconds::zero());
}


RabbitMQConsumerPtr StorageRabbitMQ::popConsumer(std::chrono::milliseconds timeout)
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


RabbitMQConsumerPtr StorageRabbitMQ::createConsumer()
{
    return std::make_shared<RabbitMQConsumer>(
        connection->getHandler(), queues, ++consumer_id, unique_strbase, log, queue_size);
}

bool StorageRabbitMQ::hasDependencies(const StorageID & table_id)
{
    return !DatabaseCatalog::instance().getReadyDependentViews(table_id, getContext()).empty();
}

void StorageRabbitMQ::streamingToViewsFunc()
{
    try
    {
        streamToViewsImpl();
    }
    catch (...)
    {
        LOG_ERROR(log, "Error while streaming to views: {}", getCurrentExceptionMessage(true));
    }

    mv_attached.store(false);

    try
    {
        /// If there is no running select, stop the loop which was
        /// activated by previous select.
        if (connection->getHandler().loopRunning())
            stopLoopIfNoReaders();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (shutdown_called)
    {
        LOG_DEBUG(log, "Shutdown called, stopping background streaming process");
    }
    else
    {
        /// Reschedule with backoff.
        if (milliseconds_to_wait < (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_empty_queue_backoff_end_ms])
            milliseconds_to_wait += (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_empty_queue_backoff_step_ms];

        LOG_DEBUG(log, "Rescheduling background streaming process in {}", milliseconds_to_wait);
        streaming_task->scheduleAfter(milliseconds_to_wait);
    }
}

void StorageRabbitMQ::streamToViewsImpl()
{
    if (!initialized)
    {
        chassert(false);
        return;
    }

    auto table_id = getStorageID();

    // Check if at least one direct dependency is attached
    size_t num_views = DatabaseCatalog::instance().getDependentViews(table_id).size();
    bool rabbit_connected = connection->isConnected() || connection->reconnect();

    if (num_views && rabbit_connected)
    {
        auto start_time = std::chrono::steady_clock::now();

        mv_attached.store(true);

        // Keep streaming as long as there are attached views and streaming is not cancelled
        while (!shutdown_called && num_created_consumers > 0)
        {
            if (!hasDependencies(table_id))
                break;

            LOG_DEBUG(log, "Started streaming to {} attached views", num_views);

            bool continue_reading = tryStreamToViews();
            if (!continue_reading)
                break;

            auto end_time = std::chrono::steady_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
            if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
            {
                LOG_TRACE(log, "Reschedule streaming. Thread work duration limit exceeded.");
                break;
            }

            milliseconds_to_wait = (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_empty_queue_backoff_start_ms];
        }
    }
}

bool StorageRabbitMQ::tryStreamToViews()
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(getContext(), false), getContext());
    auto block_size = getMaxBlockSize();

    // Create a stream for each consumer and join them in a union stream
    std::vector<std::shared_ptr<RabbitMQSource>> sources;
    Pipes pipes;
    sources.reserve(num_created_consumers);
    pipes.reserve(num_created_consumers);

    uint64_t max_execution_time_ms = (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_flush_interval_ms].changed
        ? (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_flush_interval_ms]
        : static_cast<UInt64>(getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds());

    auto new_context = Context::createCopy(rabbitmq_context);

    /// Create a fresh query context from rabbitmq_context, discarding any caches attached to the previous context to
    /// ensure no stale state is reused.
    new_context->makeQueryContext();

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto source = std::make_shared<RabbitMQSource>(
            *this, storage_snapshot, new_context, Names{}, block_size,
            max_execution_time_ms, (*rabbitmq_settings)[RabbitMQSetting::rabbitmq_handle_error_mode],
            reject_unhandled_messages, /* ack_in_suffix */false, log);

        sources.emplace_back(source);
        pipes.emplace_back(source);
    }

    // Create an INSERT query for streaming data
    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = table_id;
    if (!sources.empty())
    {
        auto column_list = make_intrusive<ASTExpressionList>();
        const auto & header = sources[0]->getPort().getHeader();
        for (const auto & column : header)
            column_list->children.emplace_back(make_intrusive<ASTIdentifier>(column.name));
        insert->columns = std::move(column_list);
    }

    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(
        insert,
        new_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_isnert */ false);
    auto block_io = interpreter.execute();

    block_io.pipeline.complete(Pipe::unitePipes(std::move(pipes)));

    std::atomic_size_t rows = 0;
    block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });

    if (!connection->getHandler().loopRunning())
        startLoop();

    bool write_failed = false;
    try
    {
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to push to views. Error: {}", getCurrentExceptionMessage(true));
        write_failed = true;
    }

    LOG_TRACE(log, "Processed {} rows", rows.load());

    /* Note: sending ack() with loop running in another thread will lead to a lot of data races inside the library, but only in case
     * error occurs or connection is lost while ack is being sent
     */
    deactivateTask(looping_task, false, true);
    size_t queue_empty = 0;

    if (!connection->isConnected())
    {
        if (shutdown_called)
        {
            LOG_DEBUG(log, "Shutdown called, quitting");
            return false;
        }

        if (connection->reconnect())
        {
            LOG_DEBUG(log, "Connection restored, updating channels");
            for (auto & source : sources)
                source->updateChannel();
        }
        else
        {
            LOG_TRACE(log, "Reschedule streaming. Unable to restore connection.");
            return false;
        }
    }
    else
    {
        LOG_TEST(log, "Will {} messages for {} channels", write_failed ? "nack" : "ack", sources.size());

        /// Commit
        for (auto & source : sources)
        {
            if (!source->hasPendingMessages())
                ++queue_empty;

            if (source->needChannelUpdate())
            {
                LOG_TEST(log, "Channel {} is in error state, will update", source->getChannelID());
                source->updateChannel(*connection);
            }
            else
            {
                /* false is returned by the sendAck function in only two cases:
                * 1) if connection failed. In this case all channels will be closed and will be unable to send ack. Also ack is made based on
                *    delivery tags, which are unique to channels, so if channels fail, those delivery tags will become invalid and there is
                *    no way to send specific ack from a different channel. Actually once the server realises that it has messages in a queue
                *    waiting for confirm from a channel which suddenly closed, it will immediately make those messages accessible to other
                *    consumers. So in this case duplicates are inevitable.
                * 2) size of the sent frame (libraries's internal request interface) exceeds max frame - internal library error. This is more
                *    common for message frames, but not likely to happen to ack frame I suppose. So I do not believe it is likely to happen.
                *    Also in this case if channel didn't get closed - it is ok if failed to send ack, because the next attempt to send ack on
                *    the same channel will also commit all previously not-committed messages. Anyway I do not think that for ack frame this
                *    will ever happen.
                */
                if (write_failed ? source->sendNack() : source->sendAck())
                {
                    /// Iterate loop to activate error callbacks if they happened
                    connection->getHandler().iterateLoop();
                    if (!connection->isConnected())
                        break;
                }

                connection->getHandler().iterateLoop();
            }
        }
    }

    if (write_failed)
    {
        LOG_TRACE(log, "Write failed, reschedule");
        return true;
    }

    if (!hasDependencies(getStorageID()))
    {
        /// Do not commit to rabbitmq if the dependency was removed.
        LOG_TRACE(log, "No dependencies, reschedule");
        return false;
    }

    if ((queue_empty == num_created_consumers) && (++read_attempts == MAX_FAILED_READ_ATTEMPTS))
    {
        connection->heartbeat();
        read_attempts = 0;
        LOG_TRACE(log, "Reschedule streaming. Queues are empty.");
        return false;
    }

    LOG_TEST(log, "Will start background loop to let messages be pushed to channel");
    startLoop();


    /// Reschedule.
    return true;
}


void registerStorageRabbitMQ(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        auto rabbitmq_settings = std::make_unique<RabbitMQSettings>();

        if (auto named_collection = tryGetNamedCollectionWithOverrides(args.engine_args, args.getLocalContext(), true, nullptr, &args.table_id))
            rabbitmq_settings->loadFromNamedCollection(named_collection);
        else if (!args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "RabbitMQ engine must have settings");

        if (args.storage_def->settings)
            rabbitmq_settings->loadFromQuery(*args.storage_def);

        if (!(*rabbitmq_settings)[RabbitMQSetting::rabbitmq_host_port].changed
           && !(*rabbitmq_settings)[RabbitMQSetting::rabbitmq_address].changed)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "You must specify either `rabbitmq_host_port` or `rabbitmq_address` settings");

        if (!(*rabbitmq_settings)[RabbitMQSetting::rabbitmq_format].changed)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "You must specify `rabbitmq_format` setting");

        return std::make_shared<StorageRabbitMQ>(args.table_id, args.getContext(), args.columns, args.comment, std::move(rabbitmq_settings), args.mode);
    };

    factory.registerStorage(
        "RabbitMQ",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .source_access_type = AccessTypeObjects::Source::RABBITMQ,
            .has_builtin_setting_fn = RabbitMQSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
This engine allows integrating ClickHouse with [RabbitMQ](https://www.rabbitmq.com).

`RabbitMQ` lets you:

- Publish or subscribe to data flows.
- Process streams as they become available.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = RabbitMQ SETTINGS
    rabbitmq_host_port = 'host:port' [or rabbitmq_address = 'amqp(s)://guest:guest@localhost/vhost'],
    rabbitmq_exchange_name = 'exchange_name',
    rabbitmq_format = 'data_format'[,]
    [rabbitmq_exchange_type = 'exchange_type',]
    [rabbitmq_routing_key_list = 'key1,key2,...',]
    [rabbitmq_secure = 0,]
    [rabbitmq_schema = '',]
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_queue_base = 'queue',]
    [rabbitmq_deadletter_exchange = 'dl-exchange',]
    [rabbitmq_persistent = 0,]
    [rabbitmq_skip_broken_messages = N,]
    [rabbitmq_max_block_size = N,]
    [rabbitmq_flush_interval_ms = N,]
    [rabbitmq_queue_settings_list = 'x-dead-letter-exchange=my-dlx,x-max-length=10,x-overflow=reject-publish',]
    [rabbitmq_queue_consume = false,]
    [rabbitmq_address = '',]
    [rabbitmq_vhost = '/',]
    [rabbitmq_username = '',]
    [rabbitmq_password = '',]
    [rabbitmq_commit_on_select = false,]
    [rabbitmq_max_rows_per_message = 1,]
    [rabbitmq_handle_error_mode = 'default']
```

Required parameters:

- `rabbitmq_host_port` – host:port (for example, `localhost:5672`).
- `rabbitmq_exchange_name` – RabbitMQ exchange name.
- `rabbitmq_format` – Message format. Uses the same notation as the SQL `FORMAT` function, such as `JSONEachRow`. For more information, see the [Formats](../../../interfaces/formats.md) section.

Optional parameters:

- `rabbitmq_exchange_type` – The type of RabbitMQ exchange: `direct`, `fanout`, `topic`, `headers`, `consistent_hash`. Default: `fanout`.
- `rabbitmq_routing_key_list` – A comma-separated list of routing keys.
- `rabbitmq_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/) requires the path to the schema file and the name of the root `schema.capnp:Message` object.
- `rabbitmq_num_consumers` – The number of consumers per table. Specify more consumers if the throughput of one consumer is insufficient. Default: `1`
- `rabbitmq_num_queues` – Total number of queues. Increasing this number can significantly improve performance. Default: `1`.
- `rabbitmq_queue_base` - Specify a hint for queue names. Use cases of this setting are described below.
- `rabbitmq_persistent` - If set to 1 (true), in insert query delivery mode will be set to 2 (marks messages as 'persistent'). Default: `0`.
- `rabbitmq_skip_broken_messages` – RabbitMQ message parser tolerance to schema-incompatible messages per block. If `rabbitmq_skip_broken_messages = N` then the engine skips *N* RabbitMQ messages that cannot be parsed (a message equals a row of data). Default: `0`.
- `rabbitmq_max_block_size` - Number of row collected before flushing data from RabbitMQ. Default: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size).
- `rabbitmq_flush_interval_ms` - Timeout for flushing data from RabbitMQ. Default: [stream_flush_interval_ms](/operations/settings/settings#stream_flush_interval_ms).
- `rabbitmq_queue_settings_list` - allows to set RabbitMQ settings when creating a queue. Available settings: `x-max-length`, `x-max-length-bytes`, `x-message-ttl`, `x-expires`, `x-priority`, `x-max-priority`, `x-overflow`, `x-dead-letter-exchange`, `x-queue-type`. The `durable` setting is enabled automatically for the queue.
- `rabbitmq_address` - Address for connection. Use ether this setting or `rabbitmq_host_port`.
- `rabbitmq_vhost` - RabbitMQ vhost. Default: `'\'`.
- `rabbitmq_queue_consume` - Use user-defined queues and do not make any RabbitMQ setup: declaring exchanges, queues, bindings. Default: `false`.
- `rabbitmq_username` - RabbitMQ username.
- `rabbitmq_password` - RabbitMQ password.
- `reject_unhandled_messages` - Reject messages (send RabbitMQ negative acknowledgement) in case of errors. This setting is automatically enabled if there is a `x-dead-letter-exchange` defined in `rabbitmq_queue_settings_list`.
- `rabbitmq_commit_on_select` - Commit messages when select query is made. Default: `false`.
- `rabbitmq_max_rows_per_message` — The maximum number of rows written in one RabbitMQ message for row-based formats. Default : `1`.
- `rabbitmq_empty_queue_backoff_start_ms` — A start backoff point to reschedule read if the rabbitmq queue is empty.
- `rabbitmq_empty_queue_backoff_end_ms` — An end backoff point to reschedule read if the rabbitmq queue is empty.
- `rabbitmq_empty_queue_backoff_step_ms` — A backoff step to reschedule read if the rabbitmq queue is empty.
- `rabbitmq_handle_error_mode` — How to handle errors for RabbitMQ engine. Possible values: default (the exception will be thrown if we fail to parse a message), stream (the exception message and raw message will be saved in virtual columns `_error` and `_raw_message`), dead_letter_queue (error related data will be saved in system.dead_letter_queue).

### SSL connection {#ssl-connection}

Use either `rabbitmq_secure = 1` or `amqps` in connection address: `rabbitmq_address = 'amqps://guest:guest@localhost/vhost'`.
The default behaviour of the used library is not to check if the created TLS connection is sufficiently secure. Whether the certificate is expired, self-signed, missing or invalid: the connection is simply permitted. More strict checking of certificates can possibly be implemented in the future.

Also format settings can be added along with rabbitmq-related settings.

Example:

```sql
CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5,
                            date_time_input_format = 'best_effort';
```

The RabbitMQ server configuration should be added using the ClickHouse config file.

Required configuration:

```xml
<rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
</rabbitmq>
```

Additional configuration:

```xml
<rabbitmq>
    <vhost>clickhouse</vhost>
</rabbitmq>
```

## Description {#description}

`SELECT` is not particularly useful for reading messages (except for debugging), because each message can be read only once. It is more practical to create real-time threads using [materialized views](../../../sql-reference/statements/create/view.md). To do this:

1.  Use the engine to create a RabbitMQ consumer and consider it a data stream.
2.  Create a table with the desired structure.
3.  Create a materialized view that converts data from the engine and puts it into a previously created table.

When the `MATERIALIZED VIEW` joins the engine, it starts collecting data in the background. This allows you to continually receive messages from RabbitMQ and convert them to the required format using `SELECT`.
One RabbitMQ table can have as many materialized views as you like.

Data can be channeled based on `rabbitmq_exchange_type` and the specified `rabbitmq_routing_key_list`.
There can be no more than one exchange per table. One exchange can be shared between multiple tables - it enables routing into multiple tables at the same time.

Exchange type options:

- `direct` - Routing is based on the exact matching of keys. Example table key list: `key1,key2,key3,key4,key5`, message key can equal any of them.
- `fanout` - Routing to all tables (where exchange name is the same) regardless of the keys.
- `topic` - Routing is based on patterns with dot-separated keys. Examples: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`.
- `headers` - Routing is based on `key=value` matches with a setting `x-match=all` or `x-match=any`. Example table key list: `x-match=all,format=logs,type=report,year=2020`.
- `consistent_hash` - Data is evenly distributed between all bound tables (where the exchange name is the same). Note that this exchange type must be enabled with RabbitMQ plugin: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

Setting `rabbitmq_queue_base` may be used for the following cases:

- to let different tables share queues, so that multiple consumers could be registered for the same queues, which makes a better performance. If using `rabbitmq_num_consumers` and/or `rabbitmq_num_queues` settings, the exact match of queues is achieved in case these parameters are the same.
- to be able to restore reading from certain durable queues when not all messages were successfully consumed. To resume consumption from one specific queue - set its name in `rabbitmq_queue_base` setting and do not specify `rabbitmq_num_consumers` and `rabbitmq_num_queues` (defaults to 1). To resume consumption from all queues, which were declared for a specific table - just specify the same settings: `rabbitmq_queue_base`, `rabbitmq_num_consumers`, `rabbitmq_num_queues`. By default, queue names will be unique to tables.
- to reuse queues as they are declared durable and not auto-deleted. (Can be deleted via any of RabbitMQ CLI tools.)

To improve performance, received messages are grouped into blocks the size of [max_insert_block_size](/operations/settings/settings#max_insert_block_size). If the block wasn't formed within [stream_flush_interval_ms](../../../operations/server-configuration-parameters/settings.md) milliseconds, the data will be flushed to the table regardless of the completeness of the block.

If `rabbitmq_num_consumers` and/or `rabbitmq_num_queues` settings are specified along with `rabbitmq_exchange_type`, then:

- `rabbitmq-consistent-hash-exchange` plugin must be enabled.
- `message_id` property of the published messages must be specified (unique for each message/batch).

For insert query there is message metadata, which is added for each published message: `messageID` and `republished` flag (true, if published more than once) - can be accessed via message headers.

Do not use the same table for inserts and materialized views.

Example:

```sql
CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_exchange_type = 'headers',
                            rabbitmq_routing_key_list = 'format=logs,type=report,year=2020',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5;

CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

SELECT key, value FROM daily ORDER BY key;
```

## Virtual columns {#virtual-columns}

- `_exchange_name` - RabbitMQ exchange name. Data type: `String`.
- `_channel_id` - ChannelID, on which consumer, who received the message, was declared. Data type: `String`.
- `_delivery_tag` - DeliveryTag of the received message. Scoped per channel. Data type: `UInt64`.
- `_redelivered` - `redelivered` flag of the message. Data type: `UInt8`.
- `_message_id` - messageID of the received message; non-empty if was set, when message was published. Data type: `String`.
- `_timestamp` - timestamp of the received message; non-empty if was set, when message was published. Data type: `UInt64`.

Additional virtual columns when `rabbitmq_handle_error_mode='stream'`:

- `_raw_message` - Raw message that couldn't be parsed successfully. Data type: `Nullable(String)`.
- `_error` - Exception message happened during failed parsing. Data type: `Nullable(String)`.

Note: `_raw_message` and `_error` virtual columns are filled only in case of exception during parsing, they are always `NULL` when message was parsed successfully.

## Caveats {#caveats}

Even though you may specify [default column expressions](/sql-reference/statements/create/table.md/#default_values) (such as `DEFAULT`, `MATERIALIZED`, `ALIAS`) in the table definition, these will be ignored. Instead, the columns will be filled with their respective default values for their types.

## Data formats support {#data-formats-support}

RabbitMQ engine supports all [formats](../../../interfaces/formats.md) supported in ClickHouse.
The number of rows in one RabbitMQ message depends on whether the format is row-based or block-based:

- For row-based formats the number of rows in one RabbitMQ message can be controlled by setting `rabbitmq_max_rows_per_message`.
- For block-based formats we cannot divide block into smaller parts, but the number of rows in one block can be controlled by general setting [max_block_size](/operations/settings/settings#max_block_size).
)DOCS_MD",
            .syntax = "ENGINE = RabbitMQ() SETTINGS rabbitmq_host_port = 'host:port', rabbitmq_exchange_name = 'exchange', rabbitmq_format = 'format', ...",
            .related = {"Kafka", "NATS", "FileLog"}});
}

}
