#include <amqpcpp.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Storages/RabbitMQ/RabbitMQSink.h>
#include <Storages/RabbitMQ/RabbitMQSource.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/parseAddress.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>

namespace DB
{

static const uint32_t QUEUE_SIZE = 100000;
static const auto MAX_FAILED_READ_ATTEMPTS = 10;
static const auto RESCHEDULE_MS = 500;
static const auto BACKOFF_TRESHOLD = 32000;
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


StorageRabbitMQ::StorageRabbitMQ(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<RabbitMQSettings> rabbitmq_settings_,
        bool is_attach_)
        : IStorage(table_id_)
        , WithContext(context_->getGlobalContext())
        , rabbitmq_settings(std::move(rabbitmq_settings_))
        , exchange_name(getContext()->getMacros()->expand(rabbitmq_settings->rabbitmq_exchange_name))
        , format_name(getContext()->getMacros()->expand(rabbitmq_settings->rabbitmq_format))
        , exchange_type(defineExchangeType(getContext()->getMacros()->expand(rabbitmq_settings->rabbitmq_exchange_type)))
        , routing_keys(parseSettings(getContext()->getMacros()->expand(rabbitmq_settings->rabbitmq_routing_key_list)))
        , row_delimiter(rabbitmq_settings->rabbitmq_row_delimiter.value)
        , schema_name(getContext()->getMacros()->expand(rabbitmq_settings->rabbitmq_schema))
        , num_consumers(rabbitmq_settings->rabbitmq_num_consumers.value)
        , num_queues(rabbitmq_settings->rabbitmq_num_queues.value)
        , queue_base(getContext()->getMacros()->expand(rabbitmq_settings->rabbitmq_queue_base))
        , queue_settings_list(parseSettings(getContext()->getMacros()->expand(rabbitmq_settings->rabbitmq_queue_settings_list)))
        , persistent(rabbitmq_settings->rabbitmq_persistent.value)
        , use_user_setup(rabbitmq_settings->rabbitmq_queue_consume.value)
        , hash_exchange(num_consumers > 1 || num_queues > 1)
        , log(&Poco::Logger::get("StorageRabbitMQ (" + table_id_.table_name + ")"))
        , semaphore(0, num_consumers)
        , unique_strbase(getRandomName())
        , queue_size(std::max(QUEUE_SIZE, static_cast<uint32_t>(getMaxBlockSize())))
        , milliseconds_to_wait(RESCHEDULE_MS)
        , is_attach(is_attach_)
{
    auto parsed_address = parseAddress(getContext()->getMacros()->expand(rabbitmq_settings->rabbitmq_host_port), 5672);
    context_->getRemoteHostFilter().checkHostAndPort(parsed_address.first, toString(parsed_address.second));

    auto rabbitmq_username = rabbitmq_settings->rabbitmq_username.value;
    auto rabbitmq_password = rabbitmq_settings->rabbitmq_password.value;
    configuration =
    {
        .host = parsed_address.first,
        .port = parsed_address.second,
        .username = rabbitmq_username.empty() ? getContext()->getConfigRef().getString("rabbitmq.username") : rabbitmq_username,
        .password = rabbitmq_password.empty() ? getContext()->getConfigRef().getString("rabbitmq.password") : rabbitmq_password,
        .vhost = getContext()->getConfigRef().getString("rabbitmq.vhost", getContext()->getMacros()->expand(rabbitmq_settings->rabbitmq_vhost)),
        .secure = rabbitmq_settings->rabbitmq_secure.value,
        .connection_string = getContext()->getMacros()->expand(rabbitmq_settings->rabbitmq_address)
    };

    if (configuration.secure)
        SSL_library_init();

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
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
        else if (!is_attach)
            throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot connect to {}", connection->connectionInfoForLog());
    }
    catch (...)
    {
        tryLogCurrentException(log);
        if (!is_attach)
            throw;
    }

    /// One looping task for all consumers as they share the same connection == the same handler == the same event loop
    looping_task = getContext()->getMessageBrokerSchedulePool().createTask("RabbitMQLoopingTask", [this]{ loopingFunc(); });
    looping_task->deactivate();

    streaming_task = getContext()->getMessageBrokerSchedulePool().createTask("RabbitMQStreamingTask", [this]{ streamingToViewsFunc(); });
    streaming_task->deactivate();

    connection_task = getContext()->getMessageBrokerSchedulePool().createTask("RabbitMQConnectionTask", [this]{ connectionFunc(); });
    connection_task->deactivate();
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
        else throw Exception("Invalid exchange type", ErrorCodes::BAD_ARGUMENTS);
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
    else
        return fmt::format("{}_{}_{}", name, table_id.database_name, table_id.table_name);
}


ContextMutablePtr StorageRabbitMQ::addSettings(ContextPtr local_context) const
{
    auto modified_context = Context::createCopy(local_context);
    modified_context->setSetting("input_format_skip_unknown_fields", true);
    modified_context->setSetting("input_format_allow_errors_ratio", 0.);
    modified_context->setSetting("input_format_allow_errors_num", rabbitmq_settings->rabbitmq_skip_broken_messages.value);

    if (!schema_name.empty())
        modified_context->setSetting("format_schema", schema_name);

    for (const auto & setting : *rabbitmq_settings)
    {
        const auto & setting_name = setting.getName();

        /// check for non-rabbitmq-related settings
        if (!setting_name.starts_with("rabbitmq_"))
            modified_context->setSetting(setting_name, setting.getValue());
    }

    return modified_context;
}


void StorageRabbitMQ::loopingFunc()
{
    connection->getHandler().startLoop();
}


void StorageRabbitMQ::stopLoop()
{
    connection->getHandler().updateLoopState(Loop::STOP);
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
    connection->getHandler().updateLoopState(Loop::STOP);
}

void StorageRabbitMQ::startLoop()
{
    assert(rabbit_is_ready);
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
    if (rabbit_is_ready)
        return;

    if (connection->reconnect())
        initRabbitMQ();
    else
        connection_task->scheduleAfter(RESCHEDULE_MS);
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
     return rabbitmq_settings->rabbitmq_max_block_size.changed
         ? rabbitmq_settings->rabbitmq_max_block_size.value
         : (getContext()->getSettingsRef().max_insert_block_size.value / num_consumers);
}


void StorageRabbitMQ::initRabbitMQ()
{
    if (shutdown_called || rabbit_is_ready)
        return;

    if (use_user_setup)
    {
        queues.emplace_back(queue_base);
        rabbit_is_ready = true;
        return;
    }

    try
    {
        auto rabbit_channel = connection->createChannel();

        /// Main exchange -> Bridge exchange -> ( Sharding exchange ) -> Queues -> Consumers

        initExchange(*rabbit_channel);
        bindExchange(*rabbit_channel);

        for (const auto i : collections::range(0, num_queues))
            bindQueue(i + 1, *rabbit_channel);

        LOG_TRACE(log, "RabbitMQ setup completed");
        rabbit_is_ready = true;
        rabbit_channel->close();
    }
    catch (...)
    {
        tryLogCurrentException(log);
        if (!is_attach)
            throw;
    }
}


void StorageRabbitMQ::initExchange(AMQP::TcpChannel & rabbit_channel)
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

    rabbit_channel.declareExchange(exchange_name, exchange_type, AMQP::durable)
    .onError([&](const char * message)
    {
        /// This error can be a result of attempt to declare exchange if it was already declared but
        /// 1) with different exchange type.
        /// 2) with different exchange settings.
        throw Exception("Unable to declare exchange. Make sure specified exchange is not already declared. Error: "
                + std::string(message), ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE);
    });

    rabbit_channel.declareExchange(bridge_exchange, AMQP::fanout, AMQP::durable | AMQP::autodelete)
    .onError([&](const char * message)
    {
        /// This error is not supposed to happen as this exchange name is always unique to type and its settings.
        throw Exception(
            ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE, "Unable to declare bridge exchange ({}). Reason: {}", bridge_exchange, std::string(message));
    });

    if (!hash_exchange)
    {
        consumer_exchange = bridge_exchange;
        return;
    }

    AMQP::Table binding_arguments;

    /// Default routing key property in case of hash exchange is a routing key, which is required to be an integer.
    /// Support for arbitrary exchange type (i.e. arbitrary pattern of routing keys) requires to eliminate this dependency.
    /// This settings changes hash property to message_id.
    binding_arguments["hash-property"] = "message_id";

    /// Declare hash exchange for sharding.
    rabbit_channel.declareExchange(sharding_exchange, AMQP::consistent_hash, AMQP::durable | AMQP::autodelete, binding_arguments)
    .onError([&](const char * message)
    {
        /// This error can be a result of same reasons as above for exchange_name, i.e. it will mean that sharding exchange name appeared
        /// to be the same as some other exchange (which purpose is not for sharding). So probably actual error reason: queue_base parameter
        /// is bad.
        throw Exception(
           ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE,
           "Unable to declare sharding exchange ({}). Reason: {}", sharding_exchange, std::string(message));
    });

    rabbit_channel.bindExchange(bridge_exchange, sharding_exchange, routing_keys[0])
    .onError([&](const char * message)
    {
        throw Exception(
            ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE,
            "Unable to bind bridge exchange ({}) to sharding exchange ({}). Reason: {}",
            bridge_exchange,
            sharding_exchange,
            std::string(message));
    });

    consumer_exchange = sharding_exchange;
}


void StorageRabbitMQ::bindExchange(AMQP::TcpChannel & rabbit_channel)
{
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
        .onSuccess([&]() { connection->getHandler().stopLoop(); })
        .onError([&](const char * message)
        {
            throw Exception(
                ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE,
                "Unable to bind exchange {} to bridge exchange ({}). Reason: {}",
                exchange_name, bridge_exchange, std::string(message));
        });
    }
    else if (exchange_type == AMQP::ExchangeType::fanout || exchange_type == AMQP::ExchangeType::consistent_hash)
    {
        rabbit_channel.bindExchange(exchange_name, bridge_exchange, routing_keys[0])
        .onSuccess([&]() { connection->getHandler().stopLoop(); })
        .onError([&](const char * message)
        {
            throw Exception(
                ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE,
                "Unable to bind exchange {} to bridge exchange ({}). Reason: {}",
                exchange_name, bridge_exchange, std::string(message));
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
                    connection->getHandler().stopLoop();
            })
            .onError([&](const char * message)
            {
                throw Exception(
                    ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE,
                    "Unable to bind exchange {} to bridge exchange ({}). Reason: {}",
                    exchange_name, bridge_exchange, std::string(message));
            });
        }
    }

    connection->getHandler().startBlockingLoop();
}


void StorageRabbitMQ::bindQueue(size_t queue_id, AMQP::TcpChannel & rabbit_channel)
{
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
        .onSuccess([&] { connection->getHandler().stopLoop(); })
        .onError([&](const char * message)
        {
            throw Exception(
                ErrorCodes::CANNOT_CREATE_RABBITMQ_QUEUE_BINDING,
                "Failed to create queue binding for exchange {}. Reason: {}", exchange_name, std::string(message));
        });
    };

    auto error_callback([&](const char * message)
    {
        /* This error is most likely a result of an attempt to declare queue with different settings if it was declared before. So for a
         * given queue name either deadletter_exchange parameter changed or queue_size changed, i.e. table was declared with different
         * max_block_size parameter. Solution: client should specify a different queue_base parameter or manually delete previously
         * declared queues via any of the various cli tools.
         */
        throw Exception("Failed to declare queue. Probably queue settings are conflicting: max_block_size, deadletter_exchange. Attempt \
                specifying differently those settings or use a different queue_base or manually delete previously declared queues, \
                which  were declared with the same names. ERROR reason: "
                + std::string(message), ErrorCodes::BAD_ARGUMENTS);
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

            String key = setting_values[0], value = setting_values[1];

            if (integer_settings.contains(key))
                queue_settings[key] = parse<uint64_t>(value);
            else if (string_settings.find(key) != string_settings.end())
                queue_settings[key] = value;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported queue setting: {}", value);
        }
    }

    /// Impose default settings if there are no user-defined settings.
    if (!queue_settings.contains("x-max-length"))
    {
        queue_settings["x-max-length"] = queue_size;
    }
    if (!queue_settings.contains("x-overflow"))
    {
        queue_settings["x-overflow"] = "reject-publish";
    }

    /// If queue_base - a single name, then it can be used as one specific queue, from which to read.
    /// Otherwise it is used as a generator (unique for current table) of queue names, because it allows to
    /// maximize performance - via setting `rabbitmq_num_queues`.
    const String queue_name = !hash_exchange ? queue_base : std::to_string(queue_id) + "_" + queue_base;

    /// AMQP::autodelete setting is not allowed, because in case of server restart there will be no consumers
    /// and deleting queues should not take place.
    rabbit_channel.declareQueue(queue_name, AMQP::durable, queue_settings).onSuccess(success_callback).onError(error_callback);
    connection->getHandler().startBlockingLoop();
}


bool StorageRabbitMQ::updateChannel(ChannelPtr & channel)
{
    try
    {
        channel = connection->createChannel();
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log);
        return false;
    }
}


void StorageRabbitMQ::prepareChannelForBuffer(ConsumerBufferPtr buffer)
{
    if (!buffer)
        return;

    if (buffer->queuesCount() != queues.size())
        buffer->updateQueues(queues);

    buffer->updateAckTracker();

    if (updateChannel(buffer->getChannel()))
        buffer->setupChannel();
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

            auto rabbit_channel = connection->createChannel();
            rabbit_channel->removeExchange(bridge_exchange)
            .onSuccess([&]()
            {
                connection->getHandler().stopLoop();
            })
            .onError([&](const char * message)
            {
                throw Exception("Unable to remove exchange. Reason: " + std::string(message), ErrorCodes::CANNOT_REMOVE_RABBITMQ_EXCHANGE);
            });

            connection->getHandler().startBlockingLoop();
            rabbit_channel->close();
        }
        catch (...)
        {
            exchange_removed = false;
            throw;
        }
    }
}


Pipe StorageRabbitMQ::read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & /* query_info */,
        ContextPtr local_context,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        unsigned /* num_streams */)
{
    if (!rabbit_is_ready)
        throw Exception("RabbitMQ setup not finished. Connection might be lost", ErrorCodes::CANNOT_CONNECT_RABBITMQ);

    if (num_created_consumers == 0)
        return {};

    if (!local_context->getSettingsRef().stream_like_engine_allow_direct_select)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`");

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

    initializeBuffers();

    Pipes pipes;
    pipes.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto rabbit_source = std::make_shared<RabbitMQSource>(
            *this, storage_snapshot, modified_context, column_names, 1, rabbitmq_settings->rabbitmq_commit_on_select);

        auto converting_dag = ActionsDAG::makeConvertingActions(
            rabbit_source->getPort().getHeader().getColumnsWithTypeAndName(),
            sample_block.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        auto converting = std::make_shared<ExpressionActions>(std::move(converting_dag));
        auto converting_transform = std::make_shared<ExpressionTransform>(rabbit_source->getPort().getHeader(), std::move(converting));

        pipes.emplace_back(std::move(rabbit_source));
        pipes.back().addTransform(std::move(converting_transform));
    }

    if (!connection->getHandler().loopRunning() && connection->isConnected())
        startLoop();

    LOG_DEBUG(log, "Starting reading {} streams", pipes.size());
    auto united_pipe = Pipe::unitePipes(std::move(pipes));
    united_pipe.addInterpreterContext(modified_context);
    return united_pipe;
}


SinkToStoragePtr StorageRabbitMQ::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    return std::make_shared<RabbitMQSink>(*this, metadata_snapshot, local_context);
}


void StorageRabbitMQ::startup()
{
    if (!rabbit_is_ready)
    {
        if (connection->isConnected())
        {
            try
            {
                initRabbitMQ();
            }
            catch (...)
            {
                if (!is_attach)
                    throw;
                tryLogCurrentException(log);
            }
        }
        else
        {
            connection_task->activateAndSchedule();
        }
    }

    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            auto buffer = createReadBuffer();
            pushReadBuffer(std::move(buffer));
            ++num_created_consumers;
        }
        catch (...)
        {
            if (!is_attach)
                throw;
            tryLogCurrentException(log);
        }
    }

    streaming_task->activateAndSchedule();
}


void StorageRabbitMQ::shutdown()
{
    shutdown_called = true;

    /// In case it has not yet been able to setup connection;
    deactivateTask(connection_task, true, false);

    /// The order of deactivating tasks is important: wait for streamingToViews() func to finish and
    /// then wait for background event loop to finish.
    deactivateTask(streaming_task, true, false);
    deactivateTask(looping_task, true, true);

    /// Just a paranoid try catch, it is not actually needed.
    try
    {
        if (drop_table)
        {
            for (auto & buffer : buffers)
                buffer->closeChannel();

            cleanupRabbitMQ();
        }

        /// It is important to close connection here - before removing consumer buffers, because
        /// it will finish and clean callbacks, which might use those buffers data.
        connection->disconnect();

        for (size_t i = 0; i < num_created_consumers; ++i)
            popReadBuffer();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
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
        /// on of them should not affect others.
        /// AMQP::ifempty is not used on purpose.

        rabbit_channel->removeQueue(queue, AMQP::ifunused)
        .onSuccess([&](uint32_t num_messages)
        {
            LOG_TRACE(log, "Successfully deleted queue {}, messages contained {}", queue, num_messages);
            connection->getHandler().stopLoop();
        })
        .onError([&](const char * message)
        {
            LOG_ERROR(log, "Failed to delete queue {}. Error message: {}", queue, message);
            connection->getHandler().stopLoop();
        });
    }
    connection->getHandler().startBlockingLoop();
    rabbit_channel->close();

    /// Also there is no need to cleanup exchanges as they were created with AMQP::autodelete option. Once queues
    /// are removed, exchanges will also be cleaned.
}


void StorageRabbitMQ::pushReadBuffer(ConsumerBufferPtr buffer)
{
    std::lock_guard lock(buffers_mutex);
    buffers.push_back(buffer);
    semaphore.set();
}


ConsumerBufferPtr StorageRabbitMQ::popReadBuffer()
{
    return popReadBuffer(std::chrono::milliseconds::zero());
}


ConsumerBufferPtr StorageRabbitMQ::popReadBuffer(std::chrono::milliseconds timeout)
{
    // Wait for the first free buffer
    if (timeout == std::chrono::milliseconds::zero())
        semaphore.wait();
    else
    {
        if (!semaphore.tryWait(timeout.count()))
            return nullptr;
    }

    // Take the first available buffer from the list
    std::lock_guard lock(buffers_mutex);
    auto buffer = buffers.back();
    buffers.pop_back();

    return buffer;
}


ConsumerBufferPtr StorageRabbitMQ::createReadBuffer()
{
    return std::make_shared<ReadBufferFromRabbitMQConsumer>(
        connection->getHandler(), queues, ++consumer_id,
        unique_strbase, log, row_delimiter, queue_size, shutdown_called);
}


ProducerBufferPtr StorageRabbitMQ::createWriteBuffer()
{
    return std::make_shared<WriteBufferToRabbitMQProducer>(
        configuration, getContext(), routing_keys, exchange_name, exchange_type,
        producer_id.fetch_add(1), persistent, shutdown_called, log,
        row_delimiter ? std::optional<char>{row_delimiter} : std::nullopt, 1, 1024);
}


bool StorageRabbitMQ::checkDependencies(const StorageID & table_id)
{
    // Check if all dependencies are attached
    auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (dependencies.empty())
        return true;

    // Check the dependencies are ready?
    for (const auto & db_tab : dependencies)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(db_tab, getContext());
        if (!table)
            return false;

        // If it materialized view, check it's target table
        auto * materialized_view = dynamic_cast<StorageMaterializedView *>(table.get());
        if (materialized_view && !materialized_view->tryGetTargetTable())
            return false;

        // Check all its dependencies
        if (!checkDependencies(db_tab))
            return false;
    }

    return true;
}


void StorageRabbitMQ::initializeBuffers()
{
    assert(rabbit_is_ready);
    if (!initialized)
    {
        for (const auto & buffer : buffers)
            prepareChannelForBuffer(buffer);
        initialized = true;
    }
}


void StorageRabbitMQ::streamingToViewsFunc()
{
    if (rabbit_is_ready)
    {
        try
        {
            auto table_id = getStorageID();

            // Check if at least one direct dependency is attached
            size_t dependencies_count = DatabaseCatalog::instance().getDependencies(table_id).size();
            bool rabbit_connected = connection->isConnected() || connection->reconnect();

            if (dependencies_count && rabbit_connected)
            {
                initializeBuffers();
                auto start_time = std::chrono::steady_clock::now();

                mv_attached.store(true);

                // Keep streaming as long as there are attached views and streaming is not cancelled
                while (!shutdown_called && num_created_consumers > 0)
                {
                    if (!checkDependencies(table_id))
                        break;

                    LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                    if (streamToViews())
                    {
                        /// Reschedule with backoff.
                        if (milliseconds_to_wait < BACKOFF_TRESHOLD)
                            milliseconds_to_wait *= 2;
                        stopLoopIfNoReaders();
                        break;
                    }
                    else
                    {
                        milliseconds_to_wait = RESCHEDULE_MS;
                    }

                    auto end_time = std::chrono::steady_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
                    if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                    {
                        stopLoopIfNoReaders();
                        LOG_TRACE(log, "Reschedule streaming. Thread work duration limit exceeded.");
                        break;
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    mv_attached.store(false);

    /// If there is no running select, stop the loop which was
    /// activated by previous select.
    if (connection->getHandler().loopRunning())
        stopLoopIfNoReaders();

    if (!shutdown_called)
        streaming_task->scheduleAfter(milliseconds_to_wait);
}


bool StorageRabbitMQ::streamToViews()
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, rabbitmq_context, false, true, true);
    auto block_io = interpreter.execute();

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
    auto column_names = block_io.pipeline.getHeader().getNames();
    auto sample_block = storage_snapshot->getSampleBlockForColumns(column_names);

    auto block_size = getMaxBlockSize();

    // Create a stream for each consumer and join them in a union stream
    std::vector<std::shared_ptr<RabbitMQSource>> sources;
    Pipes pipes;
    sources.reserve(num_created_consumers);
    pipes.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto source = std::make_shared<RabbitMQSource>(
            *this, storage_snapshot, rabbitmq_context, column_names, block_size, false);
        sources.emplace_back(source);
        pipes.emplace_back(source);

        // Limit read batch to maximum block size to allow DDL
        StreamLocalLimits limits;

        limits.speed_limits.max_execution_time = rabbitmq_settings->rabbitmq_flush_interval_ms.changed
                                                  ? rabbitmq_settings->rabbitmq_flush_interval_ms
                                                  : getContext()->getSettingsRef().stream_flush_interval_ms;

        limits.timeout_overflow_mode = OverflowMode::BREAK;

        source->setLimits(limits);
    }

    block_io.pipeline.complete(Pipe::unitePipes(std::move(pipes)));

    if (!connection->getHandler().loopRunning())
        startLoop();

    {
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }

    /* Note: sending ack() with loop running in another thread will lead to a lot of data races inside the library, but only in case
     * error occurs or connection is lost while ack is being sent
     */
    deactivateTask(looping_task, false, true);
    size_t queue_empty = 0;

    if (!connection->isConnected())
    {
        if (shutdown_called)
            return true;

        if (connection->reconnect())
        {
            LOG_DEBUG(log, "Connection restored, updating channels");
            for (auto & source : sources)
                source->updateChannel();
        }
        else
        {
            LOG_TRACE(log, "Reschedule streaming. Unable to restore connection.");
            return true;
        }
    }
    else
    {
        /// Commit
        for (auto & source : sources)
        {
            if (source->queueEmpty())
                ++queue_empty;

            if (source->needChannelUpdate())
            {
                auto buffer = source->getBuffer();
                prepareChannelForBuffer(buffer);
            }

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
            if (!source->sendAck())
            {
                /// Iterate loop to activate error callbacks if they happened
                connection->getHandler().iterateLoop();
                if (!connection->isConnected())
                    break;
            }

            connection->getHandler().iterateLoop();
        }
    }

    if ((queue_empty == num_created_consumers) && (++read_attempts == MAX_FAILED_READ_ATTEMPTS))
    {
        connection->heartbeat();
        read_attempts = 0;
        LOG_TRACE(log, "Reschedule streaming. Queues are empty.");
        return true;
    }
    else
    {
        startLoop();
    }

    /// Do not reschedule, do not stop event loop.
    return false;
}


void registerStorageRabbitMQ(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        auto rabbitmq_settings = std::make_unique<RabbitMQSettings>();
        bool with_named_collection = getExternalDataSourceConfiguration(args.engine_args, *rabbitmq_settings, args.getLocalContext());
        if (!with_named_collection && !args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "RabbitMQ engine must have settings");

        rabbitmq_settings->loadFromQuery(*args.storage_def);

        if (!rabbitmq_settings->rabbitmq_host_port.changed
           && !rabbitmq_settings->rabbitmq_address.changed)
                throw Exception("You must specify either `rabbitmq_host_port` or `rabbitmq_address` settings",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!rabbitmq_settings->rabbitmq_format.changed)
            throw Exception("You must specify `rabbitmq_format` setting", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<StorageRabbitMQ>(args.table_id, args.getContext(), args.columns, std::move(rabbitmq_settings), args.attach);
    };

    factory.registerStorage("RabbitMQ", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });
}


NamesAndTypesList StorageRabbitMQ::getVirtuals() const
{
    return NamesAndTypesList{
            {"_exchange_name", std::make_shared<DataTypeString>()},
            {"_channel_id", std::make_shared<DataTypeString>()},
            {"_delivery_tag", std::make_shared<DataTypeUInt64>()},
            {"_redelivered", std::make_shared<DataTypeUInt8>()},
            {"_message_id", std::make_shared<DataTypeString>()},
            {"_timestamp", std::make_shared<DataTypeUInt64>()}
    };
}

}
