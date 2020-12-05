#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/RabbitMQ/RabbitMQBlockInputStream.h>
#include <Storages/RabbitMQ/RabbitMQBlockOutputStream.h>
#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/config_version.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/parseAddress.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <amqpcpp.h>

namespace DB
{

static const auto CONNECT_SLEEP = 200;
static const auto RETRIES_MAX = 20;
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
        const Context & context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<RabbitMQSettings> rabbitmq_settings_)
        : IStorage(table_id_)
        , global_context(context_.getGlobalContext())
        , rabbitmq_settings(std::move(rabbitmq_settings_))
        , exchange_name(global_context.getMacros()->expand(rabbitmq_settings->rabbitmq_exchange_name.value))
        , format_name(global_context.getMacros()->expand(rabbitmq_settings->rabbitmq_format.value))
        , exchange_type(defineExchangeType(global_context.getMacros()->expand(rabbitmq_settings->rabbitmq_exchange_type.value)))
        , routing_keys(parseRoutingKeys(global_context.getMacros()->expand(rabbitmq_settings->rabbitmq_routing_key_list.value)))
        , row_delimiter(rabbitmq_settings->rabbitmq_row_delimiter.value)
        , schema_name(global_context.getMacros()->expand(rabbitmq_settings->rabbitmq_schema.value))
        , num_consumers(rabbitmq_settings->rabbitmq_num_consumers.value)
        , num_queues(rabbitmq_settings->rabbitmq_num_queues.value)
        , queue_base(global_context.getMacros()->expand(rabbitmq_settings->rabbitmq_queue_base.value))
        , deadletter_exchange(global_context.getMacros()->expand(rabbitmq_settings->rabbitmq_deadletter_exchange.value))
        , persistent(rabbitmq_settings->rabbitmq_persistent.value)
        , hash_exchange(num_consumers > 1 || num_queues > 1)
        , log(&Poco::Logger::get("StorageRabbitMQ (" + table_id_.table_name + ")"))
        , address(global_context.getMacros()->expand(rabbitmq_settings->rabbitmq_host_port.value))
        , parsed_address(parseAddress(address, 5672))
        , login_password(std::make_pair(
                    global_context.getConfigRef().getString("rabbitmq.username"),
                    global_context.getConfigRef().getString("rabbitmq.password")))
        , semaphore(0, num_consumers)
        , unique_strbase(getRandomName())
        , queue_size(std::max(QUEUE_SIZE, static_cast<uint32_t>(getMaxBlockSize())))
{
    loop = std::make_unique<uv_loop_t>();
    uv_loop_init(loop.get());
    event_handler = std::make_shared<RabbitMQHandler>(loop.get(), log);
    restoreConnection(false);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    rabbitmq_context = addSettings(global_context);
    rabbitmq_context->makeQueryContext();

    /// One looping task for all consumers as they share the same connection == the same handler == the same event loop
    event_handler->updateLoopState(Loop::STOP);
    looping_task = global_context.getSchedulePool().createTask("RabbitMQLoopingTask", [this]{ loopingFunc(); });
    looping_task->deactivate();

    streaming_task = global_context.getSchedulePool().createTask("RabbitMQStreamingTask", [this]{ streamingToViewsFunc(); });
    streaming_task->deactivate();

    connection_task = global_context.getSchedulePool().createTask("RabbitMQConnectionTask", [this]{ connectionFunc(); });
    connection_task->deactivate();

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
}


Names StorageRabbitMQ::parseRoutingKeys(String routing_key_list)
{
    Names result;
    boost::split(result, routing_key_list, [](char c){ return c == ','; });
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


std::shared_ptr<Context> StorageRabbitMQ::addSettings(const Context & context) const
{
    auto modified_context = std::make_shared<Context>(context);
    modified_context->setSetting("input_format_skip_unknown_fields", true);
    modified_context->setSetting("input_format_allow_errors_ratio", 0.);
    modified_context->setSetting("input_format_allow_errors_num", rabbitmq_settings->rabbitmq_skip_broken_messages.value);

    if (!schema_name.empty())
        modified_context->setSetting("format_schema", schema_name);

    return modified_context;
}


void StorageRabbitMQ::loopingFunc()
{
    if (event_handler->connectionRunning())
        event_handler->startLoop();
}


void StorageRabbitMQ::connectionFunc()
{
    if (restoreConnection(true))
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
        event_handler->updateLoopState(Loop::STOP);

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
         : (global_context.getSettingsRef().max_insert_block_size.value / num_consumers);
 }


void StorageRabbitMQ::initRabbitMQ()
{
    setup_channel = std::make_shared<AMQP::TcpChannel>(connection.get());

    initExchange();
    bindExchange();

    for (const auto i : ext::range(0, num_queues))
        bindQueue(i + 1);

    LOG_TRACE(log, "RabbitMQ setup completed");

    rabbit_is_ready = true;
    setup_channel->close();
}


void StorageRabbitMQ::initExchange()
{
    /* Binding scheme is the following: client's exchange -> key bindings by routing key list -> bridge exchange (fanout) ->
     * -> sharding exchange (only if needed) -> queues
     */
    setup_channel->declareExchange(exchange_name, exchange_type, AMQP::durable)
    .onError([&](const char * message)
    {
        /* This error can be a result of attempt to declare exchange if it was already declared but
         * 1) with different exchange type. In this case can
         * - manually delete previously declared exchange and create a new one.
         * - throw an error that the exchange with this name but another type is already declared and ask client to delete it himself
         *   if it is not needed anymore or use another exchange name.
         * 2) with different exchange settings. This can only happen if client himself declared exchange with the same name and
         * specified its own settings, which differ from this implementation.
         */
        throw Exception("Unable to declare exchange. Make sure specified exchange is not already declared. Error: "
                + std::string(message), ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE);
    });

    /// Bridge exchange is needed to easily disconnect consumer queues and also simplifies queue bindings
    setup_channel->declareExchange(bridge_exchange, AMQP::fanout, AMQP::durable + AMQP::autodelete)
    .onError([&](const char * message)
    {
        /// This error is not supposed to happen as this exchange name is always unique to type and its settings
        throw Exception(
            ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE, "Unable to declare bridge exchange ({}). Reason: {}", bridge_exchange, std::string(message));
    });

    if (!hash_exchange)
    {
        consumer_exchange = bridge_exchange;
        return;
    }

    /* Change hash property because by default it will be routing key, which has to be an integer, but with support for any exchange
     * type - routing keys might be of any type
     */
    AMQP::Table binding_arguments;
    binding_arguments["hash-property"] = "message_id";

    /// Declare exchange for sharding.
    setup_channel->declareExchange(sharding_exchange, AMQP::consistent_hash, AMQP::durable + AMQP::autodelete, binding_arguments)
    .onError([&](const char * message)
    {
        /* This error can be a result of same reasons as above for exchange_name, i.e. it will mean that sharding exchange name appeared
         * to be the same as some other exchange (which purpose is not for sharding). So probably actual error reason: queue_base parameter
         * is bad.
         */
        throw Exception(
           ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE,
           "Unable to declare sharding exchange ({}). Reason: {}", sharding_exchange, std::string(message));
    });

    setup_channel->bindExchange(bridge_exchange, sharding_exchange, routing_keys[0])
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


void StorageRabbitMQ::bindExchange()
{
    std::atomic<bool> binding_created = false;
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

        setup_channel->bindExchange(exchange_name, bridge_exchange, routing_keys[0], bind_headers)
        .onSuccess([&]() { binding_created = true; })
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
        setup_channel->bindExchange(exchange_name, bridge_exchange, routing_keys[0])
        .onSuccess([&]() { binding_created = true; })
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
            setup_channel->bindExchange(exchange_name, bridge_exchange, routing_key)
            .onSuccess([&]()
            {
                ++bound_keys;
                if (bound_keys == routing_keys.size())
                    binding_created = true;
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

    while (!binding_created)
    {
        event_handler->iterateLoop();
    }
}


void StorageRabbitMQ::bindQueue(size_t queue_id)
{
    std::atomic<bool> binding_created = false;

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
        setup_channel->bindQueue(consumer_exchange, queue_name, std::to_string(queue_id))
        .onSuccess([&] { binding_created = true; })
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

    queue_settings["x-max-length"] = queue_size;

    if (!deadletter_exchange.empty())
        queue_settings["x-dead-letter-exchange"] = deadletter_exchange;
    else
        queue_settings["x-overflow"] = "reject-publish";

    /* The first option not just simplifies queue_name, but also implements the possibility to be able to resume reading from one
     * specific queue when its name is specified in queue_base setting
     */
    const String queue_name = !hash_exchange ? queue_base : std::to_string(queue_id) + "_" + queue_base;
    setup_channel->declareQueue(queue_name, AMQP::durable, queue_settings).onSuccess(success_callback).onError(error_callback);

    while (!binding_created)
    {
        event_handler->iterateLoop();
    }
}


bool StorageRabbitMQ::restoreConnection(bool reconnecting)
{
    size_t cnt_retries = 0;

    if (reconnecting)
    {
        connection->close(); /// Connection might be unusable, but not closed

        /* Connection is not closed immediately (firstly, all pending operations are completed, and then
         * an AMQP closing-handshake is  performed). But cannot open a new connection until previous one is properly closed
         */
        while (!connection->closed() && ++cnt_retries != RETRIES_MAX)
            event_handler->iterateLoop();

        /// This will force immediate closure if not yet closed
        if (!connection->closed())
            connection->close(true);

        LOG_TRACE(log, "Trying to restore connection to " + address);
    }

    connection = std::make_unique<AMQP::TcpConnection>(event_handler.get(),
            AMQP::Address(parsed_address.first, parsed_address.second, AMQP::Login(login_password.first, login_password.second), "/"));

    cnt_retries = 0;
    while (!connection->ready() && !stream_cancelled && ++cnt_retries != RETRIES_MAX)
    {
        event_handler->iterateLoop();
        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }

    return event_handler->connectionRunning();
}


bool StorageRabbitMQ::updateChannel(ChannelPtr & channel)
{
    if (event_handler->connectionRunning())
    {
        channel = std::make_shared<AMQP::TcpChannel>(connection.get());
        return true;
    }

    channel = nullptr;
    return false;
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
    std::call_once(flag, [&]()
    {
        streaming_task->deactivate();
        event_handler->updateLoopState(Loop::STOP);
        looping_task->deactivate();

        setup_channel = std::make_shared<AMQP::TcpChannel>(connection.get());
        setup_channel->removeExchange(bridge_exchange)
        .onSuccess([&]()
        {
            exchange_removed.store(true);
        })
        .onError([&](const char * message)
        {
            throw Exception("Unable to remove exchange. Reason: " + std::string(message), ErrorCodes::CANNOT_REMOVE_RABBITMQ_EXCHANGE);
        });

        while (!exchange_removed.load())
        {
            event_handler->iterateLoop();
        }

        setup_channel->close();
    });
}


Pipe StorageRabbitMQ::read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & /* query_info */,
        const Context & context,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        unsigned /* num_streams */)
{
    if (!rabbit_is_ready)
        throw Exception("RabbitMQ setup not finished. Connection might be lost", ErrorCodes::CANNOT_CONNECT_RABBITMQ);

    if (num_created_consumers == 0)
        return {};

    auto sample_block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
    auto modified_context = addSettings(context);
    auto block_size = getMaxBlockSize();

    if (!event_handler->connectionRunning())
    {
        if (event_handler->loopRunning())
            deactivateTask(looping_task, false, true);
        restoreConnection(true);
    }

    Pipes pipes;
    pipes.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto rabbit_stream = std::make_shared<RabbitMQBlockInputStream>(
                *this, metadata_snapshot, modified_context, column_names, block_size);

        auto converting_stream = std::make_shared<ConvertingBlockInputStream>(
            rabbit_stream, sample_block, ConvertingBlockInputStream::MatchColumnsMode::Name);
        pipes.emplace_back(std::make_shared<SourceFromInputStream>(converting_stream));
    }

    if (!event_handler->loopRunning() && event_handler->connectionRunning())
        looping_task->activateAndSchedule();

    LOG_DEBUG(log, "Starting reading {} streams", pipes.size());
    auto united_pipe = Pipe::unitePipes(std::move(pipes));
    united_pipe.addInterpreterContext(modified_context);
    return united_pipe;
}


BlockOutputStreamPtr StorageRabbitMQ::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, const Context & context)
{
    return std::make_shared<RabbitMQBlockOutputStream>(*this, metadata_snapshot, context);
}


void StorageRabbitMQ::startup()
{
    if (event_handler->connectionRunning())
        initRabbitMQ();
    else
        connection_task->activateAndSchedule();

    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            pushReadBuffer(createReadBuffer());
            ++num_created_consumers;
        }
        catch (const AMQP::Exception & e)
        {
            LOG_ERROR(log, "Got AMQ exception {}", e.what());
            throw;
        }
    }

    event_handler->updateLoopState(Loop::RUN);
    streaming_task->activateAndSchedule();
}


void StorageRabbitMQ::shutdown()
{
    stream_cancelled = true;
    wait_confirm = false;

    deactivateTask(streaming_task, true, false);
    deactivateTask(looping_task, true, true);
    deactivateTask(connection_task, true, false);

    connection->close();

    size_t cnt_retries = 0;
    while (!connection->closed() && ++cnt_retries != RETRIES_MAX)
        event_handler->iterateLoop();

    /// Should actually force closure, if not yet closed, but it generates distracting error logs
    //if (!connection->closed())
    //    connection->close(true);

    for (size_t i = 0; i < num_created_consumers; ++i)
        popReadBuffer();
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
    ChannelPtr consumer_channel;
    if (event_handler->connectionRunning())
        consumer_channel = std::make_shared<AMQP::TcpChannel>(connection.get());

    return std::make_shared<ReadBufferFromRabbitMQConsumer>(
        consumer_channel, event_handler, queues, ++consumer_id,
        unique_strbase, log, row_delimiter, queue_size, stream_cancelled);
}


ProducerBufferPtr StorageRabbitMQ::createWriteBuffer()
{
    return std::make_shared<WriteBufferToRabbitMQProducer>(
        parsed_address, global_context, login_password, routing_keys, exchange_name, exchange_type,
        producer_id.fetch_add(1), persistent, wait_confirm, log,
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
        auto table = DatabaseCatalog::instance().tryGetTable(db_tab, global_context);
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


void StorageRabbitMQ::streamingToViewsFunc()
{
    if (rabbit_is_ready && (event_handler->connectionRunning() || restoreConnection(true)))
    {
        try
        {
            auto table_id = getStorageID();

            // Check if at least one direct dependency is attached
            size_t dependencies_count = DatabaseCatalog::instance().getDependencies(table_id).size();

            if (dependencies_count)
            {
                auto start_time = std::chrono::steady_clock::now();

                // Keep streaming as long as there are attached views and streaming is not cancelled
                while (!stream_cancelled && num_created_consumers > 0)
                {
                    if (!checkDependencies(table_id))
                        break;

                    LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                    if (streamToViews())
                        break;

                    auto end_time = std::chrono::steady_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
                    if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                    {
                        event_handler->updateLoopState(Loop::STOP);
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

    /// Wait for attached views
    if (!stream_cancelled)
        streaming_task->scheduleAfter(RESCHEDULE_MS);
}


bool StorageRabbitMQ::streamToViews()
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, global_context);
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, *rabbitmq_context, false, true, true);
    auto block_io = interpreter.execute();

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto column_names = block_io.out->getHeader().getNames();
    auto sample_block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());

    auto block_size = getMaxBlockSize();

    // Create a stream for each consumer and join them in a union stream
    BlockInputStreams streams;
    streams.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto stream = std::make_shared<RabbitMQBlockInputStream>(
                *this, metadata_snapshot, rabbitmq_context, column_names, block_size, false);
        streams.emplace_back(stream);

        // Limit read batch to maximum block size to allow DDL
        StreamLocalLimits limits;

        limits.speed_limits.max_execution_time = rabbitmq_settings->rabbitmq_flush_interval_ms.changed
                                                  ? rabbitmq_settings->rabbitmq_flush_interval_ms
                                                  : global_context.getSettingsRef().stream_flush_interval_ms;

        limits.timeout_overflow_mode = OverflowMode::BREAK;

        stream->setLimits(limits);
    }

    // Join multiple streams if necessary
    BlockInputStreamPtr in;
    if (streams.size() > 1)
        in = std::make_shared<UnionBlockInputStream>(streams, nullptr, streams.size());
    else
        in = streams[0];

    std::atomic<bool> stub = {false};

    if (!event_handler->loopRunning())
    {
        event_handler->updateLoopState(Loop::RUN);
        looping_task->activateAndSchedule();
    }

    copyData(*in, *block_io.out, &stub);

    /* Note: sending ack() with loop running in another thread will lead to a lot of data races inside the library, but only in case
     * error occurs or connection is lost while ack is being sent
     */
    deactivateTask(looping_task, false, true);
    size_t queue_empty = 0;

    if (!event_handler->connectionRunning())
    {
        if (stream_cancelled)
            return true;

        if (restoreConnection(true))
        {
            for (auto & stream : streams)
                stream->as<RabbitMQBlockInputStream>()->updateChannel();
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
        for (auto & stream : streams)
        {
            if (stream->as<RabbitMQBlockInputStream>()->queueEmpty())
                ++queue_empty;

            if (stream->as<RabbitMQBlockInputStream>()->needChannelUpdate())
            {
                auto buffer = stream->as<RabbitMQBlockInputStream>()->getBuffer();
                if (buffer)
                {
                    if (buffer->queuesCount() != queues.size())
                        buffer->updateQueues(queues);

                    buffer->updateAckTracker();

                    if (updateChannel(buffer->getChannel()))
                        buffer->setupChannel();
                }
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
            if (!stream->as<RabbitMQBlockInputStream>()->sendAck())
            {
                /// Iterate loop to activate error callbacks if they happened
                event_handler->iterateLoop();
                if (!event_handler->connectionRunning())
                    break;
            }

            event_handler->iterateLoop();
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
        event_handler->updateLoopState(Loop::RUN);
        looping_task->activateAndSchedule();
    }

    return false;
}


void registerStorageRabbitMQ(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();
        bool has_settings = args.storage_def->settings;

        auto rabbitmq_settings = std::make_unique<RabbitMQSettings>();
        if (has_settings)
            rabbitmq_settings->loadFromQuery(*args.storage_def);

        // Check arguments and settings
        #define CHECK_RABBITMQ_STORAGE_ARGUMENT(ARG_NUM, ARG_NAME)                                           \
            /* One of the three required arguments is not specified */                                       \
            if (args_count < (ARG_NUM) && (ARG_NUM) <= 3 && !rabbitmq_settings->ARG_NAME.changed)            \
            {                                                                                                \
                throw Exception("Required parameter '" #ARG_NAME "' for storage RabbitMQ not specified",     \
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);                                           \
            }                                                                                                \
            if (args_count >= (ARG_NUM))                                                                     \
            {                                                                                                \
                if (rabbitmq_settings->ARG_NAME.changed) /* The same argument is given in two places */      \
                {                                                                                            \
                    throw Exception("The argument №" #ARG_NUM " of storage RabbitMQ "                        \
                        "and the parameter '" #ARG_NAME "' is duplicated", ErrorCodes::BAD_ARGUMENTS);       \
                }                                                                                            \
            }

        CHECK_RABBITMQ_STORAGE_ARGUMENT(1, rabbitmq_host_port)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(2, rabbitmq_exchange_name)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(3, rabbitmq_format)

        CHECK_RABBITMQ_STORAGE_ARGUMENT(4, rabbitmq_exchange_type)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(5, rabbitmq_routing_key_list)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(6, rabbitmq_row_delimiter)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(7, rabbitmq_schema)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(8, rabbitmq_num_consumers)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(9, rabbitmq_num_queues)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(10, rabbitmq_queue_base)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(11, rabbitmq_deadletter_exchange)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(12, rabbitmq_persistent)

        CHECK_RABBITMQ_STORAGE_ARGUMENT(13, rabbitmq_skip_broken_messages)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(14, rabbitmq_max_block_size)
        CHECK_RABBITMQ_STORAGE_ARGUMENT(15, rabbitmq_flush_interval_ms)

        #undef CHECK_RABBITMQ_STORAGE_ARGUMENT

        return StorageRabbitMQ::create(args.table_id, args.context, args.columns, std::move(rabbitmq_settings));
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
