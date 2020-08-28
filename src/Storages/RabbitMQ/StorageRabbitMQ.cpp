#include <Storages/RabbitMQ/StorageRabbitMQ.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
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
#include <Storages/RabbitMQ/RabbitMQSettings.h>
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
static const auto HEARTBEAT_RESCHEDULE_MS = 3000;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CONNECT_RABBITMQ;
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
        Context & context_,
        const ColumnsDescription & columns_,
        const String & host_port_,
        const Names & routing_keys_,
        const String & exchange_name_,
        const String & format_name_,
        char row_delimiter_,
        const String & schema_name_,
        const String & exchange_type_,
        size_t num_consumers_,
        size_t num_queues_,
        const String & queue_base_,
        const String & deadletter_exchange_,
        const bool persistent_)
        : IStorage(table_id_)
        , global_context(context_.getGlobalContext())
        , routing_keys(global_context.getMacros()->expand(routing_keys_))
        , exchange_name(exchange_name_)
        , format_name(global_context.getMacros()->expand(format_name_))
        , row_delimiter(row_delimiter_)
        , schema_name(global_context.getMacros()->expand(schema_name_))
        , num_consumers(num_consumers_)
        , num_queues(num_queues_)
        , queue_base(queue_base_)
        , deadletter_exchange(deadletter_exchange_)
        , persistent(persistent_)
        , log(&Poco::Logger::get("StorageRabbitMQ (" + table_id_.table_name + ")"))
        , parsed_address(parseAddress(global_context.getMacros()->expand(host_port_), 5672))
        , login_password(std::make_pair(
                    global_context.getConfigRef().getString("rabbitmq.username"),
                    global_context.getConfigRef().getString("rabbitmq.password")))
        , semaphore(0, num_consumers_)
{
    loop = std::make_unique<uv_loop_t>();
    uv_loop_init(loop.get());
    event_handler = std::make_shared<RabbitMQHandler>(loop.get(), log);

    if (!restoreConnection(false))
    {
        if (!connection->closed())
            connection->close(true);

        throw Exception("Cannot connect to RabbitMQ", ErrorCodes::CANNOT_CONNECT_RABBITMQ);
    }

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    /// One looping task for all consumers as they share the same connection == the same handler == the same event loop
    event_handler->updateLoopState(Loop::STOP);
    looping_task = global_context.getSchedulePool().createTask("RabbitMQLoopingTask", [this]{ loopingFunc(); });
    looping_task->deactivate();

    streaming_task = global_context.getSchedulePool().createTask("RabbitMQStreamingTask", [this]{ threadFunc(); });
    streaming_task->deactivate();

    heartbeat_task = global_context.getSchedulePool().createTask("RabbitMQHeartbeatTask", [this]{ heartbeatFunc(); });
    heartbeat_task->deactivate();

    hash_exchange = num_consumers > 1 || num_queues > 1;

    if (exchange_type_ != ExchangeType::DEFAULT)
    {
        if (exchange_type_ == ExchangeType::FANOUT)              exchange_type = AMQP::ExchangeType::fanout;
        else if (exchange_type_ == ExchangeType::DIRECT)         exchange_type = AMQP::ExchangeType::direct;
        else if (exchange_type_ == ExchangeType::TOPIC)          exchange_type = AMQP::ExchangeType::topic;
        else if (exchange_type_ == ExchangeType::HASH)           exchange_type = AMQP::ExchangeType::consistent_hash;
        else if (exchange_type_ == ExchangeType::HEADERS)        exchange_type = AMQP::ExchangeType::headers;
        else throw Exception("Invalid exchange type", ErrorCodes::BAD_ARGUMENTS);
    }
    else
    {
        exchange_type = AMQP::ExchangeType::fanout;
    }

    auto table_id = getStorageID();
    String table_name = table_id.table_name;

    if (queue_base.empty())
    {
        /* Make sure that local exchange name is unique for each table and is not the same as client's exchange name. It also needs to
         * be table_name and not just a random string, because local exchanges should be declared the same for same tables
         */
        sharding_exchange = exchange_name + "_" + table_name;

        /* By default without a specified queue name in queue's declaration - its name will be generated by the library, but its better
         * to specify it unique for each table to reuse them once the table is recreated. So it means that queues remain the same for every
         * table unless queue_base table setting is specified (which allows to register consumers to specific queues). Now this is a base
         * for the names of later declared queues
         */
        queue_base = table_name;
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

    /* Generate a random string, which will be used for channelID's, which must be unique to tables and to channels within each table.
     * (Cannot use table_name here because it must be a different string if table was restored)
     */
    unique_strbase = getRandomName();
}


void StorageRabbitMQ::heartbeatFunc()
{
    if (!stream_cancelled && event_handler->connectionRunning())
    {
        connection->heartbeat();
        heartbeat_task->scheduleAfter(HEARTBEAT_RESCHEDULE_MS);
    }
}


void StorageRabbitMQ::loopingFunc()
{
    if (event_handler->connectionRunning())
        event_handler->startLoop();
}


void StorageRabbitMQ::initExchange()
{
    /* Binding scheme is the following: client's exchange -> key bindings by routing key list -> bridge exchange (fanout) ->
     * -> sharding exchange (only if needed) -> queues
     */
    setup_channel->declareExchange(exchange_name, exchange_type, AMQP::durable)
    .onError([&](const char * message)
    {
        throw Exception("Unable to declare exchange. Make sure specified exchange is not already declared. Error: "
                + std::string(message), ErrorCodes::LOGICAL_ERROR);
    });

    /// Bridge exchange is needed to easily disconnect consumer queues and also simplifies queue bindings
    setup_channel->declareExchange(bridge_exchange, AMQP::fanout, AMQP::durable + AMQP::autodelete)
    .onError([&](const char * message)
    {
        throw Exception("Unable to declare exchange. Reason: " + std::string(message), ErrorCodes::LOGICAL_ERROR);
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
        throw Exception("Unable to declare exchange. Reason: " + std::string(message), ErrorCodes::LOGICAL_ERROR);
    });

    setup_channel->bindExchange(bridge_exchange, sharding_exchange, routing_keys[0])
    .onError([&](const char * message)
    {
        throw Exception("Unable to bind exchange. Reason: " + std::string(message), ErrorCodes::LOGICAL_ERROR);
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
        .onSuccess([&]()
        {
            binding_created = true;
        })
        .onError([&](const char * message)
        {
            throw Exception("Unable to bind exchange. Reason: " + std::string(message), ErrorCodes::LOGICAL_ERROR);
        });
    }
    else if (exchange_type == AMQP::ExchangeType::fanout || exchange_type == AMQP::ExchangeType::consistent_hash)
    {
        setup_channel->bindExchange(exchange_name, bridge_exchange, routing_keys[0])
        .onSuccess([&]()
        {
            binding_created = true;
        })
        .onError([&](const char * message)
        {
            throw Exception("Unable to bind exchange. Reason: " + std::string(message), ErrorCodes::LOGICAL_ERROR);
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
                throw Exception("Unable to bind exchange. Reason: " + std::string(message), ErrorCodes::LOGICAL_ERROR);
            });
        }
    }

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
        heartbeat_task->deactivate();
        connection->close(); /// Connection might be unusable, but not closed

        /* Connection is not closed immediately (firstly, all pending operations are completed, and then
         * an AMQP closing-handshake is  performed). But cannot open a new connection untill previous one is properly closed
         */
        while (!connection->closed() && ++cnt_retries != RETRIES_MAX)
            event_handler->iterateLoop();

        /// This will force immediate closure if not yet closed
        if (!connection->closed())
            connection->close(true);

        LOG_TRACE(log, "Trying to restore consumer connection");
    }

    connection = std::make_shared<AMQP::TcpConnection>(event_handler.get(),
            AMQP::Address(parsed_address.first, parsed_address.second, AMQP::Login(login_password.first, login_password.second), "/"));

    cnt_retries = 0;
    while (!connection->ready() && ++cnt_retries != RETRIES_MAX)
    {
        event_handler->iterateLoop();
        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }

    return event_handler->connectionRunning();
}


void StorageRabbitMQ::updateChannel(ChannelPtr & channel)
{
    channel = std::make_shared<AMQP::TcpChannel>(connection.get());
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
     * bindings to remove redunadant message copies, but after that mv cannot work unless thoso bindings recreated. Recreating them is not
     * difficult but very ugly and as probably nobody will do such thing - bindings will not be recreated.
     */
    std::call_once(flag, [&]()
    {
        heartbeat_task->deactivate();
        streaming_task->deactivate();
        event_handler->updateLoopState(Loop::STOP);
        looping_task->deactivate();

        setup_channel->removeExchange(bridge_exchange)
        .onSuccess([&]()
        {
            exchange_removed.store(true);
        })
        .onError([&](const char * message)
        {
            throw Exception("Unable to remove exchange. Reason: " + std::string(message), ErrorCodes::CANNOT_CONNECT_RABBITMQ);
        });

        while (!exchange_removed.load())
        {
            event_handler->iterateLoop();
        }
    });
}


Pipe StorageRabbitMQ::read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & /* query_info */,
        const Context & context,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        unsigned /* num_streams */)
{
    if (num_created_consumers == 0)
        return {};

    auto sample_block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());

    auto new_context = std::make_shared<Context>(context);
    if (!schema_name.empty())
        new_context->setSetting("format_schema", schema_name);

    bool update_channels = false;
    if (!event_handler->connectionRunning())
    {
        if (event_handler->loopRunning())
        {
            event_handler->updateLoopState(Loop::STOP);
            looping_task->deactivate();
        }

        if ((update_channels = restoreConnection(true)))
            heartbeat_task->scheduleAfter(HEARTBEAT_RESCHEDULE_MS);
    }

    Pipes pipes;
    pipes.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto rabbit_stream = std::make_shared<RabbitMQBlockInputStream>(*this, metadata_snapshot, new_context, column_names);

        /* It is a possible but rare case when channel gets into error state and does not also close connection, so need manual update.
         * But I believe that in current context and with local rabbitmq settings this will never happen and any channel error will also
         * close connection, but checking anyway (in second condition of if statement). This must be done here (and also in streamToViews())
         * and not in readPrefix as it requires to stop heartbeats and looping tasks to avoid race conditions inside the library
         */
        if (update_channels || rabbit_stream->needManualChannelUpdate())
        {
            if (event_handler->loopRunning())
            {
                event_handler->updateLoopState(Loop::STOP);
                looping_task->deactivate();
                heartbeat_task->deactivate();
            }

            rabbit_stream->updateChannel();
        }

        auto converting_stream = std::make_shared<ConvertingBlockInputStream>(
            rabbit_stream, sample_block, ConvertingBlockInputStream::MatchColumnsMode::Name);
        pipes.emplace_back(std::make_shared<SourceFromInputStream>(converting_stream));
    }

    if (!event_handler->loopRunning() && event_handler->connectionRunning())
        looping_task->activateAndSchedule();

    LOG_DEBUG(log, "Starting reading {} streams", pipes.size());
    return Pipe::unitePipes(std::move(pipes));
}


BlockOutputStreamPtr StorageRabbitMQ::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, const Context & context)
{
    return std::make_shared<RabbitMQBlockOutputStream>(*this, metadata_snapshot, context);
}


void StorageRabbitMQ::startup()
{
    setup_channel = std::make_shared<AMQP::TcpChannel>(connection.get());
    initExchange();
    bindExchange();

    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            pushReadBuffer(createReadBuffer());
            ++num_created_consumers;
        }
        catch (const AMQP::Exception & e)
        {
            std::cerr << e.what();
            throw;
        }
    }

    event_handler->updateLoopState(Loop::RUN);
    streaming_task->activateAndSchedule();
    heartbeat_task->activateAndSchedule();
}


void StorageRabbitMQ::shutdown()
{
    stream_cancelled = true;
    wait_confirm.store(false);

    streaming_task->deactivate();
    heartbeat_task->deactivate();

    event_handler->updateLoopState(Loop::STOP);
    looping_task->deactivate();

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
    std::lock_guard lock(mutex);
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
    std::lock_guard lock(mutex);
    auto buffer = buffers.back();
    buffers.pop_back();

    return buffer;
}


ConsumerBufferPtr StorageRabbitMQ::createReadBuffer()
{
    ChannelPtr consumer_channel = std::make_shared<AMQP::TcpChannel>(connection.get());

    return std::make_shared<ReadBufferFromRabbitMQConsumer>(
        consumer_channel, setup_channel, event_handler, consumer_exchange, ++consumer_id,
        unique_strbase, queue_base, log, row_delimiter, hash_exchange, num_queues,
        deadletter_exchange, stream_cancelled);
}


ProducerBufferPtr StorageRabbitMQ::createWriteBuffer()
{
    return std::make_shared<WriteBufferToRabbitMQProducer>(
        parsed_address, global_context, login_password, routing_keys, exchange_name, exchange_type,
        producer_id.fetch_add(1), unique_strbase, persistent, wait_confirm, log,
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


void StorageRabbitMQ::threadFunc()
{
    try
    {
        auto table_id = getStorageID();
        // Check if at least one direct dependency is attached
        size_t dependencies_count = DatabaseCatalog::instance().getDependencies(table_id).size();

        if (dependencies_count)
        {
            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!stream_cancelled && num_created_consumers > 0)
            {
                if (!checkDependencies(table_id))
                    break;

                LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                if (!streamToViews())
                    break;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    /// Wait for attached views
    if (!stream_cancelled)
        streaming_task->schedule();
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

    auto rabbitmq_context = std::make_shared<Context>(global_context);
    rabbitmq_context->makeQueryContext();
    if (!schema_name.empty())
        rabbitmq_context->setSetting("format_schema", schema_name);

    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, *rabbitmq_context, false, true, true);
    auto block_io = interpreter.execute();

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto column_names = block_io.out->getHeader().getNames();
    auto sample_block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());

    if (!event_handler->loopRunning() && event_handler->connectionRunning())
        looping_task->activateAndSchedule();

    // Create a stream for each consumer and join them in a union stream
    BlockInputStreams streams;
    streams.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto stream = std::make_shared<RabbitMQBlockInputStream>(*this, metadata_snapshot, rabbitmq_context, column_names, false);
        streams.emplace_back(stream);

        // Limit read batch to maximum block size to allow DDL
        IBlockInputStream::LocalLimits limits;

        limits.speed_limits.max_execution_time = global_context.getSettingsRef().stream_flush_interval_ms;
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
    copyData(*in, *block_io.out, &stub);

    /* Need to stop loop even if connection is ok, because sending ack() with loop running in another thread will lead to a lot of data
     * races inside the library, but only in case any error occurs or connection is lost while ack is being sent
     */
    if (event_handler->loopRunning())
    {
        event_handler->updateLoopState(Loop::STOP);
        looping_task->deactivate();
    }

    if (!event_handler->connectionRunning())
    {
        if (restoreConnection(true))
        {
            for (auto & stream : streams)
                stream->as<RabbitMQBlockInputStream>()->updateChannel();

        }
        else
        {
            /// Reschedule if unable to connect to rabbitmq
            return false;
        }
    }
    else
    {
        heartbeat_task->deactivate();

        /// Commit
        for (auto & stream : streams)
        {
            if (!stream->as<RabbitMQBlockInputStream>()->sendAck())
            {
                /* Almost any error with channel will lead to connection closure, but if so happens that channel errored and connection
                 * is not closed - also need to restore channels
                 */
                if (!stream->as<RabbitMQBlockInputStream>()->needManualChannelUpdate())
                    stream->as<RabbitMQBlockInputStream>()->updateChannel();
                else
                    break;
            }
        }
    }

    event_handler->updateLoopState(Loop::RUN);
    looping_task->activateAndSchedule();
    heartbeat_task->scheduleAfter(HEARTBEAT_RESCHEDULE_MS); /// It is also deactivated in restoreConnection(), so reschedule anyway

    // Check whether the limits were applied during query execution
    bool limits_applied = false;
    const BlockStreamProfileInfo & info = in->getProfileInfo();
    limits_applied = info.hasAppliedLimit();

    return limits_applied;
}


void registerStorageRabbitMQ(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();
        bool has_settings = args.storage_def->settings;

        RabbitMQSettings rabbitmq_settings;
        if (has_settings)
        {
            rabbitmq_settings.loadFromQuery(*args.storage_def);
        }

        String host_port = rabbitmq_settings.rabbitmq_host_port;
        if (args_count >= 1)
        {
            const auto * ast = engine_args[0]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                host_port = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception(String("RabbitMQ host:port must be a string"), ErrorCodes::BAD_ARGUMENTS);
            }
        }

        String routing_key_list = rabbitmq_settings.rabbitmq_routing_key_list.value;
        if (args_count >= 2)
        {
            engine_args[1] = evaluateConstantExpressionAsLiteral(engine_args[1], args.local_context);
            routing_key_list = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        }

        Names routing_keys;
        boost::split(routing_keys, routing_key_list, [](char c){ return c == ','; });
        for (String & key : routing_keys)
        {
            boost::trim(key);
        }

        String exchange = rabbitmq_settings.rabbitmq_exchange_name.value;
        if (args_count >= 3)
        {
            engine_args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[2], args.local_context);

            const auto * ast = engine_args[2]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                exchange = safeGet<String>(ast->value);
            }
        }

        String format = rabbitmq_settings.rabbitmq_format.value;
        if (args_count >= 4)
        {
            engine_args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[3], args.local_context);

            const auto * ast = engine_args[3]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                format = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception("Format must be a string", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        char row_delimiter = rabbitmq_settings.rabbitmq_row_delimiter;
        if (args_count >= 5)
        {
            engine_args[4] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[4], args.local_context);

            const auto * ast = engine_args[4]->as<ASTLiteral>();
            String arg;
            if (ast && ast->value.getType() == Field::Types::String)
            {
                arg = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception("Row delimiter must be a char", ErrorCodes::BAD_ARGUMENTS);
            }
            if (arg.size() > 1)
            {
                throw Exception("Row delimiter must be a char", ErrorCodes::BAD_ARGUMENTS);
            }
            else if (arg.empty())
            {
                row_delimiter = '\0';
            }
            else
            {
                row_delimiter = arg[0];
            }
        }

        String schema = rabbitmq_settings.rabbitmq_schema.value;
        if (args_count >= 6)
        {
            engine_args[5] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[5], args.local_context);

            const auto * ast = engine_args[5]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                schema = safeGet<String>(ast->value);
            }
            else
            {
                throw Exception("Format schema must be a string", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        String exchange_type = rabbitmq_settings.rabbitmq_exchange_type.value;
        if (args_count >= 7)
        {
            engine_args[6] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[6], args.local_context);

            const auto * ast = engine_args[6]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                exchange_type = safeGet<String>(ast->value);
            }
        }

        UInt64 num_consumers = rabbitmq_settings.rabbitmq_num_consumers;
        if (args_count >= 8)
        {
            const auto * ast = engine_args[7]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                num_consumers = safeGet<UInt64>(ast->value);
            }
            else
            {
                throw Exception("Number of consumers must be a positive integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        UInt64 num_queues = rabbitmq_settings.rabbitmq_num_queues;
        if (args_count >= 9)
        {
            const auto * ast = engine_args[8]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                num_consumers = safeGet<UInt64>(ast->value);
            }
            else
            {
                throw Exception("Number of queues must be a positive integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        String queue_base = rabbitmq_settings.rabbitmq_queue_base.value;
        if (args_count >= 10)
        {
            engine_args[9] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[9], args.local_context);

            const auto * ast = engine_args[9]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                queue_base = safeGet<String>(ast->value);
            }
        }

        String deadletter_exchange = rabbitmq_settings.rabbitmq_deadletter_exchange.value;
        if (args_count >= 11)
        {
            engine_args[10] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[10], args.local_context);

            const auto * ast = engine_args[10]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                deadletter_exchange = safeGet<String>(ast->value);
            }
        }

        bool persistent = static_cast<bool>(rabbitmq_settings.rabbitmq_persistent_mode);
        if (args_count >= 12)
        {
            const auto * ast = engine_args[11]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                persistent = static_cast<bool>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Transactional channel parameter is a bool", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        return StorageRabbitMQ::create(
                args.table_id, args.context, args.columns,
                host_port, routing_keys, exchange, format, row_delimiter, schema, exchange_type, num_consumers,
                num_queues, queue_base, deadletter_exchange, persistent);
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
            {"_message_id", std::make_shared<DataTypeString>()}
    };
}

}
