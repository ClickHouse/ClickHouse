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
static const auto RETRIES_MAX = 1000;
static const auto HEARTBEAT_RESCHEDULE_MS = 3000;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CONNECT_RABBITMQ;
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
        const String & exchange_type_,
        size_t num_consumers_,
        size_t num_queues_,
        const bool use_transactional_channel_)
        : IStorage(table_id_)
        , global_context(context_.getGlobalContext())
        , rabbitmq_context(Context(global_context))
        , routing_keys(global_context.getMacros()->expand(routing_keys_))
        , exchange_name(exchange_name_)
        , format_name(global_context.getMacros()->expand(format_name_))
        , row_delimiter(row_delimiter_)
        , num_consumers(num_consumers_)
        , num_queues(num_queues_)
        , exchange_type(exchange_type_)
        , use_transactional_channel(use_transactional_channel_)
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
    connection = std::make_shared<AMQP::TcpConnection>(event_handler.get(), AMQP::Address(parsed_address.first, parsed_address.second, AMQP::Login(login_password.first, login_password.second), "/"));

    size_t cnt_retries = 0;
    while (!connection->ready() && ++cnt_retries != RETRIES_MAX)
    {
        event_handler->iterateLoop();
        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }

    if (!connection->ready())
        throw Exception("Cannot set up connection for consumers", ErrorCodes::CANNOT_CONNECT_RABBITMQ);

    rabbitmq_context.makeQueryContext();
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    streaming_task = global_context.getSchedulePool().createTask("RabbitMQStreamingTask", [this]{ threadFunc(); });
    streaming_task->deactivate();
    heartbeat_task = global_context.getSchedulePool().createTask("RabbitMQHeartbeatTask", [this]{ heartbeatFunc(); });
    heartbeat_task->deactivate();

    bind_by_id = num_consumers > 1 || num_queues > 1;

    auto table_id = getStorageID();
    String table_name = table_id.table_name;

    /// Make sure that local exchange name is unique for each table and is not the same as client's exchange name
    local_exchange_name = exchange_name + "_" + table_name;

    /// One looping task for all consumers as they share the same connection == the same handler == the same event loop
    looping_task = global_context.getSchedulePool().createTask("RabbitMQLoopingTask", [this]{ loopingFunc(); });
    looping_task->deactivate();
}


void StorageRabbitMQ::heartbeatFunc()
{
    if (!stream_cancelled)
    {
        LOG_TRACE(log, "Sending RabbitMQ heartbeat");
        connection->heartbeat();
        heartbeat_task->scheduleAfter(HEARTBEAT_RESCHEDULE_MS);
    }
}


void StorageRabbitMQ::loopingFunc()
{
    LOG_DEBUG(log, "Starting event looping iterations");
    event_handler->startLoop();
}


Pipes StorageRabbitMQ::read(
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

    Pipes pipes;
    pipes.reserve(num_created_consumers);

    auto sample_block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto rabbit_stream = std::make_shared<RabbitMQBlockInputStream>(
            *this, metadata_snapshot, context, column_names);
        auto converting_stream = std::make_shared<ConvertingBlockInputStream>(
            rabbit_stream, sample_block, ConvertingBlockInputStream::MatchColumnsMode::Name);
        pipes.emplace_back(std::make_shared<SourceFromInputStream>(converting_stream));
    }

    if (!loop_started)
    {
        loop_started = true;
        looping_task->activateAndSchedule();
    }

    LOG_DEBUG(log, "Starting reading {} streams", pipes.size());
    return pipes;
}


BlockOutputStreamPtr StorageRabbitMQ::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, const Context & context)
{
    return std::make_shared<RabbitMQBlockOutputStream>(*this, metadata_snapshot, context);
}


void StorageRabbitMQ::startup()
{
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

    streaming_task->activateAndSchedule();
    heartbeat_task->activateAndSchedule();
}


void StorageRabbitMQ::shutdown()
{
    stream_cancelled = true;

    event_handler->stop();

    looping_task->deactivate();
    streaming_task->deactivate();
    heartbeat_task->deactivate();

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        popReadBuffer();
    }

    connection->close();
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
    if (update_channel_id)
        next_channel_id += num_queues;
    update_channel_id = true;

    ChannelPtr consumer_channel = std::make_shared<AMQP::TcpChannel>(connection.get());

    return std::make_shared<ReadBufferFromRabbitMQConsumer>(
        consumer_channel, event_handler, exchange_name, routing_keys,
        next_channel_id, log, row_delimiter, bind_by_id, num_queues,
        exchange_type, local_exchange_name, stream_cancelled);
}


ProducerBufferPtr StorageRabbitMQ::createWriteBuffer()
{
    return std::make_shared<WriteBufferToRabbitMQProducer>(
        parsed_address, global_context, login_password, routing_keys[0], local_exchange_name,
        log, num_consumers * num_queues, bind_by_id, use_transactional_channel,
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

    InterpreterInsertQuery interpreter(insert, rabbitmq_context, false, true, true);
    auto block_io = interpreter.execute();

    // Create a stream for each consumer and join them in a union stream
    BlockInputStreams streams;
    streams.reserve(num_created_consumers);

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto column_names = block_io.out->getHeader().getNames();
    auto sample_block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto rabbit_stream = std::make_shared<RabbitMQBlockInputStream>(*this, metadata_snapshot, rabbitmq_context, column_names);
        auto converting_stream = std::make_shared<ConvertingBlockInputStream>(rabbit_stream, sample_block, ConvertingBlockInputStream::MatchColumnsMode::Name);

        streams.emplace_back(converting_stream);

        // Limit read batch to maximum block size to allow DDL
        IBlockInputStream::LocalLimits limits;
        const Settings & settings = global_context.getSettingsRef();
        limits.speed_limits.max_execution_time = settings.stream_flush_interval_ms;
        limits.timeout_overflow_mode = OverflowMode::BREAK;
        rabbit_stream->setLimits(limits);
    }

    if (!loop_started)
    {
        loop_started = true;
        looping_task->activateAndSchedule();
    }

    // Join multiple streams if necessary
    BlockInputStreamPtr in;
    if (streams.size() > 1)
        in = std::make_shared<UnionBlockInputStream>(streams, nullptr, streams.size());
    else
        in = streams[0];

    std::atomic<bool> stub = {false};
    copyData(*in, *block_io.out, &stub);

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

        String exchange_type = rabbitmq_settings.rabbitmq_exchange_type.value;
        if (args_count >= 6)
        {
            engine_args[5] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[5], args.local_context);

            const auto * ast = engine_args[5]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::String)
            {
                exchange_type = safeGet<String>(ast->value);
            }

            if (exchange_type != "fanout" && exchange_type != "direct" && exchange_type != "topic"
                    && exchange_type != "headers" && exchange_type != "consistent_hash")
                throw Exception("Invalid exchange type", ErrorCodes::BAD_ARGUMENTS);
        }

        UInt64 num_consumers = rabbitmq_settings.rabbitmq_num_consumers;
        if (args_count >= 7)
        {
            const auto * ast = engine_args[6]->as<ASTLiteral>();
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
        if (args_count >= 8)
        {
            const auto * ast = engine_args[7]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                num_consumers = safeGet<UInt64>(ast->value);
            }
            else
            {
                throw Exception("Number of queues must be a positive integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        bool use_transactional_channel = static_cast<bool>(rabbitmq_settings.rabbitmq_transactional_channel);
        if (args_count >= 9)
        {
            const auto * ast = engine_args[8]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                use_transactional_channel = static_cast<bool>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Transactional channel parameter is a bool", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        return StorageRabbitMQ::create(
                args.table_id, args.context, args.columns,
                host_port, routing_keys, exchange, format, row_delimiter, exchange_type, num_consumers,
                num_queues, use_transactional_channel);
    };

    factory.registerStorage("RabbitMQ", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });

}


NamesAndTypesList StorageRabbitMQ::getVirtuals() const
{
    return NamesAndTypesList{
            {"_exchange", std::make_shared<DataTypeString>()}
    };
}

}
