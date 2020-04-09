#include <Storages/RabbitMQ/StorageRabbitMQ.h>

#include <DataStreams/IBlockInputStream.h>
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
#include <amqpcpp/linux_tcp.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_READ_FROM_ISTREAM;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNSUPPORTED_METHOD;
    extern const int UNKNOWN_SETTING;
    extern const int READONLY_SETTING;
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
        size_t num_consumers_,
        UInt64 max_block_size_,
        size_t skip_broken_)
        : IStorage(table_id_,
                   ColumnsDescription({
                                              {"_exchange", std::make_shared<DataTypeString>()},
                                              {"_routingKey", std::make_shared<DataTypeString>()},
                                              {"_deliveryTag", std::make_shared<DataTypeString>()}
                                      }, true))
        , global_context(context_.getGlobalContext())
        , host_port(global_context.getMacros()->expand(host_port_))
        , routing_keys(global_context.getMacros()->expand(routing_keys_))
        , exchange_name(exchange_name_)
        , format_name(global_context.getMacros()->expand(format_name_))
        , row_delimiter(row_delimiter_)
        , num_consumers(num_consumers_)
        , max_block_size(max_block_size_)
        , skip_broken(skip_broken_)
        , log(&Logger::get("StorageRabbitMQ (" + table_id_.table_name + ")"))
        , semaphore(0, num_consumers_)
        , connection_handler(log)
        , address(parseAddress(host_port, 15672).first, parseAddress(host_port, 15672).second, AMQP::Login(), "/")
        , connection(&connection_handler, address)
{
    setColumns(columns_);
    task = global_context.getSchedulePool().createTask(log->name(), [this]{ threadFunc(); });
    task->deactivate();

    if (num_consumers == 1)
    {
        consumer_channel = std::make_shared<AMQP::TcpChannel>(&connection);
        consumer_channel->onReady([&]()
                    {
                        LOG_TRACE(log, "Created consumer channel.");
                    });
        initQueues(consumer_channel, routing_keys[0]);
    }

    LOG_TRACE(log, "connection ready? - " + std::to_string(connection.ready()));
}

/*
 * Messages are pushed to consumers (and not pulled by consumers), so if there is no queue binding from exchange
 * to any queue with a routing key of the message, then the message is not routed anywhere from the exchange and lost.
 */
void StorageRabbitMQ::initQueues(ChannelPtr channel_, String routing_key)
{
    LOG_DEBUG(log, "Binding queues.");

    channel_->declareExchange(exchange_name, AMQP::direct)
        .onSuccess([&]()
                {
                    LOG_TRACE(log, "Exchange declared");
                });
    channel_->declareQueue(AMQP::exclusive)
        .onSuccess([&](const std::string & queue_name, int /*msgcount*/, int /*consumercount*/)
        {
            LOG_TRACE(log, "Queue declared with the binding key - "+ routing_key);
            channel_->bindQueue(exchange_name, queue_name, routing_key);
        });
}


Pipes StorageRabbitMQ::read(
        const Names & column_names,
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

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::make_shared<RabbitMQBlockInputStream>(*this, context, column_names, 1, log)));
    }

    LOG_DEBUG(log, "Starting reading " << pipes.size() << " streams");
    return pipes;
}

BlockOutputStreamPtr StorageRabbitMQ::write(const ASTPtr &, const Context & context)
{
    return std::make_shared<RabbitMQBlockOutputStream>(*this, context);
}

void StorageRabbitMQ::startup()
{
    LOG_DEBUG(log, "startup.");

    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            pushReadBuffer(createReadBuffer());
            ++num_created_consumers;
        }
        catch (const AMQP::Exception &)
        {
            tryLogCurrentException(log);
        }
    }

    task->activateAndSchedule();
}

void StorageRabbitMQ::shutdown()
{
    LOG_DEBUG(log, "shutdown.");
    stream_cancelled = true;

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        LOG_DEBUG(log, "poping read buffer.");
        auto buffer = popReadBuffer();
    }

    task->deactivate();
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
        LOG_DEBUG(log, ":(");
        if (!semaphore.tryWait(timeout.count()))
            return nullptr;
    }

    // Take the first available buffer from the list
    std::lock_guard lock(mutex);
    auto buffer = buffers.back();
    buffers.pop_back();

    LOG_DEBUG(log, "Poping read buffer.");
    return buffer;
}


ProducerBufferPtr StorageRabbitMQ::createWriteBuffer()
{
    LOG_DEBUG(log, "CreateWriteBuffer.");

    /// Sharing one channel between publishers
    if (!publishing_channel)
    {
        publishing_channel = std::make_shared<AMQP::TcpChannel>(&connection);
    }

    return std::make_shared<WriteBufferToRabbitMQProducer>(
            publishing_channel, routing_keys[0],
            row_delimiter ? std::optional<char>{row_delimiter} : std::nullopt, 1, 1024);
}


ConsumerBufferPtr StorageRabbitMQ::createReadBuffer()
{
    LOG_DEBUG(log, "CreateReadBuffer.");

    const Settings & settings = global_context.getSettingsRef();
    size_t batch_size = max_block_size;
    if (!batch_size)
        batch_size = settings.max_block_size.value;

    if (num_consumers != 1)
        return std::make_shared<ReadBufferFromRabbitMQConsumer>(
                std::make_shared<AMQP::TcpChannel>(&connection), log, batch_size, stream_cancelled);
    else
        return std::make_shared<ReadBufferFromRabbitMQConsumer>(
                consumer_channel, log, batch_size, stream_cancelled);
}


bool StorageRabbitMQ::checkDependencies(const StorageID & table_id)
{
    LOG_DEBUG(log, "CheckDependencies.");

    // Check if all dependencies are attached
    auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (dependencies.empty())
        return true;

    // Check the dependencies are ready?
    for (const auto & db_tab : dependencies)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(db_tab);
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
    LOG_DEBUG(log, "threadFunc");

    try
    {
        auto table_id = getStorageID();
        // Check if at least one direct dependency is attached
        auto dependencies = DatabaseCatalog::instance().getDependencies(table_id);

        // Keep streaming as long as there are attached views and streaming is not cancelled
        while (!stream_cancelled && num_created_consumers > 0 && dependencies.size() > 0)
        {
            if (!checkDependencies(table_id))
                break;

            LOG_DEBUG(log, "Started streaming to " << dependencies.size() << " attached views");

            if (!streamToViews())
                break;
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    /// Wait for attached views
    if (!stream_cancelled)
        task->scheduleAfter(500);
}


bool StorageRabbitMQ::streamToViews()
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id);
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    const Settings & settings = global_context.getSettingsRef();
    size_t block_size = max_block_size;
    if (block_size == 0)
        block_size = settings.max_block_size;

    InterpreterInsertQuery interpreter(insert, global_context, false, true, true);
    auto block_io = interpreter.execute();

    // Create a stream for each consumer and join them in a union stream
    BlockInputStreams streams;
    streams.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto stream = std::make_shared<RabbitMQBlockInputStream>(
                *this, global_context, block_io.out->getHeader().getNames(), block_size, log, false);
        streams.emplace_back(stream);

        // Limit read batch to maximum block size to allow DDL
        IBlockInputStream::LocalLimits limits;
        limits.speed_limits.max_execution_time = settings.stream_flush_interval_ms;
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

    for (auto & stream : streams)
        stream->as<RabbitMQBlockInputStream>()->commitNotSubscribed(routing_keys);

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

        /** Arguments of engine is following:
              * - RabbitMQ host:port (default: localhost:5672)
              * - List of routing keys to bind producer->exchange->queue <-> consumer (default: "")
              * optional for now:
              *  - echxange name
              * - Number of consumers
              * - Message format (string)
              * - Row delimiter
              * - Max block size for background consumption
              * - Skip (at least) unreadable messages number
              */

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


        UInt64 num_consumers = rabbitmq_settings.rabbitmq_num_consumers;
        if (args_count >= 4)
        {
            const auto * ast = engine_args[3]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                num_consumers = safeGet<UInt64>(ast->value);
            }
            else
            {
                throw Exception("Number of consumers must be a positive integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }


        String format = rabbitmq_settings.rabbitmq_format.value;
        if (args_count >= 5)
        {
            engine_args[4] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[4], args.local_context);

            const auto * ast = engine_args[4]->as<ASTLiteral>();
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
        if (args_count >= 6)
        {
            engine_args[5] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[5], args.local_context);

            const auto * ast = engine_args[5]->as<ASTLiteral>();
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

        UInt64 max_block_size = static_cast<size_t>(rabbitmq_settings.rabbitmq_max_block_size);
        if (args_count >= 7)
        {
            const auto * ast = engine_args[6]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                max_block_size = static_cast<size_t>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Maximum block size must be a positive integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        size_t skip_broken = static_cast<size_t>(rabbitmq_settings.rabbitmq_skip_broken_messages);
        if (args_count >= 8)
        {
            const auto * ast = engine_args[7]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64)
            {
                skip_broken = static_cast<size_t>(safeGet<UInt64>(ast->value));
            }
            else
            {
                throw Exception("Number of broken messages to skip must be a non-negative integer", ErrorCodes::BAD_ARGUMENTS);
            }
        }

        return StorageRabbitMQ::create(
                args.table_id, args.context, args.columns,
                host_port, routing_keys, exchange, 
                format, row_delimiter, num_consumers, max_block_size, skip_broken);
    };

    factory.registerStorage("RabbitMQ", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });

}
}
