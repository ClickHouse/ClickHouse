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
#include <Storages/NATS/NATSSink.h>
#include <Storages/NATS/NATSSource.h>
#include <Storages/NATS/StorageNATS.h>
#include <Storages/NATS/WriteBufferToNATSProducer.h>
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
#include <base/logger_useful.h>

//static void
//onMsg(natsConnection *, natsSubscription *, natsMsg *msg, void *)
//{
//        printf("\n\n\nReceived msg: %s - %.*s\n\n\n",
//               natsMsg_GetSubject(msg),
//               natsMsg_GetDataLength(msg),
//               natsMsg_GetData(msg));
//        fflush(stdout);
//
//    natsMsg_Destroy(msg);
//}

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


StorageNATS::StorageNATS(
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<NATSSettings> nats_settings_,
        bool is_attach_)
        : IStorage(table_id_)
        , WithContext(context_->getGlobalContext())
        , nats_settings(std::move(nats_settings_))
        , subjects(parseSubjects(getContext()->getMacros()->expand(nats_settings->nats_subjects)))
        , format_name(getContext()->getMacros()->expand(nats_settings->nats_format))
        , row_delimiter(nats_settings->nats_row_delimiter.value)
        , schema_name(getContext()->getMacros()->expand(nats_settings->nats_schema))
        , num_consumers(nats_settings->nats_num_consumers.value)
        , persistent(nats_settings->nats_persistent.value)
//        , use_user_setup(nats_settings->nats_queue_consume.value)
        , log(&Poco::Logger::get("StorageNATS (" + table_id_.table_name + ")"))
        , semaphore(0, num_consumers)
        , unique_strbase(getRandomName())
        , queue_size(std::max(QUEUE_SIZE, static_cast<uint32_t>(getMaxBlockSize())))
        , milliseconds_to_wait(RESCHEDULE_MS)
        , is_attach(is_attach_)
{
    auto parsed_address = parseAddress(getContext()->getMacros()->expand(nats_settings->nats_host_port), 5672);
    context_->getRemoteHostFilter().checkHostAndPort(parsed_address.first, toString(parsed_address.second));

    auto nats_username = nats_settings->nats_username.value;
    auto nats_password = nats_settings->nats_password.value;
    configuration =
    {
        .host = parsed_address.first,
        .port = parsed_address.second,
        .username = nats_username.empty() ? getContext()->getConfigRef().getString("nats.username") : nats_username,
        .password = nats_password.empty() ? getContext()->getConfigRef().getString("nats.password") : nats_password,
        .vhost = getContext()->getConfigRef().getString("nats.vhost", getContext()->getMacros()->expand(nats_settings->nats_vhost)),
        .secure = nats_settings->nats_secure.value,
        .connection_string = getContext()->getMacros()->expand(nats_settings->nats_address)
    };

    if (configuration.secure)
        SSL_library_init();

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    nats_context = addSettings(getContext());
    nats_context->makeQueryContext();
//
//    if (queue_base.empty())
//    {
//        /* Make sure that local exchange name is unique for each table and is not the same as client's exchange name. It also needs to
//         * be table-based and not just a random string, because local exchanges should be declared the same for same tables
//         */
//        sharding_exchange = getTableBasedName(exchange_name, table_id_);
//
//        /* By default without a specified queue name in queue's declaration - its name will be generated by the library, but its better
//         * to specify it unique for each table to reuse them once the table is recreated. So it means that queues remain the same for every
//         * table unless queue_base table setting is specified (which allows to register consumers to specific queues). Now this is a base
//         * for the names of later declared queues
//         */
//        queue_base = getTableBasedName("", table_id_);
//    }
//    else
//    {
//        /* In case different tables are used to register multiple consumers to the same queues (so queues are shared between tables) and
//         * at the same time sharding exchange is needed (if there are multiple shared queues), then those tables also need to share
//         * sharding exchange and bridge exchange
//         */
//        sharding_exchange = exchange_name + "_" + queue_base;
//    }
//
//    bridge_exchange = sharding_exchange + "_bridge";

    try
    {
        connection = std::make_shared<NATSConnectionManager>(configuration, log);
        if (!connection->connect() && !is_attach)
            throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot connect to {}", connection->connectionInfoForLog());
//        if (connection->connect())
//            initNATS();
//        else if (!is_attach)
//            throw Exception(ErrorCodes::CANNOT_CONNECT_RABBITMQ, "Cannot connect to {}", connection->connectionInfoForLog());
//                             auto sub = connection->createSubscription("foo", onMsg, nullptr);
//                             int64_t n;
//                             connection->getHandler().startBlockingLoop();
//                             while (true) {
//                                 natsSubscription_GetDelivered(sub.get(), &n);
//                                 printf("Read n : %ld\n", n);
//                                 fflush(stdout);
//                                 std::this_thread::sleep_for(std::chrono::milliseconds(500));
//                             }
//        auto t2 = std::thread([this](){
//                                  connection->getHandler().updateLoopState(Loop::RUN);
//                                  LOG_DEBUG(log, "Storteng lup");
//                                  connection->getHandler().startLoop();
//        });
//        t2.join();
    }
    catch (...)
    {
        tryLogCurrentException(log);
        if (!is_attach)
            throw;
    }

    /// One looping task for all consumers as they share the same connection == the same handler == the same event loop
    looping_task = getContext()->getMessageBrokerSchedulePool().createTask("NATSLoopingTask", [this]{ loopingFunc(); });
    looping_task->deactivate();

    streaming_task = getContext()->getMessageBrokerSchedulePool().createTask("NATSStreamingTask", [this]{ streamingToViewsFunc(); });
    streaming_task->deactivate();

    connection_task = getContext()->getMessageBrokerSchedulePool().createTask("NATSConnectionManagerTask", [this]{ connectionFunc(); });
    connection_task->deactivate();
}


Names StorageNATS::parseSubjects(String subjects_list)
{
    Names result;
    if (subjects_list.empty())
        return result;
    boost::split(result, subjects_list, [](char c){ return c == ','; });
    for (String & key : result)
        boost::trim(key);

    return result;
}


String StorageNATS::getTableBasedName(String name, const StorageID & table_id)
{
    if (name.empty())
        return fmt::format("{}_{}", table_id.database_name, table_id.table_name);
    else
        return fmt::format("{}_{}_{}", name, table_id.database_name, table_id.table_name);
}


ContextMutablePtr StorageNATS::addSettings(ContextPtr local_context) const
{
    auto modified_context = Context::createCopy(local_context);
    modified_context->setSetting("input_format_skip_unknown_fields", true);
    modified_context->setSetting("input_format_allow_errors_ratio", 0.);
    modified_context->setSetting("input_format_allow_errors_num", nats_settings->nats_skip_broken_messages.value);

    if (!schema_name.empty())
        modified_context->setSetting("format_schema", schema_name);

    for (const auto & setting : *nats_settings)
    {
        const auto & setting_name = setting.getName();

        /// check for non-nats-related settings
        if (!setting_name.starts_with("nats_"))
            modified_context->setSetting(setting_name, setting.getValue());
    }

    return modified_context;
}


void StorageNATS::loopingFunc()
{
    connection->getHandler().startLoop();
}


void StorageNATS::stopLoop()
{
    connection->getHandler().updateLoopState(Loop::STOP);
}

void StorageNATS::stopLoopIfNoReaders()
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

void StorageNATS::startLoop()
{
//    assert(nats_is_ready);
    connection->getHandler().updateLoopState(Loop::RUN);
    looping_task->activateAndSchedule();
}


void StorageNATS::incrementReader()
{
    ++readers_count;
}


void StorageNATS::decrementReader()
{
    --readers_count;
}


void StorageNATS::connectionFunc()
{
//    if (nats_is_ready)
//        return;

    if (!connection->reconnect())
        connection_task->scheduleAfter(RESCHEDULE_MS);
//    if (connection->reconnect())
//        initNATS();
//    else
//        connection_task->scheduleAfter(RESCHEDULE_MS);
}


/* Need to deactivate this way because otherwise might get a deadlock when first deactivate streaming task in shutdown and then
 * inside streaming task try to deactivate any other task
 */
void StorageNATS::deactivateTask(BackgroundSchedulePool::TaskHolder & task, bool wait, bool stop_loop)
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


size_t StorageNATS::getMaxBlockSize() const
{
     return nats_settings->nats_max_block_size.changed
         ? nats_settings->nats_max_block_size.value
         : (getContext()->getSettingsRef().max_insert_block_size.value / num_consumers);
}


//void StorageNATS::initNATS()
//{
//    if (shutdown_called || nats_is_ready)
//        return;
//
//    if (use_user_setup)
//    {
//        queues.emplace_back(queue_base);
//        nats_is_ready = true;
//        return;
//    }
//
//    try
//    {
//        auto nats_channel = connection->createChannel();
//
//        /// Main exchange -> Bridge exchange -> ( Sharding exchange ) -> Queues -> Consumers
//
//        initExchange(*nats_channel);
//        bindExchange(*nats_channel);
//
//        for (const auto i : collections::range(0, num_queues))
//            bindQueue(i + 1, *nats_channel);
//
//        LOG_TRACE(log, "NATS setup completed");
//        nats_is_ready = true;
//        nats_channel->close();
//    }
//    catch (...)
//    {
//        tryLogCurrentException(log);
//        if (!is_attach)
//            throw;
//    }
//}
//
//
//void StorageNATS::initExchange(AMQP::TcpChannel & nats_channel)
//{
//    /// Exchange hierarchy:
//    /// 1. Main exchange (defined with table settings - nats_exchange_name, nats_exchange_type).
//    /// 2. Bridge exchange (fanout). Used to easily disconnect main exchange and to simplify queue bindings.
//    /// 3. Sharding (or hash) exchange. Used in case of multiple queues.
//    /// 4. Consumer exchange. Just an alias for bridge_exchange or sharding exchange to know to what exchange
//    ///    queues will be bound.
//
//    /// All exchanges are declared with options:
//    /// 1. `durable` (survive NATS server restart)
//    /// 2. `autodelete` (auto delete in case of queue bindings are dropped).
//
//    nats_channel.declareExchange(exchange_name, exchange_type, AMQP::durable)
//    .onError([&](const char * message)
//    {
//        /// This error can be a result of attempt to declare exchange if it was already declared but
//        /// 1) with different exchange type.
//        /// 2) with different exchange settings.
//        throw Exception("Unable to declare exchange. Make sure specified exchange is not already declared. Error: "
//                + std::string(message), ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE);
//    });
//
//    nats_channel.declareExchange(bridge_exchange, AMQP::fanout, AMQP::durable | AMQP::autodelete)
//    .onError([&](const char * message)
//    {
//        /// This error is not supposed to happen as this exchange name is always unique to type and its settings.
//        throw Exception(
//            ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE, "Unable to declare bridge exchange ({}). Reason: {}", bridge_exchange, std::string(message));
//    });
//
//    if (!hash_exchange)
//    {
//        consumer_exchange = bridge_exchange;
//        return;
//    }
//
//    AMQP::Table binding_arguments;
//
//    /// Default routing key property in case of hash exchange is a routing key, which is required to be an integer.
//    /// Support for arbitrary exchange type (i.e. arbitrary pattern of routing keys) requires to eliminate this dependency.
//    /// This settings changes hash property to message_id.
//    binding_arguments["hash-property"] = "message_id";
//
//    /// Declare hash exchange for sharding.
//    nats_channel.declareExchange(sharding_exchange, AMQP::consistent_hash, AMQP::durable | AMQP::autodelete, binding_arguments)
//    .onError([&](const char * message)
//    {
//        /// This error can be a result of same reasons as above for exchange_name, i.e. it will mean that sharding exchange name appeared
//        /// to be the same as some other exchange (which purpose is not for sharding). So probably actual error reason: queue_base parameter
//        /// is bad.
//        throw Exception(
//           ErrorCodes::CANNOT_DECLARE_RABBITMQ_EXCHANGE,
//           "Unable to declare sharding exchange ({}). Reason: {}", sharding_exchange, std::string(message));
//    });
//
//    nats_channel.bindExchange(bridge_exchange, sharding_exchange, routing_keys[0])
//    .onError([&](const char * message)
//    {
//        throw Exception(
//            ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE,
//            "Unable to bind bridge exchange ({}) to sharding exchange ({}). Reason: {}",
//            bridge_exchange,
//            sharding_exchange,
//            std::string(message));
//    });
//
//    consumer_exchange = sharding_exchange;
//}
//
//
//void StorageNATS::bindExchange(AMQP::TcpChannel & nats_channel)
//{
//    size_t bound_keys = 0;
//
//    if (exchange_type == AMQP::ExchangeType::headers)
//    {
//        AMQP::Table bind_headers;
//        for (const auto & header : routing_keys)
//        {
//            std::vector<String> matching;
//            boost::split(matching, header, [](char c){ return c == '='; });
//            bind_headers[matching[0]] = matching[1];
//        }
//
//        nats_channel.bindExchange(exchange_name, bridge_exchange, routing_keys[0], bind_headers)
//        .onSuccess([&]() { connection->getHandler().stopLoop(); })
//        .onError([&](const char * message)
//        {
//            throw Exception(
//                ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE,
//                "Unable to bind exchange {} to bridge exchange ({}). Reason: {}",
//                exchange_name, bridge_exchange, std::string(message));
//        });
//    }
//    else if (exchange_type == AMQP::ExchangeType::fanout || exchange_type == AMQP::ExchangeType::consistent_hash)
//    {
//        nats_channel.bindExchange(exchange_name, bridge_exchange, routing_keys[0])
//        .onSuccess([&]() { connection->getHandler().stopLoop(); })
//        .onError([&](const char * message)
//        {
//            throw Exception(
//                ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE,
//                "Unable to bind exchange {} to bridge exchange ({}). Reason: {}",
//                exchange_name, bridge_exchange, std::string(message));
//        });
//    }
//    else
//    {
//        for (const auto & routing_key : routing_keys)
//        {
//            nats_channel.bindExchange(exchange_name, bridge_exchange, routing_key)
//            .onSuccess([&]()
//            {
//                ++bound_keys;
//                if (bound_keys == routing_keys.size())
//                    connection->getHandler().stopLoop();
//            })
//            .onError([&](const char * message)
//            {
//                throw Exception(
//                    ErrorCodes::CANNOT_BIND_RABBITMQ_EXCHANGE,
//                    "Unable to bind exchange {} to bridge exchange ({}). Reason: {}",
//                    exchange_name, bridge_exchange, std::string(message));
//            });
//        }
//    }
//
//    connection->getHandler().startBlockingLoop();
//}
//
//
//void StorageNATS::bindQueue(size_t queue_id, AMQP::TcpChannel & nats_channel)
//{
//    auto success_callback = [&](const std::string &  queue_name, int msgcount, int /* consumercount */)
//    {
//        queues.emplace_back(queue_name);
//        LOG_DEBUG(log, "Queue {} is declared", queue_name);
//
//        if (msgcount)
//            LOG_INFO(log, "Queue {} is non-empty. Non-consumed messaged will also be delivered", queue_name);
//
//       /* Here we bind either to sharding exchange (consistent-hash) or to bridge exchange (fanout). All bindings to routing keys are
//        * done between client's exchange and local bridge exchange. Binding key must be a string integer in case of hash exchange, for
//        * fanout exchange it can be arbitrary
//        */
//        nats_channel.bindQueue(consumer_exchange, queue_name, std::to_string(queue_id))
//        .onSuccess([&] { connection->getHandler().stopLoop(); })
//        .onError([&](const char * message)
//        {
//            throw Exception(
//                ErrorCodes::CANNOT_CREATE_RABBITMQ_QUEUE_BINDING,
//                "Failed to create queue binding for exchange {}. Reason: {}", exchange_name, std::string(message));
//        });
//    };
//
//    auto error_callback([&](const char * message)
//    {
//        /* This error is most likely a result of an attempt to declare queue with different settings if it was declared before. So for a
//         * given queue name either deadletter_exchange parameter changed or queue_size changed, i.e. table was declared with different
//         * max_block_size parameter. Solution: client should specify a different queue_base parameter or manually delete previously
//         * declared queues via any of the various cli tools.
//         */
//        throw Exception("Failed to declare queue. Probably queue settings are conflicting: max_block_size, deadletter_exchange. Attempt \
//                specifying differently those settings or use a different queue_base or manually delete previously declared queues, \
//                which  were declared with the same names. ERROR reason: "
//                + std::string(message), ErrorCodes::BAD_ARGUMENTS);
//    });
//
//    AMQP::Table queue_settings;
//
//    std::unordered_set<String> integer_settings = {"x-max-length", "x-max-length-bytes", "x-message-ttl", "x-expires", "x-priority", "x-max-priority"};
//    std::unordered_set<String> string_settings = {"x-overflow", "x-dead-letter-exchange", "x-queue-type"};
//
//    /// Check user-defined settings.
//    if (!queue_settings_list.empty())
//    {
//        for (const auto & setting : queue_settings_list)
//        {
//            Strings setting_values;
//            splitInto<'='>(setting_values, setting);
//            if (setting_values.size() != 2)
//                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid settings string: {}", setting);
//
//            String key = setting_values[0], value = setting_values[1];
//
//            if (integer_settings.contains(key))
//                queue_settings[key] = parse<uint64_t>(value);
//            else if (string_settings.find(key) != string_settings.end())
//                queue_settings[key] = value;
//            else
//                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported queue setting: {}", value);
//        }
//    }
//
//    /// Impose default settings if there are no user-defined settings.
//    if (!queue_settings.contains("x-max-length"))
//    {
//        queue_settings["x-max-length"] = queue_size;
//    }
//    if (!queue_settings.contains("x-overflow"))
//    {
//        queue_settings["x-overflow"] = "reject-publish";
//    }
//
//    /// If queue_base - a single name, then it can be used as one specific queue, from which to read.
//    /// Otherwise it is used as a generator (unique for current table) of queue names, because it allows to
//    /// maximize performance - via setting `nats_num_queues`.
//    const String queue_name = !hash_exchange ? queue_base : std::to_string(queue_id) + "_" + queue_base;
//
//    /// AMQP::autodelete setting is not allowed, because in case of server restart there will be no consumers
//    /// and deleting queues should not take place.
//    nats_channel.declareQueue(queue_name, AMQP::durable, queue_settings).onSuccess(success_callback).onError(error_callback);
//    connection->getHandler().startBlockingLoop();
//}
//
//
//bool StorageNATS::updateChannel(ChannelPtr & channel)
//{
//    try
//    {
//        channel = connection->createChannel();
//        return true;
//    }
//    catch (...)
//    {
//        tryLogCurrentException(log);
//        return false;
//    }
//}
//
//
//void StorageNATS::prepareChannelForBuffer(ConsumerBufferPtr buffer)
//{
//    if (!buffer)
//        return;
//
//    if (buffer->queuesCount() != queues.size())
//        buffer->updateQueues(queues);
//
//    buffer->updateAckTracker();
//
//    if (updateChannel(buffer->getChannel()))
//        buffer->setupChannel();
//}
//
//
//void StorageNATS::unbindExchange()
//{
//    /* This is needed because with NATS (without special adjustments) can't, for example, properly make mv if there was insert query
//     * on the same table before, and in another direction it will make redundant copies, but most likely nobody will do that.
//     * As publishing is done to exchange, publisher never knows to which queues the message will go, every application interested in
//     * consuming from certain exchange - declares its owns exchange-bound queues, messages go to all such exchange-bound queues, and as
//     * input streams are always created at startup, then they will also declare its own exchange bound queues, but they will not be visible
//     * externally - client declares its own exchange-bound queues, from which to consume, so this means that if not disconnecting this local
//     * queues, then messages will go both ways and in one of them they will remain not consumed. So need to disconnect local exchange
//     * bindings to remove redunadant message copies, but after that mv cannot work unless those bindings are recreated. Recreating them is
//     * not difficult but very ugly and as probably nobody will do such thing - bindings will not be recreated.
//     */
//    if (!exchange_removed.exchange(true))
//    {
//        try
//        {
//            streaming_task->deactivate();
//
//            stopLoop();
//            looping_task->deactivate();
//
//            auto nats_channel = connection->createChannel();
//            nats_channel->removeExchange(bridge_exchange)
//            .onSuccess([&]()
//            {
//                connection->getHandler().stopLoop();
//            })
//            .onError([&](const char * message)
//            {
//                throw Exception("Unable to remove exchange. Reason: " + std::string(message), ErrorCodes::CANNOT_REMOVE_RABBITMQ_EXCHANGE);
//            });
//
//            connection->getHandler().startBlockingLoop();
//            nats_channel->close();
//        }
//        catch (...)
//        {
//            exchange_removed = false;
//            throw;
//        }
//    }
//}


Pipe StorageNATS::read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & /* query_info */,
        ContextPtr local_context,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t /* max_block_size */,
        unsigned /* num_streams */)
{
//    if (!nats_is_ready)
//        throw Exception("NATS setup not finished. Connection might be lost", ErrorCodes::CANNOT_CONNECT_RABBITMQ);

    if (num_created_consumers == 0)
        return {};

    if (!local_context->getSettingsRef().stream_like_engine_allow_direct_select)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Direct select is not allowed. To enable use setting `stream_like_engine_allow_direct_select`");

    if (mv_attached)
        throw Exception(ErrorCodes::QUERY_NOT_ALLOWED, "Cannot read from StorageNATS with attached materialized views");

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

//    initializeBuffers();

    Pipes pipes;
    pipes.reserve(num_created_consumers);

    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        auto nats_source = std::make_shared<NATSSource>(
            *this, storage_snapshot, modified_context, column_names, 1, nats_settings->nats_commit_on_select);

        auto converting_dag = ActionsDAG::makeConvertingActions(
            nats_source->getPort().getHeader().getColumnsWithTypeAndName(),
            sample_block.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        auto converting = std::make_shared<ExpressionActions>(std::move(converting_dag));
        auto converting_transform = std::make_shared<ExpressionTransform>(nats_source->getPort().getHeader(), std::move(converting));

        pipes.emplace_back(std::move(nats_source));
        pipes.back().addTransform(std::move(converting_transform));
    }

    if (!connection->getHandler().loopRunning() && connection->isConnected())
        startLoop();

    LOG_DEBUG(log, "Starting reading {} streams", pipes.size());
    auto united_pipe = Pipe::unitePipes(std::move(pipes));
    united_pipe.addInterpreterContext(modified_context);
    return united_pipe;
}


SinkToStoragePtr StorageNATS::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    return std::make_shared<NATSSink>(*this, metadata_snapshot, local_context);
}


void StorageNATS::startup()
{
    if (!nats_is_ready)
    {
        if (connection->isConnected())
        {
            try
            {
//                initNATS();
                LOG_DEBUG(log, "Fake init lul");
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


void StorageNATS::shutdown()
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

//            cleanupNATS();
        }

        /// It is important to close connection here - before removing consumer buffers, because
        /// it will finish and clean callbacks, which might use those buffers data.
        if (connection->getHandler().loopRunning())
            stopLoop();
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
//void StorageNATS::cleanupNATS() const
//{
//    if (use_user_setup)
//        return;
//
//    connection->heartbeat();
//    if (!connection->isConnected())
//    {
//        String queue_names;
//        for (const auto & queue : queues)
//        {
//            if (!queue_names.empty())
//                queue_names += ", ";
//            queue_names += queue;
//        }
//        LOG_WARNING(log,
//                    "NATS clean up not done, because there is no connection in table's shutdown."
//                    "There are {} queues ({}), which might need to be deleted manually. Exchanges will be auto-deleted",
//                    queues.size(), queue_names);
//        return;
//    }
//
//    auto nats_channel = connection->createChannel();
//    for (const auto & queue : queues)
//    {
//        /// AMQP::ifunused is needed, because it is possible to share queues between multiple tables and dropping
//        /// on of them should not affect others.
//        /// AMQP::ifempty is not used on purpose.
//
//        nats_channel->removeQueue(queue, AMQP::ifunused)
//        .onSuccess([&](uint32_t num_messages)
//        {
//            LOG_TRACE(log, "Successfully deleted queue {}, messages contained {}", queue, num_messages);
//            connection->getHandler().stopLoop();
//        })
//        .onError([&](const char * message)
//        {
//            LOG_ERROR(log, "Failed to delete queue {}. Error message: {}", queue, message);
//            connection->getHandler().stopLoop();
//        });
//    }
//    connection->getHandler().startBlockingLoop();
//    nats_channel->close();
//
//    /// Also there is no need to cleanup exchanges as they were created with AMQP::autodelete option. Once queues
//    /// are removed, exchanges will also be cleaned.
//}


void StorageNATS::pushReadBuffer(ConsumerBufferPtr buffer)
{
    std::lock_guard lock(buffers_mutex);
    buffers.push_back(buffer);
    semaphore.set();
}


ConsumerBufferPtr StorageNATS::popReadBuffer()
{
    return popReadBuffer(std::chrono::milliseconds::zero());
}


ConsumerBufferPtr StorageNATS::popReadBuffer(std::chrono::milliseconds timeout)
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


ConsumerBufferPtr StorageNATS::createReadBuffer()
{
    return std::make_shared<ReadBufferFromNATSConsumer>(
        connection->getHandler(), connection, subjects,
        unique_strbase, log, row_delimiter, queue_size, shutdown_called);
}


//ProducerBufferPtr StorageNATS::createWriteBuffer()
//{
//    return std::make_shared<WriteBufferToNATSProducer>(
//        configuration, getContext(), routing_keys, exchange_name, exchange_type,
//        producer_id.fetch_add(1), persistent, shutdown_called, log,
//        row_delimiter ? std::optional<char>{row_delimiter} : std::nullopt, 1, 1024);
//}


bool StorageNATS::checkDependencies(const StorageID & table_id)
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


//void StorageNATS::initializeBuffers()
//{
//    assert(nats_is_ready);
//    if (!initialized)
//    {
//        for (const auto & buffer : buffers)
//            prepareChannelForBuffer(buffer);
//        initialized = true;
//    }
//}


void StorageNATS::streamingToViewsFunc()
{
    try
    {
        auto table_id = getStorageID();

        // Check if at least one direct dependency is attached
        size_t dependencies_count = DatabaseCatalog::instance().getDependencies(table_id).size();
        bool nats_connected = connection->isConnected() || connection->reconnect();

        if (dependencies_count && nats_connected)
        {
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

    mv_attached.store(false);

    /// If there is no running select, stop the loop which was
    /// activated by previous select.
    if (connection->getHandler().loopRunning())
        stopLoopIfNoReaders();

    if (!shutdown_called)
        streaming_task->scheduleAfter(milliseconds_to_wait);
}


bool StorageNATS::streamToViews()
{
    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, nats_context, false, true, true);
    auto block_io = interpreter.execute();

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
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
            *this, storage_snapshot, nats_context, column_names, block_size, false);
        sources.emplace_back(source);
        pipes.emplace_back(source);

        // Limit read batch to maximum block size to allow DDL
        StreamLocalLimits limits;

        limits.speed_limits.max_execution_time = nats_settings->nats_flush_interval_ms.changed
                                                  ? nats_settings->nats_flush_interval_ms
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
            LOG_DEBUG(log, "Connection restored");
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

            connection->getHandler().iterateLoop();
        }
    }

    if ((queue_empty == num_created_consumers) && (++read_attempts == MAX_FAILED_READ_ATTEMPTS))
    {
//        connection->heartbeat();
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


void registerStorageNATS(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        auto nats_settings = std::make_unique<NATSSettings>();
        bool with_named_collection = getExternalDataSourceConfiguration(args.engine_args, *nats_settings, args.getLocalContext());
        if (!with_named_collection && !args.storage_def->settings)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "NATS engine must have settings");

        nats_settings->loadFromQuery(*args.storage_def);

        if (!nats_settings->nats_host_port.changed
           && !nats_settings->nats_address.changed)
                throw Exception("You must specify either `nats_host_port` or `nats_address` settings",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!nats_settings->nats_format.changed)
            throw Exception("You must specify `nats_format` setting", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageNATS::create(args.table_id, args.getContext(), args.columns, std::move(nats_settings), args.attach);
    };

    factory.registerStorage("NATS", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });
}


NamesAndTypesList StorageNATS::getVirtuals() const
{
    return NamesAndTypesList{
            {"_subject", std::make_shared<DataTypeString>()},
            {"_timestamp", std::make_shared<DataTypeUInt64>()}
    };
}

}
