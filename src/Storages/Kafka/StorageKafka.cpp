#include <Storages/Kafka/StorageKafka.h>
#include <Storages/Kafka/parseSyslogLevel.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/KafkaBlockInputStream.h>
#include <Storages/Kafka/KafkaBlockOutputStream.h>
#include <Storages/Kafka/WriteBufferToKafkaProducer.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/formatReadable.h>
#include <Common/config_version.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <librdkafka/rdkafka.h>
#include <common/getFQDNOrHostName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct StorageKafkaInterceptors
{
    static rd_kafka_resp_err_t rdKafkaOnThreadStart(rd_kafka_t *, rd_kafka_thread_type_t thread_type, const char *, void * ctx)
    {
        StorageKafka * self = reinterpret_cast<StorageKafka *>(ctx);

        const auto & storage_id = self->getStorageID();
        const auto & table = storage_id.getTableName();

        switch (thread_type)
        {
            case RD_KAFKA_THREAD_MAIN:
                setThreadName(("rdk:m/" + table.substr(0, 9)).c_str());
                break;
            case RD_KAFKA_THREAD_BACKGROUND:
                setThreadName(("rdk:bg/" + table.substr(0, 8)).c_str());
                break;
            case RD_KAFKA_THREAD_BROKER:
                setThreadName(("rdk:b/" + table.substr(0, 9)).c_str());
                break;
        }

        /// Create ThreadStatus to track memory allocations from librdkafka threads.
        //
        /// And store them in a separate list (thread_statuses) to make sure that they will be destroyed,
        /// regardless how librdkafka calls the hooks.
        /// But this can trigger use-after-free if librdkafka will not destroy threads after rd_kafka_wait_destroyed()
        auto thread_status = std::make_shared<ThreadStatus>();
        std::lock_guard lock(self->thread_statuses_mutex);
        self->thread_statuses.emplace_back(std::move(thread_status));

        return RD_KAFKA_RESP_ERR_NO_ERROR;
    }
    static rd_kafka_resp_err_t rdKafkaOnThreadExit(rd_kafka_t *, rd_kafka_thread_type_t, const char *, void * ctx)
    {
        StorageKafka * self = reinterpret_cast<StorageKafka *>(ctx);

        std::lock_guard lock(self->thread_statuses_mutex);
        const auto it = std::find_if(self->thread_statuses.begin(), self->thread_statuses.end(), [](const auto & thread_status_ptr)
        {
            return thread_status_ptr.get() == current_thread;
        });
        if (it == self->thread_statuses.end())
            throw Exception("No thread status for this librdkafka thread.", ErrorCodes::LOGICAL_ERROR);

        self->thread_statuses.erase(it);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
    }

    static rd_kafka_resp_err_t rdKafkaOnNew(rd_kafka_t * rk, const rd_kafka_conf_t *, void * ctx, char * /*errstr*/, size_t /*errstr_size*/)
    {
        StorageKafka * self = reinterpret_cast<StorageKafka *>(ctx);
        rd_kafka_resp_err_t status;

        status = rd_kafka_interceptor_add_on_thread_start(rk, "init-thread", rdKafkaOnThreadStart, ctx);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            LOG_ERROR(self->log, "Cannot set on thread start interceptor due to {} error", status);
            return status;
        }

        status = rd_kafka_interceptor_add_on_thread_exit(rk, "exit-thread", rdKafkaOnThreadExit, ctx);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(self->log, "Cannot set on thread exit interceptor due to {} error", status);

        return status;
    }

    static rd_kafka_resp_err_t rdKafkaOnConfDup(rd_kafka_conf_t * new_conf, const rd_kafka_conf_t * /*old_conf*/, size_t /*filter_cnt*/, const char ** /*filter*/, void * ctx)
    {
        StorageKafka * self = reinterpret_cast<StorageKafka *>(ctx);
        rd_kafka_resp_err_t status;

        // cppkafka copies configuration multiple times
        status = rd_kafka_conf_interceptor_add_on_conf_dup(new_conf, "init", rdKafkaOnConfDup, ctx);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            LOG_ERROR(self->log, "Cannot set on conf dup interceptor due to {} error", status);
            return status;
        }

        status = rd_kafka_conf_interceptor_add_on_new(new_conf, "init", rdKafkaOnNew, ctx);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(self->log, "Cannot set on conf new interceptor due to {} error", status);

        return status;
    }
};

namespace
{
    const auto RESCHEDULE_MS = 500;
    const auto CLEANUP_TIMEOUT_MS = 3000;
    const auto MAX_THREAD_WORK_DURATION_MS = 60000;  // once per minute leave do reschedule (we can't lock threads in pool forever)

    /// Configuration prefix
    const String CONFIG_PREFIX = "kafka";

    void loadFromConfig(cppkafka::Configuration & conf, const Poco::Util::AbstractConfiguration & config, const std::string & path)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        std::vector<char> errstr(512);

        config.keys(path, keys);

        for (const auto & key : keys)
        {
            const String key_path = path + "." + key;
            // log_level has valid underscore, rest librdkafka setting use dot.separated.format
            // which is not acceptable for XML.
            // See also https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
            const String key_name = (key == "log_level") ? key : boost::replace_all_copy(key, "_", ".");
            conf.set(key_name, config.getString(key_path));
        }
    }
}

StorageKafka::StorageKafka(
    const StorageID & table_id_, ContextPtr context_, const ColumnsDescription & columns_, std::unique_ptr<KafkaSettings> kafka_settings_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , kafka_settings(std::move(kafka_settings_))
    , topics(parseTopics(getContext()->getMacros()->expand(kafka_settings->kafka_topic_list.value)))
    , brokers(getContext()->getMacros()->expand(kafka_settings->kafka_broker_list.value))
    , group(getContext()->getMacros()->expand(kafka_settings->kafka_group_name.value))
    , client_id(
          kafka_settings->kafka_client_id.value.empty() ? getDefaultClientId(table_id_)
                                                        : getContext()->getMacros()->expand(kafka_settings->kafka_client_id.value))
    , format_name(getContext()->getMacros()->expand(kafka_settings->kafka_format.value))
    , row_delimiter(kafka_settings->kafka_row_delimiter.value)
    , schema_name(getContext()->getMacros()->expand(kafka_settings->kafka_schema.value))
    , num_consumers(kafka_settings->kafka_num_consumers.value)
    , log(&Poco::Logger::get("StorageKafka (" + table_id_.table_name + ")"))
    , semaphore(0, num_consumers)
    , intermediate_commit(kafka_settings->kafka_commit_every_batch.value)
    , settings_adjustments(createSettingsAdjustments())
    , thread_per_consumer(kafka_settings->kafka_thread_per_consumer.value)
{
    if (kafka_settings->kafka_handle_error_mode == HandleKafkaErrorMode::STREAM)
    {
        kafka_settings->input_format_allow_errors_num = 0;
        kafka_settings->input_format_allow_errors_ratio = 0;
    }
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
    auto task_count = thread_per_consumer ? num_consumers : 1;
    for (size_t i = 0; i < task_count; ++i)
    {
        auto task = getContext()->getMessageBrokerSchedulePool().createTask(log->name(), [this, i]{ threadFunc(i); });
        task->deactivate();
        tasks.emplace_back(std::make_shared<TaskContext>(std::move(task)));
    }
}

SettingsChanges StorageKafka::createSettingsAdjustments()
{
    SettingsChanges result;
    // Needed for backward compatibility
    if (!kafka_settings->input_format_skip_unknown_fields.changed)
    {
        // Always skip unknown fields regardless of the context (JSON or TSKV)
        kafka_settings->input_format_skip_unknown_fields = true;
    }

    if (!kafka_settings->input_format_allow_errors_ratio.changed)
    {
        kafka_settings->input_format_allow_errors_ratio = 0.;
    }

    if (!kafka_settings->input_format_allow_errors_num.changed)
    {
        kafka_settings->input_format_allow_errors_num = kafka_settings->kafka_skip_broken_messages.value;
    }

    if (!schema_name.empty())
        result.emplace_back("format_schema", schema_name);

    for (const auto & setting : *kafka_settings)
    {
        const auto & name = setting.getName();
        if (name.find("kafka_") == std::string::npos)
            result.emplace_back(name, setting.getValue());
    }
    return result;
}

Names StorageKafka::parseTopics(String topic_list)
{
    Names result;
    boost::split(result,topic_list,[](char c){ return c == ','; });
    for (String & topic : result)
    {
        boost::trim(topic);
    }
    return result;
}

String StorageKafka::getDefaultClientId(const StorageID & table_id_)
{
    return fmt::format("{}-{}-{}-{}", VERSION_NAME, getFQDNOrHostName(), table_id_.database_name, table_id_.table_name);
}


Pipe StorageKafka::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /* query_info */,
    ContextPtr local_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    unsigned /* num_streams */)
{
    if (num_created_consumers == 0)
        return {};

    /// Always use all consumers at once, otherwise SELECT may not read messages from all partitions.
    Pipes pipes;
    pipes.reserve(num_created_consumers);
    auto modified_context = Context::createCopy(local_context);
    modified_context->applySettingsChanges(settings_adjustments);

    // Claim as many consumers as requested, but don't block
    for (size_t i = 0; i < num_created_consumers; ++i)
    {
        /// Use block size of 1, otherwise LIMIT won't work properly as it will buffer excess messages in the last block
        /// TODO: probably that leads to awful performance.
        /// FIXME: seems that doesn't help with extra reading and committing unprocessed messages.
        /// TODO: rewrite KafkaBlockInputStream to KafkaSource. Now it is used in other place.
        pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::make_shared<KafkaBlockInputStream>(*this, metadata_snapshot, modified_context, column_names, log, 1)));
    }

    LOG_DEBUG(log, "Starting reading {} streams", pipes.size());
    return Pipe::unitePipes(std::move(pipes));
}


BlockOutputStreamPtr StorageKafka::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    auto modified_context = Context::createCopy(local_context);
    modified_context->applySettingsChanges(settings_adjustments);

    if (topics.size() > 1)
        throw Exception("Can't write to Kafka table with multiple topics!", ErrorCodes::NOT_IMPLEMENTED);
    return std::make_shared<KafkaBlockOutputStream>(*this, metadata_snapshot, modified_context);
}


void StorageKafka::startup()
{
    for (size_t i = 0; i < num_consumers; ++i)
    {
        try
        {
            pushReadBuffer(createReadBuffer(i));
            ++num_created_consumers;
        }
        catch (const cppkafka::Exception &)
        {
            tryLogCurrentException(log);
        }
    }

    // Start the reader thread
    for (auto & task : tasks)
    {
        task->holder->activateAndSchedule();
    }
}


void StorageKafka::shutdown()
{
    for (auto & task : tasks)
    {
        // Interrupt streaming thread
        task->stream_cancelled = true;

        LOG_TRACE(log, "Waiting for cleanup");
        task->holder->deactivate();
    }

    LOG_TRACE(log, "Closing consumers");
    for (size_t i = 0; i < num_created_consumers; ++i)
        auto buffer = popReadBuffer();
    LOG_TRACE(log, "Consumers closed");

    rd_kafka_wait_destroyed(CLEANUP_TIMEOUT_MS);
}


void StorageKafka::pushReadBuffer(ConsumerBufferPtr buffer)
{
    std::lock_guard lock(mutex);
    buffers.push_back(buffer);
    semaphore.set();
}


ConsumerBufferPtr StorageKafka::popReadBuffer()
{
    return popReadBuffer(std::chrono::milliseconds::zero());
}


ConsumerBufferPtr StorageKafka::popReadBuffer(std::chrono::milliseconds timeout)
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

ProducerBufferPtr StorageKafka::createWriteBuffer(const Block & header)
{
    cppkafka::Configuration conf;
    conf.set("metadata.broker.list", brokers);
    conf.set("group.id", group);
    conf.set("client.id", client_id);
    conf.set("client.software.name", VERSION_NAME);
    conf.set("client.software.version", VERSION_DESCRIBE);
    // TODO: fill required settings
    updateConfiguration(conf);

    auto producer = std::make_shared<cppkafka::Producer>(conf);
    const Settings & settings = getContext()->getSettingsRef();
    size_t poll_timeout = settings.stream_poll_timeout_ms.totalMilliseconds();

    return std::make_shared<WriteBufferToKafkaProducer>(
        producer, topics[0], row_delimiter ? std::optional<char>{row_delimiter} : std::nullopt, 1, 1024, std::chrono::milliseconds(poll_timeout), header);
}


ConsumerBufferPtr StorageKafka::createReadBuffer(const size_t consumer_number)
{
    cppkafka::Configuration conf;

    conf.set("metadata.broker.list", brokers);
    conf.set("group.id", group);
    if (num_consumers > 1)
    {
        conf.set("client.id", fmt::format("{}-{}", client_id, consumer_number));
    }
    else
    {
        conf.set("client.id", client_id);
    }
    conf.set("client.software.name", VERSION_NAME);
    conf.set("client.software.version", VERSION_DESCRIBE);
    conf.set("auto.offset.reset", "smallest");     // If no offset stored for this group, read all messages from the start

    // that allows to prevent fast draining of the librdkafka queue
    // during building of single insert block. Improves performance
    // significantly, but may lead to bigger memory consumption.
    size_t default_queued_min_messages = 100000; // we don't want to decrease the default
    conf.set("queued.min.messages", std::max(getMaxBlockSize(),default_queued_min_messages));

    updateConfiguration(conf);

    // those settings should not be changed by users.
    conf.set("enable.auto.commit", "false");       // We manually commit offsets after a stream successfully finished
    conf.set("enable.auto.offset.store", "false"); // Update offset automatically - to commit them all at once.
    conf.set("enable.partition.eof", "false");     // Ignore EOF messages

    // Create a consumer and subscribe to topics
    auto consumer = std::make_shared<cppkafka::Consumer>(conf);
    consumer->set_destroy_flags(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

    /// NOTE: we pass |stream_cancelled| by reference here, so the buffers should not outlive the storage.
    if (thread_per_consumer)
    {
        auto& stream_cancelled = tasks[consumer_number]->stream_cancelled;
        return std::make_shared<ReadBufferFromKafkaConsumer>(consumer, log, getPollMaxBatchSize(), getPollTimeoutMillisecond(), intermediate_commit, stream_cancelled, topics);
    }
    return std::make_shared<ReadBufferFromKafkaConsumer>(consumer, log, getPollMaxBatchSize(), getPollTimeoutMillisecond(), intermediate_commit, tasks.back()->stream_cancelled, topics);
}

size_t StorageKafka::getMaxBlockSize() const
{
    return kafka_settings->kafka_max_block_size.changed
        ? kafka_settings->kafka_max_block_size.value
        : (getContext()->getSettingsRef().max_insert_block_size.value / num_consumers);
}

size_t StorageKafka::getPollMaxBatchSize() const
{
    size_t batch_size = kafka_settings->kafka_poll_max_batch_size.changed
                        ? kafka_settings->kafka_poll_max_batch_size.value
                        : getContext()->getSettingsRef().max_block_size.value;

    return std::min(batch_size,getMaxBlockSize());
}

size_t StorageKafka::getPollTimeoutMillisecond() const
{
    return kafka_settings->kafka_poll_timeout_ms.changed
        ? kafka_settings->kafka_poll_timeout_ms.totalMilliseconds()
        : getContext()->getSettingsRef().stream_poll_timeout_ms.totalMilliseconds();
}

void StorageKafka::updateConfiguration(cppkafka::Configuration & conf)
{
    // Update consumer configuration from the configuration
    const auto & config = getContext()->getConfigRef();
    if (config.has(CONFIG_PREFIX))
        loadFromConfig(conf, config, CONFIG_PREFIX);

    // Update consumer topic-specific configuration
    for (const auto & topic : topics)
    {
        const auto topic_config_key = CONFIG_PREFIX + "_" + topic;
        if (config.has(topic_config_key))
            loadFromConfig(conf, config, topic_config_key);
    }

    // No need to add any prefix, messages can be distinguished
    conf.set_log_callback([this](cppkafka::KafkaHandleBase &, int level, const std::string & facility, const std::string & message)
    {
        auto [poco_level, client_logs_level] = parseSyslogLevel(level);
        LOG_IMPL(log, client_logs_level, poco_level, "[rdk:{}] {}", facility, message);
    });

    // Configure interceptor to change thread name
    //
    // TODO: add interceptors support into the cppkafka.
    // XXX:  rdkafka uses pthread_set_name_np(), but glibc-compatibliity overrides it to noop.
    {
        // This should be safe, since we wait the rdkafka object anyway.
        void * self = static_cast<void *>(this);

        int status;

        status = rd_kafka_conf_interceptor_add_on_new(conf.get_handle(),
            "init", StorageKafkaInterceptors::rdKafkaOnNew, self);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(log, "Cannot set new interceptor due to {} error", status);

        // cppkafka always copy the configuration
        status = rd_kafka_conf_interceptor_add_on_conf_dup(conf.get_handle(),
            "init", StorageKafkaInterceptors::rdKafkaOnConfDup, self);
        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
            LOG_ERROR(log, "Cannot set dup conf interceptor due to {} error", status);
    }
}

bool StorageKafka::checkDependencies(const StorageID & table_id)
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

void StorageKafka::threadFunc(size_t idx)
{
    assert(idx < tasks.size());
    auto task = tasks[idx];
    try
    {
        auto table_id = getStorageID();
        // Check if at least one direct dependency is attached
        size_t dependencies_count = DatabaseCatalog::instance().getDependencies(table_id).size();
        if (dependencies_count)
        {
            auto start_time = std::chrono::steady_clock::now();

            // Keep streaming as long as there are attached views and streaming is not cancelled
            while (!task->stream_cancelled && num_created_consumers > 0)
            {
                if (!checkDependencies(table_id))
                    break;

                LOG_DEBUG(log, "Started streaming to {} attached views", dependencies_count);

                // Exit the loop & reschedule if some stream stalled
                auto some_stream_is_stalled = streamToViews();
                if (some_stream_is_stalled)
                {
                    LOG_TRACE(log, "Stream(s) stalled. Reschedule.");
                    break;
                }

                auto ts = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(ts-start_time);
                if (duration.count() > MAX_THREAD_WORK_DURATION_MS)
                {
                    LOG_TRACE(log, "Thread work duration limit exceeded. Reschedule.");
                    break;
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    // Wait for attached views
    if (!task->stream_cancelled)
        task->holder->scheduleAfter(RESCHEDULE_MS);
}


bool StorageKafka::streamToViews()
{
    Stopwatch watch;

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!table)
        throw Exception("Engine table " + table_id.getNameForLogs() + " doesn't exist.", ErrorCodes::LOGICAL_ERROR);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    // Create an INSERT query for streaming data
    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    size_t block_size = getMaxBlockSize();

    auto kafka_context = Context::createCopy(getContext());
    kafka_context->makeQueryContext();
    kafka_context->applySettingsChanges(settings_adjustments);

    // Create a stream for each consumer and join them in a union stream
    // Only insert into dependent views and expect that input blocks contain virtual columns
    InterpreterInsertQuery interpreter(insert, kafka_context, false, true, true);
    auto block_io = interpreter.execute();

    // Create a stream for each consumer and join them in a union stream
    BlockInputStreams streams;

    auto stream_count = thread_per_consumer ? 1 : num_created_consumers;
    streams.reserve(stream_count);
    for (size_t i = 0; i < stream_count; ++i)
    {
        auto stream = std::make_shared<KafkaBlockInputStream>(*this, metadata_snapshot, kafka_context, block_io.out->getHeader().getNames(), log, block_size, false);
        streams.emplace_back(stream);

        // Limit read batch to maximum block size to allow DDL
        StreamLocalLimits limits;

        limits.speed_limits.max_execution_time = kafka_settings->kafka_flush_interval_ms.changed
                                                 ? kafka_settings->kafka_flush_interval_ms
                                                 : getContext()->getSettingsRef().stream_flush_interval_ms;

        limits.timeout_overflow_mode = OverflowMode::BREAK;
        stream->setLimits(limits);
    }

    // Join multiple streams if necessary
    BlockInputStreamPtr in;
    if (streams.size() > 1)
        in = std::make_shared<UnionBlockInputStream>(streams, nullptr, streams.size());
    else
        in = streams[0];

    // We can't cancel during copyData, as it's not aware of commits and other kafka-related stuff.
    // It will be cancelled on underlying layer (kafka buffer)
    std::atomic<bool> stub = {false};
    size_t rows = 0;
    copyData(*in, *block_io.out, [&rows](const Block & block)
    {
        rows += block.rows();
    }, &stub);

    bool some_stream_is_stalled = false;
    for (auto & stream : streams)
    {
        some_stream_is_stalled = some_stream_is_stalled || stream->as<KafkaBlockInputStream>()->isStalled();
        stream->as<KafkaBlockInputStream>()->commit();
    }

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(log, "Pushing {} rows to {} took {} ms.",
        formatReadableQuantity(rows), table_id.getNameForLogs(), milliseconds);

    return some_stream_is_stalled;
}

void registerStorageKafka(StorageFactory & factory)
{
    auto creator_fn = [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;
        size_t args_count = engine_args.size();
        bool has_settings = args.storage_def->settings;

        auto kafka_settings = std::make_unique<KafkaSettings>();
        if (has_settings)
        {
            kafka_settings->loadFromQuery(*args.storage_def);
        }

        // Check arguments and settings
        #define CHECK_KAFKA_STORAGE_ARGUMENT(ARG_NUM, PAR_NAME, EVAL)       \
            /* One of the four required arguments is not specified */       \
            if (args_count < (ARG_NUM) && (ARG_NUM) <= 4 &&                 \
                !kafka_settings->PAR_NAME.changed)                          \
            {                                                               \
                throw Exception(                                            \
                    "Required parameter '" #PAR_NAME "' "                   \
                    "for storage Kafka not specified",                      \
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);          \
            }                                                               \
            if (args_count >= (ARG_NUM))                                    \
            {                                                               \
                /* The same argument is given in two places */              \
                if (has_settings &&                                         \
                    kafka_settings->PAR_NAME.changed)                       \
                {                                                           \
                    throw Exception(                                        \
                        "The argument №" #ARG_NUM " of storage Kafka "      \
                        "and the parameter '" #PAR_NAME "' "                \
                        "in SETTINGS cannot be specified at the same time", \
                        ErrorCodes::BAD_ARGUMENTS);                         \
                }                                                           \
                /* move engine args to settings */                          \
                else                                                        \
                {                                                           \
                    if ((EVAL) == 1)                                        \
                    {                                                       \
                        engine_args[(ARG_NUM)-1] =                          \
                            evaluateConstantExpressionAsLiteral(            \
                                engine_args[(ARG_NUM)-1],                   \
                                args.getLocalContext());                    \
                    }                                                       \
                    if ((EVAL) == 2)                                        \
                    {                                                       \
                        engine_args[(ARG_NUM)-1] =                          \
                           evaluateConstantExpressionOrIdentifierAsLiteral( \
                                engine_args[(ARG_NUM)-1],                   \
                                args.getLocalContext());                    \
                    }                                                       \
                    kafka_settings->PAR_NAME =                              \
                        engine_args[(ARG_NUM)-1]->as<ASTLiteral &>().value; \
                }                                                           \
            }

        /** Arguments of engine is following:
          * - Kafka broker list
          * - List of topics
          * - Group ID (may be a constaint expression with a string result)
          * - Message format (string)
          * - Row delimiter
          * - Schema (optional, if the format supports it)
          * - Number of consumers
          * - Max block size for background consumption
          * - Skip (at least) unreadable messages number
          * - Do intermediate commits when the batch consumed and handled
          */

        /* 0 = raw, 1 = evaluateConstantExpressionAsLiteral, 2=evaluateConstantExpressionOrIdentifierAsLiteral */
        CHECK_KAFKA_STORAGE_ARGUMENT(1, kafka_broker_list, 0)
        CHECK_KAFKA_STORAGE_ARGUMENT(2, kafka_topic_list, 1)
        CHECK_KAFKA_STORAGE_ARGUMENT(3, kafka_group_name, 2)
        CHECK_KAFKA_STORAGE_ARGUMENT(4, kafka_format, 2)
        CHECK_KAFKA_STORAGE_ARGUMENT(5, kafka_row_delimiter, 2)
        CHECK_KAFKA_STORAGE_ARGUMENT(6, kafka_schema, 2)
        CHECK_KAFKA_STORAGE_ARGUMENT(7, kafka_num_consumers, 0)
        CHECK_KAFKA_STORAGE_ARGUMENT(8, kafka_max_block_size, 0)
        CHECK_KAFKA_STORAGE_ARGUMENT(9, kafka_skip_broken_messages, 0)
        CHECK_KAFKA_STORAGE_ARGUMENT(10, kafka_commit_every_batch, 0)

        #undef CHECK_KAFKA_STORAGE_ARGUMENT

        auto num_consumers = kafka_settings->kafka_num_consumers.value;

        if (num_consumers > 16)
        {
            throw Exception("Number of consumers can not be bigger than 16", ErrorCodes::BAD_ARGUMENTS);
        }
        else if (num_consumers < 1)
        {
            throw Exception("Number of consumers can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        if (kafka_settings->kafka_max_block_size.changed && kafka_settings->kafka_max_block_size.value < 1)
        {
            throw Exception("kafka_max_block_size can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        if (kafka_settings->kafka_poll_max_batch_size.changed && kafka_settings->kafka_poll_max_batch_size.value < 1)
        {
            throw Exception("kafka_poll_max_batch_size can not be lower than 1", ErrorCodes::BAD_ARGUMENTS);
        }

        return StorageKafka::create(args.table_id, args.getContext(), args.columns, std::move(kafka_settings));
    };

    factory.registerStorage("Kafka", creator_fn, StorageFactory::StorageFeatures{ .supports_settings = true, });
}

NamesAndTypesList StorageKafka::getVirtuals() const
{
    auto result = NamesAndTypesList{
        {"_topic", std::make_shared<DataTypeString>()},
        {"_key", std::make_shared<DataTypeString>()},
        {"_offset", std::make_shared<DataTypeUInt64>()},
        {"_partition", std::make_shared<DataTypeUInt64>()},
        {"_timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())},
        {"_timestamp_ms", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(3))},
        {"_headers.name", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"_headers.value", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}
    };
    if (kafka_settings->kafka_handle_error_mode == HandleKafkaErrorMode::STREAM)
    {
        result.push_back({"_raw_message", std::make_shared<DataTypeString>()});
        result.push_back({"_error", std::make_shared<DataTypeString>()});
    }
    return result;
}

Names StorageKafka::getVirtualColumnNames() const
{
    auto result = Names {
        "_topic",
        "_key",
        "_offset",
        "_partition",
        "_timestamp",
        "_timestamp_ms",
        "_headers.name",
        "_headers.value",
    };
    if (kafka_settings->kafka_handle_error_mode == HandleKafkaErrorMode::STREAM)
    {
        result.push_back({"_raw_message"});
        result.push_back({"_error"});
    }
    return result;
}

}
