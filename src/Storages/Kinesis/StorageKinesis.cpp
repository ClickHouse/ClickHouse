/*
    WIP
*/

#include <Storages/Kinesis/StorageKinesis.h>
#include <Storages/Kinesis/KinesisSettings.h>
#include <Storages/Kinesis/KinesisConsumer.h>
#include <Storages/Kinesis/KinesisSource.h>
#include <Storages/Kinesis/KinesisSink.h>

#include <Access/Common/AccessType.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Parsers/ASTInsertQuery.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Poco/Logger.h>

#if USE_AWS_KINESIS

#include <IO/S3/PocoHTTPClientFactory.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>
#include <aws/kinesis/model/PutRecordRequest.h>
#include <aws/kinesis/model/RegisterStreamConsumerRequest.h>
// #include <aws/kinesis/model/SubscribeToShardRequest.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_CONNECT_KINESIS;
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsUInt64 max_block_size;
    extern const SettingsUInt64 max_insert_block_size;
    extern const SettingsMilliseconds stream_flush_interval_ms;
}

namespace
{
    Poco::Logger * global_kinesis_log = &Poco::Logger::get("KinesisGlobal");

    void initializeAWSSDK(const Aws::SDKOptions & options)
    {
        LOG_INFO(global_kinesis_log, "Setting up AWS SDK options");
        
        Aws::InitAPI(options);
        Aws::Http::SetHttpClientFactory(std::make_shared<S3::PocoHTTPClientFactory>());
        
        LOG_INFO(global_kinesis_log, "AWS SDK successfully initialized");        
    }

}

// Создание Kinesis клиента
std::shared_ptr<Aws::Kinesis::KinesisClient> createKinesisClient(
    std::shared_ptr<KinesisSettings> kinesis_settings,
    const String & endpoint)
{
    
    LOG_INFO(global_kinesis_log, "Starting Kinesis client creation");

    Aws::Client::ClientConfiguration client_configuration;

    client_configuration.region = kinesis_settings->aws_region;
    client_configuration.verifySSL = kinesis_settings->verify_ssl;
    client_configuration.requestTimeoutMs = kinesis_settings->request_timeout_ms;
    client_configuration.connectTimeoutMs = kinesis_settings->connect_timeout_ms;
    client_configuration.scheme = kinesis_settings->use_http ? Aws::Http::Scheme::HTTP : Aws::Http::Scheme::HTTPS;
    client_configuration.endpointOverride = endpoint;
    
    // Add performance parameters
    client_configuration.maxConnections = kinesis_settings->max_connections;
    
    // TCP keep-alive settings
    client_configuration.enableTcpKeepAlive = kinesis_settings->enable_tcp_keep_alive;
    client_configuration.tcpKeepAliveIntervalMs = kinesis_settings->tcp_keep_alive_interval_ms;

    // Configure retry strategy
    if (kinesis_settings->max_retries > 0) {
        client_configuration.retryStrategy = 
            std::make_shared<Aws::Client::DefaultRetryStrategy>(
                kinesis_settings->max_retries,
                kinesis_settings->retry_initial_delay_ms);
    }

    Aws::Auth::AWSCredentials credentials(kinesis_settings->aws_access_key_id, kinesis_settings->aws_secret_access_key);

    LOG_INFO(global_kinesis_log, "About to create Kinesis client with credentials");
    auto client = std::make_shared<Aws::Kinesis::KinesisClient>(
        credentials,
        Aws::MakeShared<Aws::Kinesis::KinesisEndpointProvider>("KinesisEndpointProvider"),
        client_configuration);
    return client;
}

StorageKinesis::StorageKinesis(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & comment,
    std::shared_ptr<KinesisSettings> kinesis_settings_)
    : IStorage(table_id_)
    , WithContext(context_)
    , kinesis_settings(std::move(kinesis_settings_))
    , log(getLogger("StorageKinesis (" + table_id_.table_name + ")"))
    , thread_per_consumer(kinesis_settings->thread_per_consumer)
{
    LOG_INFO(global_kinesis_log, "Initializing StorageKinesis");

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata); 
    
    LOG_INFO(global_kinesis_log, "Metadata initialized");

    if (!kinesis_settings->stream_name.empty())
    {
        Poco::URI uri(kinesis_settings->stream_name);
        endpoint_override = uri.getScheme() + "://" + uri.getHost();
        if (uri.getPort() != 0 && uri.getPort() != 80 && uri.getPort() != 443)
            endpoint_override += ":" + std::to_string(uri.getPort());
    }

    LOG_INFO(global_kinesis_log, "AWS SDK initialization");
    aws_sdk_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Off;
    aws_sdk_options.httpOptions.installSigPipeHandler = true;
    initializeAWSSDK(aws_sdk_options);

    try
    {
        LOG_INFO(global_kinesis_log, "Creating Kinesis client");
        LOG_INFO(global_kinesis_log, "Using explicit credentials for Kinesis client");
        client = createKinesisClient(kinesis_settings, endpoint_override);
        
        LOG_INFO(global_kinesis_log, "Kinesis client created successfully for endpoint {}", endpoint_override);
    }
    catch (...)
    {
        tryLogCurrentException(global_kinesis_log, "Failed to create Kinesis client");
        throw;
    }

    LOG_INFO(global_kinesis_log, "StorageKinesis initialization completed");
}

void StorageKinesis::startup()
{
    if (shutdown_called)
        return;

    LOG_INFO(log, "Starting StorageKinesis for table {}", getStorageID().getNameForLogs());

    for (size_t i = 0; i < kinesis_settings->num_consumers; ++i)
    {
        auto consumer = createConsumer();
        consumers_ref.push_back(consumer);
        pushConsumer(consumer);
    }

    LOG_INFO(log, "Created {} consumers for table {}", kinesis_settings->num_consumers, getStorageID().getNameForLogs());
    
    // Start background streaming to MaterializedViews
    streaming_task = getContext()->getSchedulePool().createTask(
        "KinesisStreamingToViews", [this]() { streamingToViewsFunc(); });
    
    LOG_INFO(log, "Activating streaming task for table {}", getStorageID().getNameForLogs());
    streaming_task->activateAndSchedule();
}

void StorageKinesis::shutdown(bool /* is_drop */)
{
    if (shutdown_called)
        return;

    shutdown_called = true;

    for (auto & consumer : consumers_ref)
    {
        LOG_INFO(log, "Stopping consumer");
        consumer.lock()->stop();
    }

    LOG_INFO(log, "Shutting down storage Kinesis for table {}", getStorageID().getNameForLogs());

    if (streaming_task)
    {
        LOG_INFO(log, "Stopping streaming task");
        streaming_task->deactivate();
    }

    LOG_INFO(log, "Task stopped");

    if (client)
    {
        LOG_INFO(log, "Shutting down Kinesis client");
        client.reset();
    }

    {
        for (size_t i = 0; i < kinesis_settings->num_consumers; ++i)
        {
            LOG_INFO(log, "Popping consumer");
            popConsumer();
        }
    }

    Aws::ShutdownAPI(aws_sdk_options);
    
    LOG_INFO(log, "Storage Kinesis for table {} successfully shut down", getStorageID().getNameForLogs());
}

KinesisConsumerPtr StorageKinesis::createConsumer()
{
    // TODO: Добавить параметры для потребителя
    return std::make_shared<KinesisConsumer>(
        kinesis_settings->stream_name,
        *client);

}

void StorageKinesis::pushConsumer(KinesisConsumerPtr consumer)
{
    std::lock_guard lock(mutex);
    consumers.push_back(std::move(consumer));
    semaphore.set();
}

KinesisConsumerPtr StorageKinesis::popConsumer()
{
    return popConsumer(std::chrono::milliseconds::zero());
}

KinesisConsumerPtr StorageKinesis::popConsumer(std::chrono::milliseconds timeout)
{
    if (timeout == std::chrono::milliseconds::zero())
        semaphore.wait();
    else
    {
        if (!semaphore.tryWait(timeout.count()))
            return nullptr;
    }

    std::lock_guard lock(mutex);
    auto consumer = consumers.back();
    consumers.pop_back();

    return consumer;
}

void StorageKinesis::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t num_streams)
{
    if (shutdown_called)
        throw Exception(ErrorCodes::CANNOT_CONNECT_KINESIS, "Storage is shutdown");
    
    LOG_INFO(log, "Reading from Kinesis");

    auto modified_num_streams = std::min(num_streams, kinesis_settings->num_consumers);
    
    std::vector<Pipe> pipes;
    for (size_t i = 0; i < modified_num_streams; ++i)
    {
        // TODO: Добавить параметры для Source
        pipes.emplace_back(std::make_shared<KinesisSource>(
            *this,
            storage_snapshot,
            kinesis_settings->format_name,
            storage_snapshot->getSampleBlockForColumns(column_names),
            max_block_size ? max_block_size : local_context->getSettingsRef()[Setting::max_block_size].value,
            kinesis_settings->flush_interval_ms ? kinesis_settings->flush_interval_ms : static_cast<UInt64>(getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds()),
            local_context));
    }
}

SinkToStoragePtr StorageKinesis::write(
    const ASTPtr & /* query */,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    if (shutdown_called)
        throw Exception(ErrorCodes::CANNOT_CONNECT_KINESIS, "Storage is shutdown");
    
    // TODO: Добавить параметры для Sink
    return std::make_shared<KinesisSink>(
        metadata_snapshot,
        *client,
        kinesis_settings->stream_name,
        kinesis_settings->format_name,
        local_context);
    
}

bool StorageKinesis::streamToViewsFunc()
{
    // Стриминг данных в материализованные представления
    // ...
}

bool StorageKinesis::recreateClient()
{
    // Пересоздание клиента при ошибках подключения
    // ...
}

size_t StorageKinesis::getMaxBlockSize() const
{
    return kinesis_settings->max_block_size.changed ? 
        kinesis_settings->max_block_size.value :
        (getContext()->getSettingsRef()[Setting::max_insert_block_size].value / kinesis_settings->num_consumers);
}

size_t StorageKinesis::getFlushIntervalMs() const
{
    return kinesis_settings->flush_interval_ms.changed ? 
        kinesis_settings->flush_interval_ms.totalMilliseconds() : 
        getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds();
}

StreamingHandleErrorMode StorageKinesis::getStreamingHandleErrorMode() const
{
    return kinesis_settings->handle_error_mode;
}

void registerStorageKinesis(StorageFactory & factory)
{
    factory.registerStorage("Kinesis", [](const StorageFactory::Arguments & args)
    {
        auto kinesis_settings = std::make_shared<KinesisSettings>(args.getContext());
        
        if (args.storage_def->settings)
            kinesis_settings->loadFromQuery(*args.storage_def);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Kinesis settings are required");
        
        return std::make_shared<StorageKinesis>(
            args.table_id,
            args.getContext(),
            args.columns,
            args.comment,
            std::move(kinesis_settings));
    },
    {
        .supports_settings = true,
        .supports_skipping_indices = false,
        .supports_projections = false,
        .supports_sort_order = false,
        .supports_ttl = false,
        .supports_replication = false,
        .supports_deduplication = false,
        .supports_parallel_insert = false,
        .supports_schema_inference = false,
        .source_access_type = AccessType::Kinesis
    });
}

}

#endif // USE_AWS_KINESIS
