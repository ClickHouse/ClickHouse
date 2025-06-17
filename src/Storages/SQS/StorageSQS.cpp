#include <Storages/SQS/SQSSettings.h>
#include <Storages/SQS/SQSConsumer.h>
#include <Storages/SQS/SQSSource.h>
#include <Storages/SQS/SQSSink.h>

#include <Access/Common/AccessType.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/formatReadable.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/CurrentThread.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageFactory.h>
#include <Storages/ColumnsDescription.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Parsers/ASTInsertQuery.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

#if USE_AWS_SQS

#include <IO/S3/PocoHTTPClientFactory.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/SQSErrors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_CONNECT_SQS;
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
    void initializeAWSSDK(const Aws::SDKOptions & options)
    {
        Aws::InitAPI(options);
        Aws::Http::SetHttpClientFactory(std::make_shared<S3::PocoHTTPClientFactory>());
    }
}

std::shared_ptr<Aws::SQS::SQSClient> createSQSClient(
        std::shared_ptr<SQSSettings> sqs_settings,
        const String & endpoint)
    {
    Aws::Client::ClientConfiguration client_configuration;
    
    client_configuration.region = sqs_settings->aws_region;
    client_configuration.verifySSL = sqs_settings->verify_ssl;
    client_configuration.requestTimeoutMs = sqs_settings->request_timeout_ms;
    client_configuration.connectTimeoutMs = sqs_settings->connect_timeout_ms;
    client_configuration.scheme = sqs_settings->use_http ? Aws::Http::Scheme::HTTP : Aws::Http::Scheme::HTTPS;
    client_configuration.endpointOverride = endpoint;

    // Add performance parameters
    client_configuration.maxConnections = sqs_settings->max_connections;
    
    // TCP keep-alive settings
    client_configuration.enableTcpKeepAlive = sqs_settings->enable_tcp_keep_alive;
    client_configuration.tcpKeepAliveIntervalMs = sqs_settings->tcp_keep_alive_interval_ms;
    
    // Proxy settings (if specified)
    if (!sqs_settings->proxy_host.empty()) {
        client_configuration.proxyHost = sqs_settings->proxy_host;
        client_configuration.proxyPort = sqs_settings->proxy_port;
        client_configuration.proxyUserName = sqs_settings->proxy_username;
        client_configuration.proxyPassword = sqs_settings->proxy_password;
    }
    
    // Configure retry strategy
    if (sqs_settings->max_retries > 0) {
        client_configuration.retryStrategy = 
            std::make_shared<Aws::Client::DefaultRetryStrategy>(
                sqs_settings->max_retries,
                sqs_settings->retry_initial_delay_ms);
    }
    
    // // Set User-Agent for debugging
    // client_configuration.userAgent = "ClickHouse-SQS-Client";

    Aws::Auth::AWSCredentials credentials(sqs_settings->aws_access_key_id, sqs_settings->aws_secret_access_key);

    auto client = std::make_shared<Aws::SQS::SQSClient>(
        credentials,
        Aws::MakeShared<Aws::SQS::SQSEndpointProvider>("SQSEndpointProvider"),
        client_configuration);
    return client;
}

StorageSQS::StorageSQS(
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & comment,
    std::shared_ptr<SQSSettings> sqs_settings_)
    : IStorage(table_id_)
    , WithContext(context_)
    , sqs_settings(std::move(sqs_settings_))
    , log(getLogger("StorageSQS (" + table_id_.table_name + ")"))
    , semaphore(0, static_cast<int>(sqs_settings->num_consumers))
{
    LOG_INFO(log, "Initializing StorageSQS");
    
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
    
    LOG_INFO(log, "Metadata initialized");

    if (!sqs_settings->queue_url.empty())
    {
        Poco::URI uri(sqs_settings->queue_url);
        endpoint_override = uri.getScheme() + "://" + uri.getHost();
        if (uri.getPort() != 0 && uri.getPort() != 80 && uri.getPort() != 443)
            endpoint_override += ":" + std::to_string(uri.getPort());
    }

    aws_sdk_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Off;
    aws_sdk_options.httpOptions.installSigPipeHandler = true;
    initializeAWSSDK(aws_sdk_options);

    try
    {
        client = createSQSClient(
            sqs_settings,
            endpoint_override);
        
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to create SQS client");
        throw;
    }
    
    LOG_INFO(log, "StorageSQS initialization completed");
}

void StorageSQS::startup()
{
    if (shutdown_called)
        return;
    
    for (size_t i = 0; i < sqs_settings->num_consumers; ++i)
    {
        auto consumer = createConsumer();
        consumers_ref.push_back(consumer);
        pushConsumer(consumer);
    }
    
    streaming_task = getContext()->getSchedulePool().createTask(
        "SQSStreamingToViews", [this]() { streamingToViewsFunc(); });
    streaming_task->activateAndSchedule();
}

void StorageSQS::shutdown(bool /* is_drop */)
{
    if (shutdown_called)
        return;

    shutdown_called = true;
    
    for (auto & consumer : consumers_ref)
    {
        consumer.lock()->stop();
    }

    LOG_INFO(log, "Shutting down storage SQS for table {}", getStorageID().getNameForLogs());
    
    if (streaming_task)
    {
        streaming_task->deactivate();
    }

    if (client)
    {
        client.reset();
    }

    {
        for (size_t i = 0; i < sqs_settings->num_consumers; ++i)
        {
            popConsumer();
        }
    }

    Aws::ShutdownAPI(aws_sdk_options);
}

SQSConsumerPtr StorageSQS::createConsumer()
{
    return std::make_shared<SQSConsumer>(
        sqs_settings->queue_url,
        *client,
        sqs_settings->max_messages_per_receive,
        static_cast<int>(sqs_settings->visibility_timeout),
        static_cast<int>(sqs_settings->wait_time_seconds),
        sqs_settings->dead_letter_queue_url,
        sqs_settings->max_receive_count,
        sqs_settings->internal_queue_size); 
}

void StorageSQS::pushConsumer(SQSConsumerPtr consumer)
{
    std::lock_guard lock(mutex);
    consumers.push_back(std::move(consumer));
    semaphore.set();
}

SQSConsumerPtr StorageSQS::popConsumer()
{
    return popConsumer(std::chrono::milliseconds::zero());
}

SQSConsumerPtr StorageSQS::popConsumer(std::chrono::milliseconds timeout)
{
    if (timeout == std::chrono::milliseconds::zero())
        semaphore.wait();
    else
    {
        if (!semaphore.tryWait(timeout.count()))
            return nullptr;
    }

    // Take the first available consumer from the list
    std::lock_guard lock(mutex);
    auto consumer = consumers.back();
    consumers.pop_back();

    return consumer;
}

void StorageSQS::read(
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
        throw Exception(ErrorCodes::CANNOT_CONNECT_SQS, "Storage is shutdown");
    
    auto modified_num_streams = std::min(num_streams, sqs_settings->num_consumers);
    
    std::vector<Pipe> pipes;
    for (size_t i = 0; i < modified_num_streams; ++i)
    {   
        pipes.emplace_back(std::make_shared<SQSSource>(
            *this,
            storage_snapshot,
            sqs_settings->format_name,
            storage_snapshot->getSampleBlockForColumns(column_names),
            max_block_size ? max_block_size : local_context->getSettingsRef()[Setting::max_block_size].value,
            sqs_settings->flush_interval_ms ? sqs_settings->flush_interval_ms : static_cast<UInt64>(getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds()),
            local_context,
            sqs_settings->skip_invalid_messages,
            sqs_settings->dead_letter_queue_url,
            true,
            sqs_settings->auto_delete));
    }
    
    auto pipe = Pipe::unitePipes(std::move(pipes));
    IStorage::readFromPipe(query_plan, std::move(pipe), column_names, storage_snapshot, query_info, local_context, shared_from_this());
}

SinkToStoragePtr StorageSQS::write(
    const ASTPtr & /* query */,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    if (shutdown_called)
        throw Exception(ErrorCodes::CANNOT_CONNECT_SQS, "Storage is shutdown");
    
    return std::make_shared<SQSSink>(
        metadata_snapshot,
        *client,
        sqs_settings->queue_url,
        sqs_settings->format_name,
        sqs_settings->max_rows_per_message,
        local_context);
}

bool StorageSQS::tryProcessMessages()
{
    if (shutdown_called)
        return false;
    
    try
    {
        if (!connection_ok)
        {
            // Try to restore connection
            if (!recreateClient())
                return false;
            connection_ok = true;
        }
        
        // Check for materialized views
        auto table_id = getStorageID();
        auto view_dependencies = DatabaseCatalog::instance().getDependentViews(table_id);
        auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());

        if (view_dependencies.empty())
        {
            return false;
        }
        
        std::vector<std::shared_ptr<SQSSource>> sources;
        Pipes pipes;
        sources.reserve(sqs_settings->num_consumers);
        pipes.reserve(sqs_settings->num_consumers);

        for (size_t i = 0; i < sqs_settings->num_consumers; ++i)
        {
            auto source = std::make_shared<SQSSource>(
                *this,
                storage_snapshot,
                sqs_settings->format_name,
                storage_snapshot->metadata->getSampleBlock(),
                sqs_settings->max_block_size,
                sqs_settings->flush_interval_ms ? sqs_settings->flush_interval_ms : static_cast<UInt64>(getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds()),
                getContext(),
                sqs_settings->skip_invalid_messages,
                sqs_settings->dead_letter_queue_url,
                false,
                sqs_settings->auto_delete);

            sources.emplace_back(source);
            pipes.emplace_back(source);
        }

        auto insert = std::make_shared<ASTInsertQuery>();
        auto insert_context = Context::createCopy(getContext());
        insert->table_id = table_id;
        if (!sources.empty())
        {
            auto column_list = std::make_shared<ASTExpressionList>();
            const auto & header = sources[0]->getPort().getHeader();
            for (const auto & column : header)
                column_list->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
            insert->columns = std::move(column_list); 
        }

        InterpreterInsertQuery interpreter(
            insert,
            insert_context,
            /* allow_materialized */ false,
            /* no_squash */ true,
            /* no_destination */ true,
            /* async_isnert */ false);
        auto block_io = interpreter.execute();

        std::atomic_size_t rows = 0;
        block_io.pipeline.complete(Pipe::unitePipes(std::move(pipes)));
        block_io.pipeline.setProgressCallback([&](const Progress & progress) { rows += progress.read_rows.load(); });

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

        LOG_TRACE(log, "Processed {} rows", rows);

        for (auto & source : sources)
        {
            if (!write_failed)
            {
                source->deleteMessages();
            }
        }

        if (write_failed)
        {
            LOG_TRACE(log, "Write failed, reschedule");
            return false;
        }

        return true;
    }
    catch (const Exception & e)
    {
        connection_ok = false;
        LOG_ERROR(log, "Error when processing message batch: {}", e.displayText());
    }
    catch (...)
    {
        connection_ok = false;
        LOG_ERROR(log, "Unknown error when processing message batch");
    }
    
    return false;
}

bool StorageSQS::recreateClient()
{
    try
    {
        client.reset();
        client = createSQSClient(
            sqs_settings,
            endpoint_override);
        
        std::lock_guard lock(mutex);
        for (auto & consumer_ptr : consumers)
        {
            consumer_ptr.reset();
        }
        
        consumers.clear();
        for (size_t i = 0; i < sqs_settings->num_consumers; ++i)
            consumers.push_back(createConsumer());
        
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to recreate SQS client");
    }
    return false;
}

void StorageSQS::scheduleNextExecution(bool success)
{
    if (!success)
        streaming_task->scheduleAfter(sqs_settings->poll_timeout_ms);
    else
        streaming_task->schedule();
}

void StorageSQS::streamingToViewsFunc()
{
    try
    {
        if (shutdown_called)
            return;
        
        bool success = tryProcessMessages();
        
        static std::atomic<size_t> success_count = 0;
        static std::atomic<size_t> failure_count = 0;
        
        if (success)
        {
            LOG_INFO(log, "Successfully processed messages, success/failure stats: {}/{}", 
                    ++success_count, failure_count.load());
        }
        else
        {
            LOG_INFO(log, "No messages processed this cycle, success/failure stats: {}/{}", 
                    success_count.load(), ++failure_count);
        }
        
        scheduleNextExecution(success);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in SQS streaming task");
        streaming_task->scheduleAfter(sqs_settings->poll_timeout_ms);
    }
}

void registerStorageSQS(StorageFactory & factory)
{
    factory.registerStorage("SQS", [](const StorageFactory::Arguments & args)
    {
        auto sqs_settings = std::make_unique<SQSSettings>(args.getContext());
        
        // Parse settings from AST
        if (args.storage_def->settings)
            sqs_settings->loadFromQuery(*args.storage_def);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SQS settings are required");
        
        return std::make_shared<StorageSQS>(
            args.table_id,
            args.getContext(),
            args.columns,
            args.comment,
            std::move(sqs_settings));
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
        .source_access_type = AccessType::SQS,
        .has_builtin_setting_fn = SQSSettings::has
    });
}

}

#endif // USE_AWS_SQS
