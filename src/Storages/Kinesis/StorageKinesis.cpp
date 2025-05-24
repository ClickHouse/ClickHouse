#include <Storages/Kinesis/StorageKinesis.h>
#include <Storages/Kinesis/KinesisSettings.h>
#include <Storages/Kinesis/KinesisConsumer.h>
#include <Storages/Kinesis/KinesisSource.h>
#include <Storages/Kinesis/KinesisSink.h>
#include <Storages/Kinesis/KinesisShardsBalancer.h>
#include <Storages/StorageFactory.h>

#include <Access/Common/AccessType.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Parsers/ASTInsertQuery.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Parsers/ASTIdentifier.h>

#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>

#include <Poco/Logger.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <Interpreters/executeQuery.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/formatAST.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/IStorage.h>
#include <Access/ContextAccess.h>

#include <Processors/Sinks/SinkToStorage.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Columns/ColumnsNumber.h>

#include <Processors/Executors/PushingPipelineExecutor.h>

#include <Columns/ColumnSparse.h>

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

#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int KINESIS_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsUInt64 max_block_size;
    extern const SettingsUInt64 max_insert_block_size;
    extern const SettingsMilliseconds stream_flush_interval_ms;
    extern const SettingsBool allow_settings_after_format_in_insert;
    extern const SettingsBool implicit_select;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace
{
    void initializeAWSSDK(const Aws::SDKOptions & options)
    {
        Aws::InitAPI(options);
        // For enhanced fan-out we need to use HTTP/2 instead of HTTP/1.1, which is not supported by the default PocoHTTPClientFactory
        Aws::Http::SetHttpClientFactory(std::make_shared<S3::PocoHTTPClientFactory>());
    }

}

std::shared_ptr<Aws::Kinesis::KinesisClient> createKinesisClient(
    std::shared_ptr<KinesisSettings> kinesis_settings,
    const String & endpoint)
{
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
    , semaphore(0, static_cast<int>(kinesis_settings->num_consumers))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata); 
    
    if (!kinesis_settings->endpoint_override.empty())
    {
        this->endpoint_override = kinesis_settings->endpoint_override;
    }

    aws_sdk_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Off;
    aws_sdk_options.httpOptions.installSigPipeHandler = true;
    initializeAWSSDK(aws_sdk_options);

    try
    {
        client = createKinesisClient(kinesis_settings, endpoint_override);
    }
    catch (...)
    {
        tryLogCurrentException(&Poco::Logger::get("StorageKinesis"), "Failed to create Kinesis client");
        throw;
    }

    shards_balancer = std::make_shared<KinesisShardsBalancer>(*this, kinesis_settings->stream_name, *client);

    if (kinesis_settings->save_checkpoints)
        createCheckpointsTableIfNotExists();
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

    shards_balancer->isStreamChanged();
    shards_balancer->balanceShards(true);

    streaming_task = getContext()->getSchedulePool().createTask(
        "KinesisStreamingToViews", [this]() { streamingToViewsFunc(); });
    streaming_task->activateAndSchedule();
}

void StorageKinesis::shutdown(bool is_drop)
{
    if (shutdown_called)
        return;

    shutdown_called = true;

    if (!is_drop)
        saveCheckpoints();

    for (auto & consumer : consumers_ref)
        consumer.lock()->stop();

    if (streaming_task)
        streaming_task->deactivate();

    if (client)
        client.reset();

    for (size_t i = 0; i < kinesis_settings->num_consumers; ++i)
        popConsumer();

    Aws::ShutdownAPI(aws_sdk_options);
}

KinesisConsumerPtr StorageKinesis::createConsumer()
{    
    KinesisConsumer::StartingPositionType starting_position = KinesisConsumer::LATEST;
    time_t timestamp_to_read_from = 0;

    if (kinesis_settings->starting_position == "TRIM_HORIZON")
        starting_position = KinesisConsumer::TRIM_HORIZON;
    else if (kinesis_settings->starting_position == "AT_TIMESTAMP")
    {
        starting_position = KinesisConsumer::AT_TIMESTAMP;
        if (kinesis_settings->at_timestamp > 0)
            timestamp_to_read_from = static_cast<time_t>(kinesis_settings->at_timestamp);
        else
            LOG_WARNING(log, "kinesis_initial_stream_position is AT_TIMESTAMP, but kinesis_timestamp_to_read_from is not set or invalid. Defaulting to LATEST.");
    }

    size_t consumer_number = consumers_ref.size();
    String consumer_name;
    if (kinesis_settings->enhanced_fan_out)
        consumer_name = kinesis_settings->consumer_name + "_" + std::to_string(consumer_number);
    else
        consumer_name = "simple_consumer_" + std::to_string(consumer_number);

    return std::make_shared<KinesisConsumer>(
        kinesis_settings->stream_name,
        *client,
        std::map<String, ShardState>(),
        kinesis_settings->max_records_per_request,
        starting_position,
        timestamp_to_read_from,
        consumer_name,
        kinesis_settings->internal_queue_size,
        kinesis_settings->enhanced_fan_out,
        kinesis_settings->max_execution_time_ms
    );
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
        throw Exception(ErrorCodes::KINESIS_ERROR, "Storage is shutdown");

    auto modified_num_streams = std::min(num_streams, kinesis_settings->num_consumers);
    
    std::vector<Pipe> pipes;
    for (size_t i = 0; i < modified_num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<KinesisSource>(
            *this,
            storage_snapshot,
            kinesis_settings->format_name,
            storage_snapshot->getSampleBlockForColumns(column_names),
            max_block_size ? max_block_size : local_context->getSettingsRef()[Setting::max_block_size].value,
            kinesis_settings->flush_interval_ms ? kinesis_settings->flush_interval_ms : static_cast<UInt64>(getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds()),
            local_context,
            true));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    IStorage::readFromPipe(query_plan, std::move(pipe), column_names, storage_snapshot, query_info, local_context, shared_from_this());
}

SinkToStoragePtr StorageKinesis::write(
    const ASTPtr & /* query */,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /* async_insert */)
{
    if (shutdown_called)
        throw Exception(ErrorCodes::KINESIS_ERROR, "Storage is shutdown");

    return std::make_shared<KinesisSink>(
        metadata_snapshot,
        *client,
        kinesis_settings->stream_name,
        kinesis_settings->format_name,
        kinesis_settings->max_rows_per_message,
        local_context);
}

void StorageKinesis::streamingToViewsFunc()
{
    try
    {
        if (shutdown_called)
            return;
        
        if (shards_balancer->isStreamChanged())
        {
            shards_balancer->balanceShards(false);
        }
        
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
        tryLogCurrentException(log, "Error in Kinesis streaming task");
        streaming_task->scheduleAfter(kinesis_settings->poll_timeout_ms);
    }
}

void StorageKinesis::scheduleNextExecution(bool success)
{
    if (!success)
        streaming_task->scheduleAfter(kinesis_settings->poll_timeout_ms);
    else
        streaming_task->schedule();
}

bool StorageKinesis::tryProcessMessages()
{
    if (shutdown_called)
        return false;
    
    try
    {
        if (!connection_ok)
        {
            if (!recreateClient())
                return false;
            connection_ok = true;
        }

        // Check for materialized views
        auto table_id = getStorageID();
        auto view_dependencies = DatabaseCatalog::instance().getDependentViews(table_id);
        auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());

        if (view_dependencies.empty())
            return false;

        std::vector<std::shared_ptr<KinesisSource>> sources;
        Pipes pipes;
        sources.reserve(kinesis_settings->num_consumers);
        pipes.reserve(kinesis_settings->num_consumers);

        for (size_t i = 0; i < kinesis_settings->num_consumers; ++i)
        {
            auto source = std::make_shared<KinesisSource>(
                *this,
                storage_snapshot,
                kinesis_settings->format_name,
                storage_snapshot->metadata->getSampleBlock(),
                kinesis_settings->max_block_size,
                kinesis_settings->flush_interval_ms ? kinesis_settings->flush_interval_ms : static_cast<UInt64>(getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds()),
                getContext(),
                true);
                
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

        if (write_failed)
        {
            LOG_WARNING(log, "Write failed, reschedule");
            return false;
        }

        for (auto & source : sources)
        {
            if (!write_failed)
                source->commit();
            else
                source->rollback();
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

bool StorageKinesis::recreateClient()
{
    try
    {
        // Recreate client with the same parameters
        LOG_INFO(log, "Recreating Kinesis client to restore connection");
        
        client.reset();
        client = createKinesisClient(
            kinesis_settings,
            endpoint_override);
        
        // Update client references in all consumers
        std::lock_guard lock(mutex);
        for (auto & consumer_ptr : consumers)
        {
            consumer_ptr.reset();
        }
        
        consumers.clear();
        for (size_t i = 0; i < kinesis_settings->num_consumers; ++i)
            consumers.push_back(createConsumer());
        
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to recreate Kinesis client");
    }
    return false;
}

void StorageKinesis::saveCheckpoints()
{
    if (!kinesis_settings->save_checkpoints)
    {
        LOG_INFO(log, "Saving checkpoints is disabled");
        return;
    }

    // showCheckpointsTable();

    std::map<String, std::map<String, ShardState>> all_checkpoints;
    
    for (auto & consumer : consumers_ref)
    {
        auto consumer_ptr = consumer.lock();
        if (consumer_ptr)
        {
            auto consumer_states = consumer_ptr->getShardsState();
            if (!consumer_states.empty())
            {
                std::map<String, ShardState> simplified_states;
                for (const auto & [shard_id, state] : consumer_states)
                {
                    ShardState simple_state;
                    simple_state.checkpoint = state.checkpoint;
                    simple_state.iterator = state.iterator; 
                    simple_state.is_closed = state.is_closed;
                    simplified_states[shard_id] = simple_state;
                }
                all_checkpoints[consumer_ptr->consumer_name] = simplified_states;
            }
        }
    }
    
    if (all_checkpoints.empty())
        return;

    ContextMutablePtr op_context = Context::createCopy(getContext());
    op_context->makeQueryContext();
    op_context->setCurrentQueryId("KinesisSaveCheckpoints"); 

    for (const auto & [consumer_name, states] : all_checkpoints)
    {
        if (!states.empty())
            saveOffsets(op_context, getStorageID(), kinesis_settings->stream_name, states);
    }
}

std::map<String, ShardState> StorageKinesis::loadCheckpoints()
{
    if (!kinesis_settings->save_checkpoints)
    {
        LOG_INFO(log, "Loading checkpoints is disabled");
        return {};
    }

    // showCheckpointsTable();

    auto loaded_states = loadOffsets(
        getStorageID(), 
        kinesis_settings->stream_name); 
    
    return loaded_states;
}

void StorageKinesis::saveOffsets(
    ContextMutablePtr op_context,
    const StorageID & table_id, 
    const String & stream_name,
    const std::map<String, ShardState> & states)
{
    try
    {
        const String checkpoints_database_name = "system";
        const String checkpoints_table_name = "kinesis_checkpoints";
        StorageID checkpoints_storage_id(checkpoints_database_name, checkpoints_table_name);

        StoragePtr checkpoints_table = DatabaseCatalog::instance().getTable(checkpoints_storage_id, op_context); 
        if (!checkpoints_table)
            return; 
        
        auto metadata_snapshot = checkpoints_table->getInMemoryMetadataPtr();
        Block sample_block = metadata_snapshot->getSampleBlock();
        MutableColumns columns = sample_block.cloneEmptyColumns();

        for (const auto & [shard_id_key, state_val] : states) 
        {
            size_t i = 0;
            columns[i++]->insert(table_id.database_name);
            columns[i++]->insert(table_id.table_name);
            columns[i++]->insert(stream_name); 
            columns[i++]->insert(shard_id_key);
            columns[i++]->insert(state_val.checkpoint); 
            columns[i++]->insert(state_val.iterator);
            columns[i++]->insert(static_cast<UInt8>(state_val.is_closed));
            if (sample_block.has("last_update_time"))
                 columns[i++]->insert(time(nullptr));
        }
        
        Block block_to_insert = sample_block.cloneWithColumns(std::move(columns));
        
        if (block_to_insert.rows() == 0)
        {
            LOG_INFO(log, "No rows prepared for insert into checkpoints table (logic error?).");
            return;
        }

        auto ast_insert_query = std::make_shared<ASTInsertQuery>();
        ast_insert_query->table_id = checkpoints_storage_id;

        InterpreterInsertQuery interpreter(ast_insert_query, op_context, false /*allow_materialized*/, true /*no_squash*/, false /*no_destination*/, false /*async_insert*/);
        auto block_io = interpreter.execute();

        if (!block_io.pipeline.initialized())
        {
            LOG_ERROR(log, "Pipeline for insert into checkpoints table was not initialized.");
            return;
        }

        auto source_from_chunk = std::make_shared<SourceFromSingleChunk>(block_to_insert);
        block_io.pipeline.complete(Pipe(std::move(source_from_chunk)));
        
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to save checkpoints for table {} stream_name_key {}. Error: {}", 
                  table_id.getNameForLogs(), stream_name, e.displayText());
        throw;
    }
    catch (...)
    {
        LOG_ERROR(log, "Unknown error while saving checkpoints for table {} stream_name_key {}. Rethrowing.", 
                  table_id.getNameForLogs(), stream_name);
        throw; 
    }
}

std::map<String, ShardState> StorageKinesis::loadOffsets(
    const StorageID & table_id, 
    const String & stream_name_key 
    )
{
    LOG_INFO(log, "Loading checkpoints for table {} and stream_name_key {}", table_id.getNameForLogs(), stream_name_key);
    
    ContextMutablePtr query_context_local = Context::createCopy(getContext());
    query_context_local->makeQueryContext();
    query_context_local->setCurrentQueryId("KinesisLoadCheckpoints");

    std::map<String, ShardState> loaded_states; 

    String query_str = fmt::format(
        R"(SELECT shard_id, sequence_number, iterator, is_closed FROM system.kinesis_checkpoints FINAL WHERE database_name = '{}' AND table_name = '{}' AND stream_name = '{}')",
        table_id.database_name, 
        table_id.table_name,    
        stream_name_key        
    );

    try
    {
        auto [ast, block_io] = executeQuery(query_str, query_context_local, {.internal = true}); 
        
        if (!block_io.pipeline.initialized())
            return loaded_states;

        PullingPipelineExecutor executor(block_io.pipeline);
        Block block;
        
        while (executor.pull(block))
        {
            if (!block || block.rows() == 0)
                continue;

            ColumnPtr shard_id_full_col = block.getByName("shard_id").column->convertToFullIfNeeded();
            const auto * shard_id_col = typeid_cast<const ColumnString *>(shard_id_full_col.get());

            ColumnPtr sequence_number_full_col = block.getByName("sequence_number").column->convertToFullIfNeeded();
            const auto * sequence_number_col = typeid_cast<const ColumnString *>(sequence_number_full_col.get());

            ColumnPtr iterator_full_col = block.getByName("iterator").column->convertToFullIfNeeded();
            const auto * iterator_col = typeid_cast<const ColumnString *>(iterator_full_col.get());

            ColumnPtr is_closed_full_col = block.getByName("is_closed").column->convertToFullIfNeeded();
            const auto * is_closed_col = typeid_cast<const ColumnVector<UInt8> *>(is_closed_full_col.get());

            if (!shard_id_col || !sequence_number_col || !iterator_col || !is_closed_col)
            {
                LOG_ERROR(log, "Invalid block structure from system.kinesis_checkpoints for table {} and stream_name_key {}. Skipping block.", table_id.getNameForLogs(), stream_name_key);
                continue;
            }

            for (size_t i = 0; i < block.rows(); ++i)
            {
                ShardState state; 
                String shard_id = shard_id_col->getDataAt(i).toString();
                state.checkpoint = sequence_number_col->getDataAt(i).toString();
                state.iterator = iterator_col->getDataAt(i).toString();
                state.is_closed = is_closed_col->getBool(i);

                loaded_states[shard_id] = state;
            }
        }
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to load checkpoints for table {} and stream_name_key {}. Error: {}. Query: {}", 
                  table_id.getNameForLogs(), stream_name_key, e.displayText(), query_str);
    }
    catch (...)
    {
        LOG_ERROR(log, "Unknown error while loading checkpoints for table {} and stream_name_key {}. Query: {}", 
                  table_id.getNameForLogs(), stream_name_key, query_str);
    }
    return loaded_states;
}

void StorageKinesis::createCheckpointsTableIfNotExists()
{
    String query = R"(
        CREATE TABLE IF NOT EXISTS system.kinesis_checkpoints
        (
            `database_name` String,
            `table_name` String,
            `stream_name` String,
            `shard_id` String,
            `sequence_number` String,
            `iterator` String,
            `is_closed` UInt8,
            `last_update_time` DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(last_update_time)
        ORDER BY (database_name, table_name, stream_name, shard_id)
        SETTINGS index_granularity = 8192;
    )";

    try
    {
        auto query_context = Context::createCopy(getContext());
        query_context->makeQueryContext();
        query_context->setCurrentQueryId("StorageKinesis::createCheckpointsTable");

        const auto & settings = query_context->getSettingsRef();
        
        ParserQuery parser(query.data() + query.size(), 
                         settings[DB::Setting::allow_settings_after_format_in_insert].value, 
                         settings[DB::Setting::implicit_select].value);
        
        ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "Create system.kinesis_checkpoints table", 
                                settings[DB::Setting::max_query_size].value,
                                settings[DB::Setting::max_parser_depth].value,
                                settings[DB::Setting::max_parser_backtracks].value
                                ); 
        
        InterpreterCreateQuery interpreter(ast, query_context);
        interpreter.setInternal(true); 
        interpreter.execute();
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to create or verify system.kinesis_checkpoints table. Error: {}", e.displayText());
        throw; 
    }
}

void StorageKinesis::showCheckpointsTable() // only for debug
{
    ContextMutablePtr query_context_local = Context::createCopy(getContext());
    query_context_local->makeQueryContext();
    query_context_local->setCurrentQueryId("KinesisShowCheckpoints");

    String query_str = "SELECT * FROM system.kinesis_checkpoints";

    try
    {
        auto [ast, block_io] = executeQuery(query_str, query_context_local, {.internal = true});

        if (!block_io.pipeline.initialized())
        {
            LOG_INFO(log, "Checkpoints table system.kinesis_checkpoints is empty or query failed to initialize.");
            return;
        }

        PullingPipelineExecutor executor(block_io.pipeline);
        Block block;
        size_t total_rows_logged = 0;

        while (executor.pull(block))
        {
            if (!block || block.rows() == 0)
                continue;

            const auto & columns_with_type_and_name = block.getColumnsWithTypeAndName();
            FormatSettings format_settings;
            format_settings.json.quote_64bit_integers = false;
            format_settings.json.quote_denormals = false;

            for (size_t i = 0; i < block.rows(); ++i)
            {
                String row_string_data;
                WriteBufferFromString row_dump(row_string_data);
                row_dump.write("{", 1);
                for (size_t j = 0; j < columns_with_type_and_name.size(); ++j)
                {
                    if (j != 0)
                        row_dump.write(", ", 2);

                    const auto & col_data = columns_with_type_and_name[j];
                    DB::writeJSONString(col_data.name, row_dump, format_settings);
                    row_dump.write(": ", 2);

                    ColumnPtr column_to_serialize = col_data.column->convertToFullColumnIfSparse();

                    SerializationPtr serialization = col_data.type->getDefaultSerialization();
                    serialization->serializeTextJSON(*column_to_serialize, i, row_dump, format_settings);
                }
                row_dump.write("}", 1);
                row_dump.finalize();
                LOG_INFO(log, "Row {}: {}", total_rows_logged + 1, row_string_data);
                total_rows_logged++;
            }
        }
        LOG_INFO(log, "Finished showing checkpoints. Total rows logged: {}", total_rows_logged);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to show checkpoints table. Error: {}. Query: {}",
                  e.displayText(), query_str);
    }
    catch (...)
    {
        LOG_ERROR(log, "Unknown error while showing checkpoints table. Query: {}",
                  query_str);
    }
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
        .source_access_type = AccessType::KINESIS,
        .has_builtin_setting_fn = KinesisSettings::has
    });
}

}

#endif // USE_AWS_KINESIS
