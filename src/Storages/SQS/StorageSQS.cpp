#include <Storages/SQS/StorageSQS.h>
#include <Storages/SQS/SQSConsumer.h>
#include <Storages/SQS/SQSSource.h>
#include <Storages/SQS/SQSSink.h>
#include <Storages/SQS/SQSSettings.h>

#include <Access/Common/AccessType.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/StorageFactory.h>
#include <Storages/ColumnsDescription.h>

#include <Poco/URI.h>

#include "config.h"

#if USE_AWS_SQS

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/SQSEndpointProvider.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_SQS;
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsUInt64 max_block_size;
    extern const SettingsMilliseconds stream_flush_interval_ms;
}

namespace SQSSetting
{
    extern const SQSSettingsBool sqs_auto_delete;
    extern const SQSSettingsString sqs_aws_access_key_id;
    extern const SQSSettingsString sqs_aws_secret_access_key;
    extern const SQSSettingsString sqs_aws_region;
    extern const SQSSettingsString sqs_dead_letter_queue_url;
    extern const SQSSettingsString sqs_endpoint;
    extern const SQSSettingsMilliseconds sqs_flush_interval_ms;
    extern const SQSSettingsString sqs_format;
    extern const SQSSettingsStreamingHandleErrorMode sqs_handle_error_mode;
    extern const SQSSettingsUInt64 sqs_max_block_size;
    extern const SQSSettingsUInt64 sqs_max_messages_per_receive;
    extern const SQSSettingsUInt64 sqs_max_receive_count;
    extern const SQSSettingsUInt64 sqs_max_rows_per_message;
    extern const SQSSettingsUInt64 sqs_num_consumers;
    extern const SQSSettingsUInt64 sqs_poll_timeout_ms;
    extern const SQSSettingsString sqs_queue_url;
    extern const SQSSettingsString sqs_schema;
    extern const SQSSettingsUInt64 sqs_skip_broken_messages;
    extern const SQSSettingsBool sqs_verify_ssl;
    extern const SQSSettingsUInt64 sqs_visibility_timeout;
    extern const SQSSettingsUInt64 sqs_wait_time_seconds;
}

VirtualColumnsDescription StorageSQS::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_message_id", std::make_shared<DataTypeString>(), "SQS message ID");
    desc.addEphemeral("_receive_count", std::make_shared<DataTypeUInt64>(), "Number of times this message has been received");
    desc.addEphemeral("_sent_timestamp", std::make_shared<DataTypeUInt64>(), "Unix timestamp (seconds) when the message was sent");
    desc.addEphemeral("_message_group_id", std::make_shared<DataTypeString>(), "FIFO queue message group ID");
    desc.addEphemeral("_message_deduplication_id", std::make_shared<DataTypeString>(), "FIFO queue message deduplication ID");
    desc.addEphemeral("_sequence_number", std::make_shared<DataTypeUInt64>(), "FIFO queue sequence number");
    return desc;
}

std::shared_ptr<Aws::SQS::SQSClient> StorageSQS::createClient() const
{
    Aws::Client::ClientConfiguration config;
    config.region = (*sqs_settings)[SQSSetting::sqs_aws_region].value;
    config.verifySSL = (*sqs_settings)[SQSSetting::sqs_verify_ssl].value;
    config.endpointOverride = endpoint_override;
    config.scheme = Aws::Http::Scheme::HTTP;

    Aws::Auth::AWSCredentials credentials(
        (*sqs_settings)[SQSSetting::sqs_aws_access_key_id].value,
        (*sqs_settings)[SQSSetting::sqs_aws_secret_access_key].value);

    return std::make_shared<Aws::SQS::SQSClient>(
        credentials,
        Aws::MakeShared<Aws::SQS::SQSEndpointProvider>("SQSEndpointProvider"),
        config);
}

StorageSQS::StorageSQS(
    const StorageID & table_id,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    const String & comment,
    std::unique_ptr<SQSSettings> sqs_settings_)
    : IStorage(table_id)
    , WithContext(context_)
    , sqs_settings(std::move(sqs_settings_))
    , log(getLogger("StorageSQS(" + table_id.table_name + ")"))
    , semaphore(0, static_cast<int>((*sqs_settings)[SQSSetting::sqs_num_consumers].value))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
    setVirtuals(createVirtuals());

    const String & queue_url = (*sqs_settings)[SQSSetting::sqs_queue_url].value;
    if (queue_url.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "sqs_queue_url setting is required for storage SQS");

    /// Derive the endpoint override from the queue URL (scheme + host + port).
    /// This allows using LocalStack or other compatible services.
    const String & endpoint_setting = (*sqs_settings)[SQSSetting::sqs_endpoint].value;
    if (!endpoint_setting.empty())
    {
        endpoint_override = endpoint_setting;
    }
    else
    {
        Poco::URI uri(queue_url);
        endpoint_override = uri.getScheme() + "://" + uri.getHost();
        if (uri.getPort() != 0 && uri.getPort() != 80 && uri.getPort() != 443)
            endpoint_override += ":" + std::to_string(uri.getPort());
    }

    try
    {
        client = createClient();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to create SQS client");
        throw;
    }
}

StorageSQS::~StorageSQS() = default;

void StorageSQS::startup()
{
    if (shutdown_called)
        return;

    const size_t num = (*sqs_settings)[SQSSetting::sqs_num_consumers].value;
    for (size_t i = 0; i < num; ++i)
    {
        auto consumer = createConsumer();
        consumers_ref.push_back(consumer);
        pushConsumer(consumer);
    }

    streaming_task = getContext()->getSchedulePool().createTask(
        "SQSStreamingToViews", [this]() { streamingToViewsFunc(); });
    streaming_task->activateAndSchedule();
}

void StorageSQS::shutdown(bool /*is_drop*/)
{
    if (shutdown_called.exchange(true))
        return;

    for (auto & weak_consumer : consumers_ref)
    {
        if (auto consumer = weak_consumer.lock())
            consumer->stop();
    }

    if (streaming_task)
        streaming_task->deactivate();

    /// Drain the semaphore so shutdown doesn't hang waiting for consumers.
    const size_t num = (*sqs_settings)[SQSSetting::sqs_num_consumers].value;
    for (size_t i = 0; i < num; ++i)
        popConsumer(std::chrono::milliseconds(0));

    client.reset();
}

SQSConsumerPtr StorageSQS::createConsumer()
{
    return std::make_shared<SQSConsumer>(
        (*sqs_settings)[SQSSetting::sqs_queue_url].value,
        *client,
        (*sqs_settings)[SQSSetting::sqs_max_messages_per_receive].value,
        static_cast<int>((*sqs_settings)[SQSSetting::sqs_visibility_timeout].value),
        static_cast<int>((*sqs_settings)[SQSSetting::sqs_wait_time_seconds].value),
        (*sqs_settings)[SQSSetting::sqs_dead_letter_queue_url].value,
        (*sqs_settings)[SQSSetting::sqs_max_receive_count].value,
        100 /* internal_queue_size */);
}

void StorageSQS::pushConsumer(SQSConsumerPtr consumer)
{
    std::lock_guard lock(mutex);
    consumers.push_back(std::move(consumer));
    semaphore.set();
}

SQSConsumerPtr StorageSQS::popConsumer()
{
    semaphore.wait();
    std::lock_guard lock(mutex);
    auto consumer = std::move(consumers.back());
    consumers.pop_back();
    return consumer;
}

SQSConsumerPtr StorageSQS::popConsumer(std::chrono::milliseconds timeout)
{
    if (timeout == std::chrono::milliseconds::zero())
    {
        if (!semaphore.tryWait(0))
            return nullptr;
    }
    else
    {
        if (!semaphore.tryWait(static_cast<long>(timeout.count())))
            return nullptr;
    }

    std::lock_guard lock(mutex);
    if (consumers.empty())
        return nullptr;
    auto consumer = std::move(consumers.back());
    consumers.pop_back();
    return consumer;
}

void StorageSQS::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    if (shutdown_called)
        throw Exception(ErrorCodes::CANNOT_CONNECT_SQS, "SQS storage is shutting down");

    const size_t num_consumers = (*sqs_settings)[SQSSetting::sqs_num_consumers].value;
    const size_t streams = std::min(num_streams, num_consumers);

    const UInt64 flush_ms = (*sqs_settings)[SQSSetting::sqs_flush_interval_ms].totalMilliseconds();
    const UInt64 max_execution_ms = flush_ms
        ? flush_ms
        : static_cast<UInt64>(local_context->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds());

    Pipes pipes;
    pipes.reserve(streams);
    for (size_t i = 0; i < streams; ++i)
    {
        pipes.emplace_back(std::make_shared<SQSSource>(
            *this,
            storage_snapshot,
            (*sqs_settings)[SQSSetting::sqs_format].value,
            storage_snapshot->getSampleBlockForColumns(column_names),
            max_block_size ? max_block_size : local_context->getSettingsRef()[Setting::max_block_size].value,
            max_execution_ms,
            local_context,
            (*sqs_settings)[SQSSetting::sqs_skip_broken_messages].value > 0,
            (*sqs_settings)[SQSSetting::sqs_skip_broken_messages].value,
            (*sqs_settings)[SQSSetting::sqs_dead_letter_queue_url].value,
            (*sqs_settings)[SQSSetting::sqs_auto_delete].value));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    IStorage::readFromPipe(query_plan, std::move(pipe), column_names, storage_snapshot, query_info, local_context, shared_from_this());
}

SinkToStoragePtr StorageSQS::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool /*async_insert*/)
{
    if (shutdown_called)
        throw Exception(ErrorCodes::CANNOT_CONNECT_SQS, "SQS storage is shutting down");

    return std::make_shared<SQSSink>(
        metadata_snapshot,
        *client,
        (*sqs_settings)[SQSSetting::sqs_queue_url].value,
        (*sqs_settings)[SQSSetting::sqs_format].value,
        (*sqs_settings)[SQSSetting::sqs_max_rows_per_message].value,
        local_context);
}

bool StorageSQS::tryStreamToViews()
{
    if (shutdown_called)
        return false;

    if (!connection_ok)
    {
        try
        {
            client = createClient();
            connection_ok = true;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to recreate SQS client");
            return false;
        }
    }

    auto table_id = getStorageID();
    auto view_dependencies = DatabaseCatalog::instance().getDependentViews(table_id);
    if (view_dependencies.empty())
        return false;

    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), getContext());
    const size_t num_consumers = (*sqs_settings)[SQSSetting::sqs_num_consumers].value;

    const UInt64 flush_ms = (*sqs_settings)[SQSSetting::sqs_flush_interval_ms].totalMilliseconds();
    const UInt64 max_execution_ms = flush_ms
        ? flush_ms
        : static_cast<UInt64>(getContext()->getSettingsRef()[Setting::stream_flush_interval_ms].totalMilliseconds());

    std::vector<std::shared_ptr<SQSSource>> sources;
    Pipes pipes;
    sources.reserve(num_consumers);
    pipes.reserve(num_consumers);

    for (size_t i = 0; i < num_consumers; ++i)
    {
        auto source = std::make_shared<SQSSource>(
            *this,
            storage_snapshot,
            (*sqs_settings)[SQSSetting::sqs_format].value,
            storage_snapshot->metadata->getSampleBlock(),
            (*sqs_settings)[SQSSetting::sqs_max_block_size].value
                ? (*sqs_settings)[SQSSetting::sqs_max_block_size].value
                : getContext()->getSettingsRef()[Setting::max_block_size].value,
            max_execution_ms,
            getContext(),
            (*sqs_settings)[SQSSetting::sqs_skip_broken_messages].value > 0,
            (*sqs_settings)[SQSSetting::sqs_skip_broken_messages].value,
            (*sqs_settings)[SQSSetting::sqs_dead_letter_queue_url].value,
            false /* auto_delete - we delete manually on success only */);

        sources.emplace_back(source);
        pipes.emplace_back(source);
    }

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    if (!sources.empty())
    {
        auto column_list = std::make_shared<ASTExpressionList>();
        for (const auto & col : sources[0]->getPort().getHeader())
            column_list->children.emplace_back(std::make_shared<ASTIdentifier>(col.name));
        insert->columns = std::move(column_list);
    }

    auto insert_context = Context::createCopy(getContext());
    InterpreterInsertQuery interpreter(
        insert,
        insert_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_insert */ false);

    auto block_io = interpreter.execute();
    block_io.pipeline.complete(Pipe::unitePipes(std::move(pipes)));

    bool write_failed = false;
    try
    {
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
    catch (...)
    {
        write_failed = true;
        connection_ok = false;
        tryLogCurrentException(log, "Failed to push SQS messages to views");
    }

    if (!write_failed)
    {
        if ((*sqs_settings)[SQSSetting::sqs_auto_delete].value)
        {
            for (auto & source : sources)
                source->deleteProcessedMessages();
        }
        return true;
    }

    return false;
}

void StorageSQS::scheduleNextExecution(bool success)
{
    if (!streaming_task)
        return;

    if (success)
        streaming_task->schedule();
    else
        streaming_task->scheduleAfter((*sqs_settings)[SQSSetting::sqs_poll_timeout_ms].value);
}

void StorageSQS::streamingToViewsFunc()
{
    if (shutdown_called)
        return;

    bool success = false;
    try
    {
        success = tryStreamToViews();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in SQS streaming task");
    }

    scheduleNextExecution(success);
}

void registerStorageSQS(StorageFactory & factory)
{
    factory.registerStorage(
        "SQS",
        [](const StorageFactory::Arguments & args)
        {
            auto sqs_settings = std::make_unique<SQSSettings>();
            if (args.storage_def->settings)
                sqs_settings->loadFromQuery(*args.storage_def);

            if ((*sqs_settings)[SQSSetting::sqs_queue_url].value.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "SQS storage requires sqs_queue_url setting");

            if ((*sqs_settings)[SQSSetting::sqs_format].value.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "SQS storage requires sqs_format setting");

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
            .has_builtin_setting_fn = SQSSettings::hasBuiltin,
        });
}

}

#endif // USE_AWS_SQS
