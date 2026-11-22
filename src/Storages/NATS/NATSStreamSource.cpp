#include <Storages/NATS/NATSStreamSource.h>

#include <Columns/IColumn.h>
#include <Core/Streaming/CursorTree.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/logger_useful.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_CONNECT_NATS;
extern const int INVALID_CURSOR_LOOKUP;
}

namespace
{

using MessagePtr = std::unique_ptr<natsMsg, decltype(&natsMsg_Destroy)>;
using MetaDataPtr = std::unique_ptr<jsMsgMetaData, decltype(&jsMsgMetaData_Destroy)>;
using ConsumerInfoPtr = std::unique_ptr<jsConsumerInfo, decltype(&jsConsumerInfo_Destroy)>;
using StreamInfoPtr = std::unique_ptr<jsStreamInfo, decltype(&jsStreamInfo_Destroy)>;

struct MessageListHolder
{
    natsMsgList list{};
    ~MessageListHolder() { natsMsgList_Destroy(&list); }
};

/// One hour.
constexpr int64_t consumer_inactive_threshold_ns = 3600LL * 1000 * 1000 * 1000;
/// The maximum number of messages in one fetch, which bounds the memory used by the fetched messages.
constexpr size_t max_fetch_batch = 1024;

}

NATSStreamSource::NATSStreamSource(
    Params params_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    const StreamSettings & stream_settings_,
    LoggerPtr log_)
    : ISource(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(columns)))
    , params(std::move(params_))
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , stream_settings(stream_settings_)
    , log(std::move(log_))
    , non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized())
    , virtual_header(storage_snapshot->metadata->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader))
    , jet_stream_ctx(nullptr, &jsCtx_Destroy)
    , subscription(nullptr, &natsSubscription_Destroy)
{
}

NATSStreamSource::~NATSStreamSource()
{
    if (subscription)
        natsSubscription_Unsubscribe(subscription.get());

    /// The consumer was created by us, not by the subscription, so the subscription does not delete it.
    /// If deleting fails, the server deletes the consumer after `consumer_inactive_threshold_ns`.
    if (!consumer_name.empty() && jet_stream_ctx)
    {
        jsErrCode error_code{};
        const auto status = js_DeleteConsumer(jet_stream_ctx.get(), params.stream_name.c_str(), consumer_name.c_str(), nullptr, &error_code);
        if (status != NATS_OK)
            LOG_WARNING(log, "Failed to delete the consumer {} of the NATS stream {}: {}", consumer_name, params.stream_name, natsStatus_GetText(status));
    }
}

void NATSStreamSource::start()
{
    if (stream_settings.cursor && stream_settings.cursor->hasSubtree(params.stream_name))
        throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP,
            "The cursor of a NATS table must map the stream name to a sequence, but it has a subtree for stream {}", params.stream_name);

    if (stream_settings.cursor && stream_settings.cursor->hasValue(params.stream_name))
    {
        const Int64 sequence = stream_settings.cursor->getValue(params.stream_name);
        if (sequence < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid sequence {} in the cursor for stream {}", sequence, params.stream_name);
        last_sequence = static_cast<UInt64>(sequence);
    }

    if (!params.connection || !params.connection->isConnected())
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "The NATS table is not connected to the server");

    jsOptions jet_stream_options;
    auto status = jsOptions_Init(&jet_stream_options);
    if (status == NATS_OK)
    {
        jsCtx * new_jet_stream_ctx = nullptr;
        status = natsConnection_JetStream(&new_jet_stream_ctx, params.connection->getConnection(), &jet_stream_options);
        jet_stream_ctx.reset(new_jet_stream_ctx);
    }
    if (status != NATS_OK)
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to create a NATS JetStream context: {}", natsStatus_GetText(status));

    if (!stream_settings.subscribe_for_updates)
    {
        /// A bounded read finishes at the last message of the stream at its start, even if publishing continues.
        jsStreamInfo * stream_info = nullptr;
        jsErrCode error_code{};
        status = js_GetStreamInfo(&stream_info, jet_stream_ctx.get(), params.stream_name.c_str(), &jet_stream_options, &error_code);
        StreamInfoPtr stream_info_holder(stream_info, &jsStreamInfo_Destroy);
        if (status != NATS_OK)
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
                "Failed to get the information of the NATS stream {}: {} (JetStream error code {})",
                params.stream_name, natsStatus_GetText(status), static_cast<int>(error_code));
        end_sequence = stream_info->State.LastSeq;

        if (end_sequence <= last_sequence)
        {
            LOG_DEBUG(log, "NATS stream {} has no messages after sequence {}", params.stream_name, last_sequence);
            finish();
            return;
        }
    }

    /// An ephemeral pull consumer without acknowledgements, which delivers the messages in the order of the stream.
    /// The messages are fetched on demand, in batches no larger than the rest of the output block, so a query that
    /// stops reading (e.g. because of a slow client) does not accumulate the messages in memory.
    jsConsumerConfig consumer_config;
    status = jsConsumerConfig_Init(&consumer_config);
    if (status != NATS_OK)
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to create a NATS JetStream consumer config: {}", natsStatus_GetText(status));

    consumer_config.AckPolicy = js_AckNone;
    consumer_config.MemoryStorage = true;
    consumer_config.Replicas = 1;
    /// The server deletes the consumer if the query is gone without deleting it (e.g. the server was killed).
    consumer_config.InactiveThreshold = consumer_inactive_threshold_ns;
    if (last_sequence > 0)
    {
        consumer_config.DeliverPolicy = js_DeliverByStartSequence;
        consumer_config.OptStartSeq = last_sequence + 1;
    }
    else
        consumer_config.DeliverPolicy = js_DeliverAll;

    std::vector<const char *> subjects;
    subjects.reserve(params.subjects.size());
    for (const auto & subject : params.subjects)
        subjects.push_back(subject.c_str());
    if (subjects.size() == 1)
        consumer_config.FilterSubject = subjects.front();
    else
    {
        consumer_config.FilterSubjects = subjects.data();
        consumer_config.FilterSubjectsLen = static_cast<int>(subjects.size());
    }

    {
        jsConsumerInfo * consumer_info = nullptr;
        jsErrCode error_code{};
        status = js_AddConsumer(&consumer_info, jet_stream_ctx.get(), params.stream_name.c_str(), &consumer_config, &jet_stream_options, &error_code);
        ConsumerInfoPtr consumer_info_holder(consumer_info, &jsConsumerInfo_Destroy);
        if (status != NATS_OK)
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
                "Failed to create a consumer of the NATS stream {}: {} (JetStream error code {}): {}",
                params.stream_name, natsStatus_GetText(status), static_cast<int>(error_code), nats_GetLastError(nullptr));
        consumer_name = consumer_info->Name;
    }

    jsSubOptions subscribe_options;
    status = jsSubOptions_Init(&subscribe_options);
    if (status != NATS_OK)
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to create NATS JetStream subscribe options: {}", natsStatus_GetText(status));
    subscribe_options.Stream = params.stream_name.c_str();
    subscribe_options.Consumer = consumer_name.c_str();
    /// The subscription checks that its config matches the config of the consumer it binds to.
    subscribe_options.Config.MemoryStorage = consumer_config.MemoryStorage;

    natsSubscription * new_subscription = nullptr;
    jsErrCode error_code{};
    status = js_PullSubscribe(&new_subscription, jet_stream_ctx.get(), nullptr, nullptr, &jet_stream_options, &subscribe_options, &error_code);
    subscription.reset(new_subscription);
    if (status != NATS_OK)
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
            "Failed to subscribe to the consumer {} of the NATS stream {}: {} (JetStream error code {}): {}",
            consumer_name, params.stream_name, natsStatus_GetText(status), static_cast<int>(error_code), nats_GetLastError(nullptr));

    LOG_DEBUG(log, "Streaming NATS stream {} after sequence {}{}", params.stream_name, last_sequence,
        stream_settings.subscribe_for_updates ? "" : fmt::format(" up to sequence {}", end_sequence));
}

bool NATSStreamSource::hasNothingPending()
{
    jsConsumerInfo * info = nullptr;
    jsErrCode error_code{};
    const auto status = natsSubscription_GetConsumerInfo(&info, subscription.get(), nullptr, &error_code);
    ConsumerInfoPtr info_holder(info, &jsConsumerInfo_Destroy);
    if (status != NATS_OK)
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
            "Failed to get the information of the consumer of the NATS stream {}: {}", params.stream_name, natsStatus_GetText(status));

    if (info->NumPending != 0)
        return false;

    /// The consumer sequence counts the messages delivered to the consumer. A message delivered after its fetch
    /// expired is received by the next fetch, so wait for it.
    return info->Delivered.Consumer <= received_messages;
}

void NATSStreamSource::finish()
{
    reportFinalCursor();
    stream_finished = true;
}

void NATSStreamSource::reportFinalCursor() const
{
    /// Only an incremental refresh of a materialized view has the holder of the cursor.
    auto streaming_cursor = context->getStreamingCursor();
    if (!streaming_cursor || last_sequence == 0)
        return;

    auto cursor = std::make_shared<CursorTreeNode>();
    cursor->setValue(params.stream_name, static_cast<Int64>(last_sequence));
    streaming_cursor->merge(cursor);
}

void NATSStreamSource::reportEndOfBoundedReadAsCursor()
{
    /// Nothing was read if the read did not start, and a finished read has already reported its cursor.
    if (!started || stream_finished || stream_settings.subscribe_for_updates)
        return;

    last_sequence = std::max(last_sequence, end_sequence);
    finish();
}

IProcessor::Status NATSStreamSource::prepare()
{
    /// The query can stop reading before the bounded read is finished (e.g. `LIMIT BY` or `DISTINCT` with constant keys).
    /// Its result is the result over all the messages of the bounded read, so the cursor advances to its end,
    /// the same as if the rest of the messages were read. Otherwise the next incremental refresh would read
    /// the same messages again. A cancelled query does not have a successful result.
    if (getPort().isFinished() && !isCancelled())
        reportEndOfBoundedReadAsCursor();

    return ISource::prepare();
}

std::optional<Chunk> NATSStreamSource::tryGenerate()
{
    /// An idle stream returns an empty chunk after a poll, so that the thread runs other processors in the meantime
    /// (e.g. the other tables of a `Merge` table) instead of waiting here.
    auto chunk = generate();
    if (!chunk && (stream_finished || isCancelled()))
        return std::nullopt;
    return chunk;
}

Chunk NATSStreamSource::generate()
{
    if (stream_finished)
        return {};

    if (!started)
    {
        start();
        started = true;
    }

    const bool bounded = !stream_settings.subscribe_for_updates;

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    EmptyReadBuffer empty_buf;
    auto parser_shared_resources = FormatParserSharedResources::singleThreaded(context->getSettingsRef());
    auto create_format = [&]
    {
        return FormatFactory::instance().getInput(
            params.format_name, empty_buf, non_virtual_header, context, params.max_block_size, std::nullopt, parser_shared_resources);
    };

    std::optional<String> exception_message;
    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e) -> size_t
    {
        if (params.handle_error_mode != StreamingHandleErrorMode::STREAM)
            throw std::move(e);

        exception_message = e.message();
        for (size_t i = 0; i < result_columns.size(); ++i)
        {
            result_columns[i]->rollback(*checkpoints[i]);
            result_columns[i]->insertDefault();
        }
        return 1;
    };

    StreamingFormatExecutor executor(non_virtual_header, std::move(create_format), std::move(on_error));

    const Field table_name = params.table_name;
    size_t total_rows = 0;

    /// Collecting a block takes at most the poll timeout, so that a steady stream of messages, slower than
    /// the block size, is returned as it arrives.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(params.poll_timeout_ms);

    auto process_message = [&](natsMsg * message)
    {
        jsMsgMetaData * raw_meta = nullptr;
        const auto meta_status = natsMsg_GetMetaData(&raw_meta, message);
        MetaDataPtr meta(raw_meta, &jsMsgMetaData_Destroy);
        if (meta_status != NATS_OK)
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
                "Failed to get the metadata of a message of the NATS stream {}: {}", params.stream_name, natsStatus_GetText(meta_status));

        /// The consumer does not redeliver the messages, so a gap means that some messages were lost.
        if (meta->Sequence.Consumer != received_messages + 1)
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
                "Lost messages of the NATS stream {}: expected the consumer sequence {}, but got {}",
                params.stream_name, received_messages + 1, meta->Sequence.Consumer);
        ++received_messages;

        const UInt64 sequence = meta->Sequence.Stream;
        if (bounded && sequence > end_sequence)
        {
            /// Published after the start of a bounded read.
            finish();
            return;
        }

        exception_message.reset();
        const char * data = natsMsg_GetData(message);
        const int data_length = natsMsg_GetDataLength(message);
        ReadBufferFromMemory buf(data, data_length);
        const size_t new_rows = data_length > 0 ? executor.execute(buf) : 0;
        last_sequence = sequence;

        const String subject = natsMsg_GetSubject(message);
        for (size_t i = 0; i < new_rows; ++i)
        {
            for (size_t column_index = 0; column_index < virtual_columns.size(); ++column_index)
            {
                const auto & name = virtual_header.getByPosition(column_index).name;
                auto & column = virtual_columns[column_index];
                if (name == "_subject")
                    column->insert(subject);
                else if (name == "_table")
                    column->insert(table_name);
                else if (name == "_raw_message" && exception_message)
                    column->insertData(data, data_length);
                else if (name == "_error" && exception_message)
                    column->insertData(exception_message->data(), exception_message->size());
                else
                    column->insertDefault();
            }
        }
        total_rows += new_rows;

        if (bounded && (sequence >= end_sequence || meta->NumPending == 0))
            finish();
    };

    while (!stream_finished && !isCancelled() && total_rows < params.max_block_size)
    {
        const auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
        if (remaining_ms <= 0)
        {
            if (!total_rows && bounded && hasNothingPending())
                finish();
            break;
        }

        MessageListHolder messages;
        const auto batch = std::min(max_fetch_batch, params.max_block_size - total_rows);
        const auto status = natsSubscription_Fetch(&messages.list, subscription.get(), static_cast<int>(batch), remaining_ms, nullptr);

        if (status == NATS_TIMEOUT)
        {
            if (!total_rows && bounded && hasNothingPending())
                finish();
            break;
        }

        if (status != NATS_OK)
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
                "Failed to read the messages of the NATS stream {}: {}", params.stream_name, natsStatus_GetText(status));

        /// The consumer has already sent the fetched messages, so process all of them, even if the block becomes
        /// larger than `max_block_size` (a message can have several rows).
        for (int i = 0; i < messages.list.Count && !stream_finished; ++i)
            process_message(messages.list.Msgs[i]);
    }

    if (!total_rows)
        return {};

    auto result_block = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));
    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    auto converting_dag = ActionsDAG::makeConvertingActions(
        result_block.cloneEmpty().getColumnsWithTypeAndName(),
        getPort().getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);
    ExpressionActions(std::move(converting_dag)).execute(result_block);

    return Chunk(result_block.getColumns(), result_block.rows());
}

}
