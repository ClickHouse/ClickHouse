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
    /// Unsubscribing deletes the ephemeral consumer on the server.
    if (subscription)
        natsSubscription_Unsubscribe(subscription.get());
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

    jsSubOptions subscribe_options;
    status = jsSubOptions_Init(&subscribe_options);
    if (status != NATS_OK)
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to create NATS JetStream subscribe options: {}", natsStatus_GetText(status));

    /// An ordered consumer is ephemeral, does not need acknowledgements, and delivers the messages in the order of the stream.
    subscribe_options.Stream = params.stream_name.c_str();
    subscribe_options.Ordered = true;
    if (last_sequence > 0)
    {
        subscribe_options.Config.DeliverPolicy = js_DeliverByStartSequence;
        subscribe_options.Config.OptStartSeq = last_sequence + 1;
    }
    else
        subscribe_options.Config.DeliverPolicy = js_DeliverAll;

    std::vector<const char *> subjects;
    subjects.reserve(params.subjects.size());
    for (const auto & subject : params.subjects)
        subjects.push_back(subject.c_str());

    natsSubscription * new_subscription = nullptr;
    jsErrCode error_code{};
    status = js_SubscribeSyncMulti(
        &new_subscription, jet_stream_ctx.get(), subjects.data(), static_cast<int>(subjects.size()),
        &jet_stream_options, &subscribe_options, &error_code);
    subscription.reset(new_subscription);
    if (status != NATS_OK)
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
            "Failed to subscribe to the subjects of the NATS stream {}: {} (JetStream error code {}): {}",
            params.stream_name, natsStatus_GetText(status), static_cast<int>(error_code), nats_GetLastError(nullptr));

    natsSubscription_SetPendingLimits(subscription.get(), -1, -1);

    LOG_DEBUG(log, "Streaming NATS stream {} after sequence {}", params.stream_name, last_sequence);
}

bool NATSStreamSource::hasNothingPending()
{
    int pending_messages = 0;
    int pending_bytes = 0;
    if (natsSubscription_GetPending(subscription.get(), &pending_messages, &pending_bytes) == NATS_OK && pending_messages > 0)
        return false;

    jsConsumerInfo * info = nullptr;
    jsErrCode error_code{};
    const auto status = natsSubscription_GetConsumerInfo(&info, subscription.get(), nullptr, &error_code);
    ConsumerInfoPtr info_holder(info, &jsConsumerInfo_Destroy);
    if (status != NATS_OK)
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
            "Failed to get the information of the consumer of the NATS stream {}: {}", params.stream_name, natsStatus_GetText(status));

    /// The consumer sequence counts the messages delivered to the consumer, so all of them were received.
    return info->NumPending == 0 && info->Delivered.Consumer <= received_messages;
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

    while (!isCancelled() && total_rows < params.max_block_size)
    {
        natsMsg * raw_message = nullptr;
        const auto status = natsSubscription_NextMsg(&raw_message, subscription.get(), static_cast<int64_t>(params.poll_timeout_ms));
        MessagePtr message(raw_message, &natsMsg_Destroy);

        if (status == NATS_TIMEOUT)
        {
            if (total_rows)
                break;

            if (bounded && hasNothingPending())
            {
                reportFinalCursor();
                stream_finished = true;
            }
            break;
        }

        if (status != NATS_OK)
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
                "Failed to read a message of the NATS stream {}: {}", params.stream_name, natsStatus_GetText(status));

        jsMsgMetaData * raw_meta = nullptr;
        const auto meta_status = natsMsg_GetMetaData(&raw_meta, message.get());
        MetaDataPtr meta(raw_meta, &jsMsgMetaData_Destroy);
        if (meta_status != NATS_OK)
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS,
                "Failed to get the metadata of a message of the NATS stream {}: {}", params.stream_name, natsStatus_GetText(meta_status));

        const UInt64 sequence = meta->Sequence.Stream;
        ++received_messages;
        const bool caught_up = meta->NumPending == 0;

        exception_message.reset();
        const char * data = natsMsg_GetData(message.get());
        const int data_length = natsMsg_GetDataLength(message.get());
        ReadBufferFromMemory buf(data, data_length);
        const size_t new_rows = data_length > 0 ? executor.execute(buf) : 0;
        last_sequence = sequence;

        const String subject = natsMsg_GetSubject(message.get());
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

        if (bounded && caught_up)
        {
            reportFinalCursor();
            stream_finished = true;
            break;
        }
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
