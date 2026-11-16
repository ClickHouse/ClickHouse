#include <Storages/Kafka/KafkaStreamSource.h>

#include <Columns/IColumn.h>
#include <Core/Streaming/CursorTree.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DeadLetterQueue.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <chrono>
#include <unordered_set>

namespace ProfileEvents
{
extern const Event KafkaMessagesRead;
extern const Event KafkaMessagesFailed;
extern const Event KafkaRowsRead;
}

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INVALID_CURSOR_LOOKUP;
}

namespace
{

constexpr auto METADATA_TIMEOUT = std::chrono::milliseconds(10'000);

}

KafkaStreamSource::KafkaStreamSource(
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
{
}

KafkaStreamSource::~KafkaStreamSource() = default;

void KafkaStreamSource::start()
{
    /// The consumer reads the partitions assigned explicitly, without a subscription. With the end of a partition
    /// reported as an event, a bounded stream knows when it has read everything.
    params.consumer_configuration.set("enable.partition.eof", "true");
    consumer = std::make_unique<cppkafka::Consumer>(params.consumer_configuration);

    const std::unordered_set<String> topics(params.topics.begin(), params.topics.end());

    /// A topic name may contain dots, and the cursor tree splits the keys of the query by dots. So the offsets are taken
    /// from the collapsed form of the tree, where the path of an offset is `<topic>.<partition>`, and the partition
    /// is the last part of it.
    std::map<TopicPartition, Int64> cursor_offsets;
    if (stream_settings.cursor)
    {
        for (const auto & entry : cursorTreeToMap(stream_settings.cursor))
        {
            const auto & tuple = entry.safeGet<Tuple>();
            const auto & path = tuple.at(0).safeGet<String>();
            const auto dot = path.rfind('.');
            Int32 partition = 0;
            if (dot == String::npos || !tryParse(partition, path.substr(dot + 1)))
                throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP,
                    "The cursor of a Kafka table must map a topic to a map from a partition number to an offset, but it has the path '{}'", path);
            cursor_offsets[TopicPartition{path.substr(0, dot), partition}] = tuple.at(1).safeGet<Int64>();
        }
    }

    const auto metadata = consumer->get_metadata(/* all_topics = */ true, METADATA_TIMEOUT);

    cppkafka::TopicPartitionList assignment;
    for (const auto & topic_metadata : metadata.get_topics())
    {
        const auto & topic = topic_metadata.get_name();
        if (!topics.contains(topic))
            continue;

        for (const auto & partition_metadata : topic_metadata.get_partitions())
        {
            const auto partition = static_cast<Int32>(partition_metadata.get_id());

            PartitionState state;
            if (auto it = cursor_offsets.find(TopicPartition{topic, partition}); it != cursor_offsets.end())
                state.last_offset = it->second;

            if (state.last_offset < -1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Invalid offset {} in the cursor for partition {} of topic {}", state.last_offset, partition, topic);

            const Int64 start_offset = state.last_offset >= 0 ? state.last_offset + 1 : cppkafka::TopicPartition::OFFSET_BEGINNING;

            /// A bounded stream reads the messages that exist at its start: up to the high watermark, unless it gets
            /// the end of the partition event first (e.g. when the last offsets are taken by transaction markers).
            if (!stream_settings.subscribe_for_updates)
            {
                const auto [low, high] = consumer->query_offsets(cppkafka::TopicPartition(topic, partition), METADATA_TIMEOUT);
                state.end_offset = high;
                if (high <= low || (state.last_offset >= 0 && state.last_offset + 1 >= high))
                    state.reached_end = true;
            }
            assignment.emplace_back(topic, partition, start_offset);
            partitions.emplace(TopicPartition{topic, partition}, state);

            LOG_DEBUG(log, "Streaming topic {} partition {} from offset {}", topic, partition,
                state.last_offset >= 0 ? toString(start_offset) : "beginning");
        }
    }

    consumer->assign(assignment);
}

bool KafkaStreamSource::allPartitionsReachedEnd() const
{
    for (const auto & [_, state] : partitions)
        if (!state.reached_end)
            return false;
    return true;
}

void KafkaStreamSource::reportFinalCursor() const
{
    /// Only an incremental refresh of a materialized view has the holder of the cursor.
    auto streaming_cursor = context->getStreamingCursor();
    if (!streaming_cursor)
        return;

    auto cursor = std::make_shared<CursorTreeNode>();
    for (const auto & [topic_partition, state] : partitions)
        if (state.last_offset >= 0)
            cursor->getSubtreeOrCreate(topic_partition.first)->setValue(toString(topic_partition.second), state.last_offset);
    streaming_cursor->merge(cursor);
}

std::optional<Chunk> KafkaStreamSource::tryGenerate()
{
    /// An idle stream returns an empty chunk after a poll, so that the thread runs other processors in the meantime
    /// (e.g. the other tables of a `Merge` table) instead of waiting here.
    auto chunk = generate();
    if (!chunk && (stream_finished || isCancelled()))
        return std::nullopt;
    return chunk;
}

Chunk KafkaStreamSource::generate()
{
    if (stream_finished)
        return {};

    if (!consumer)
        start();

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
    bool is_dead_letter = false;
    const cppkafka::Message * current_message = nullptr;

    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e) -> size_t
    {
        ProfileEvents::increment(ProfileEvents::KafkaMessagesFailed);

        switch (params.handle_error_mode)
        {
            case StreamingHandleErrorMode::STREAM:
            {
                exception_message = e.message();
                for (size_t i = 0; i < result_columns.size(); ++i)
                {
                    result_columns[i]->rollback(*checkpoints[i]);
                    result_columns[i]->insertDefault();
                }
                return 1;
            }
            case StreamingHandleErrorMode::DEAD_LETTER_QUEUE:
            {
                exception_message = e.message();
                for (size_t i = 0; i < result_columns.size(); ++i)
                    result_columns[i]->rollback(*checkpoints[i]);
                is_dead_letter = true;
                return 0;
            }
            case StreamingHandleErrorMode::DEFAULT:
            {
                e.addMessage("while parsing Kafka message (topic: {}, partition: {}, offset: {})",
                    current_message->get_topic(), current_message->get_partition(), current_message->get_offset());
                throw std::move(e);
            }
        }
    };

    StreamingFormatExecutor executor(non_virtual_header, std::move(create_format), std::move(on_error));

    const Field table_name = params.storage_id.getTableName();
    size_t total_rows = 0;

    while (!isCancelled())
    {
        if (bounded && allPartitionsReachedEnd())
        {
            reportFinalCursor();
            stream_finished = true;
            break;
        }

        auto messages = consumer->poll_batch(params.poll_max_batch_size, std::chrono::milliseconds(params.poll_timeout_ms));

        for (const auto & message : messages)
        {
            if (!message)
                continue;

            if (message.get_error())
            {
                if (message.is_eof())
                {
                    if (bounded)
                    {
                        auto it = partitions.find(TopicPartition{message.get_topic(), message.get_partition()});
                        if (it != partitions.end() && !it->second.reached_end)
                        {
                            it->second.reached_end = true;
                            consumer->pause_partitions({cppkafka::TopicPartition(message.get_topic(), message.get_partition())});
                        }
                    }
                    continue;
                }
                throw cppkafka::ConsumerException(message.get_error());
            }

            auto it = partitions.find(TopicPartition{message.get_topic(), message.get_partition()});
            /// A bounded stream does not read past the end it has seen; the messages polled before pausing are skipped.
            if (it == partitions.end() || (bounded && it->second.reached_end))
                continue;

            ProfileEvents::increment(ProfileEvents::KafkaMessagesRead);

            current_message = &message;
            exception_message.reset();
            is_dead_letter = false;

            size_t new_rows = 0;
            const auto & payload = message.get_payload();
            if (payload.get_data() && payload.get_size() >= params.schema_registry_skip_bytes)
            {
                ReadBufferFromMemory buf(payload.get_data() + params.schema_registry_skip_bytes, payload.get_size() - params.schema_registry_skip_bytes);
                new_rows = executor.execute(buf);
            }

            it->second.last_offset = message.get_offset();
            if (bounded && it->second.last_offset + 1 >= it->second.end_offset)
            {
                it->second.reached_end = true;
                consumer->pause_partitions({cppkafka::TopicPartition(message.get_topic(), message.get_partition())});
            }

            if (new_rows)
            {
                ProfileEvents::increment(ProfileEvents::KafkaRowsRead, new_rows);

                Array headers_names;
                Array headers_values;
                for (const auto & header : message.get_header_list())
                {
                    headers_names.emplace_back(header.get_name());
                    headers_values.emplace_back(static_cast<std::string>(header.get_value()));
                }

                const auto timestamp = message.get_timestamp();
                const String key = static_cast<std::string>(message.get_key());

                for (size_t i = 0; i < new_rows; ++i)
                {
                    for (size_t column_index = 0; column_index < virtual_columns.size(); ++column_index)
                    {
                        const auto & name = virtual_header.getByPosition(column_index).name;
                        auto & column = virtual_columns[column_index];
                        if (name == "_topic")
                            column->insert(message.get_topic());
                        else if (name == "_key")
                            column->insert(key);
                        else if (name == "_offset")
                            column->insert(static_cast<UInt64>(message.get_offset()));
                        else if (name == "_partition")
                            column->insert(static_cast<UInt64>(message.get_partition()));
                        else if (name == "_timestamp" && timestamp)
                            column->insert(std::chrono::duration_cast<std::chrono::seconds>(timestamp->get_timestamp()).count());
                        else if (name == "_timestamp_ms" && timestamp)
                            column->insert(DecimalField<Decimal64>(
                                std::chrono::duration_cast<std::chrono::milliseconds>(timestamp->get_timestamp()).count(), 3));
                        else if (name == "_headers.name")
                            column->insert(headers_names);
                        else if (name == "_headers.value")
                            column->insert(headers_values);
                        else if (name == "_table")
                            column->insert(table_name);
                        else if (name == "_raw_message" && exception_message)
                            column->insertData(reinterpret_cast<const char *>(payload.get_data()), payload.get_size());
                        else if (name == "_error" && exception_message)
                            column->insertData(exception_message->data(), exception_message->size());
                        else
                            column->insertDefault();
                    }
                }

                total_rows += new_rows;
            }

            if (is_dead_letter)
            {
                const auto time_now = std::chrono::system_clock::now();
                if (auto dead_letter_queue = context->getDeadLetterQueue())
                    dead_letter_queue->add([&](DeadLetterQueueElement & element) { element = DeadLetterQueueElement{
                            .table_engine = DeadLetterQueueElement::StreamType::Kafka,
                            .event_time = timeInSeconds(time_now),
                            .event_time_microseconds = timeInMicroseconds(time_now),
                            .database = params.storage_id.database_name,
                            .table = params.storage_id.table_name,
                            .raw_message = String(reinterpret_cast<const char *>(payload.get_data()), payload.get_size()),
                            .error = exception_message.value(),
                            .details = DeadLetterQueueElement::KafkaDetails{
                                .topic_name = message.get_topic(),
                                .partition = message.get_partition(),
                                .offset = message.get_offset(),
                                .key = static_cast<std::string>(message.get_key())}}; });
                else
                    LOG_WARNING(log, "Table system.dead_letter_queue is not configured, skipping message");
            }
        }

        current_message = nullptr;

        /// Return the rows as soon as there are some: a streaming query should not delay them.
        /// Without rows, return after one poll as well, see `tryGenerate`.
        break;
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
