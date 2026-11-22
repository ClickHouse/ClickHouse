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
#include <Interpreters/getColumnFromBlock.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Common/Epoll.h>
#include <Common/ErrnoException.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <chrono>
#include <limits>

#include <librdkafka/rdkafka.h>
#include <unistd.h>

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
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int INVALID_CURSOR_LOOKUP;
}

namespace
{

constexpr auto METADATA_TIMEOUT = std::chrono::milliseconds(10'000);

/// How often an unbounded stream checks the topics for new partitions.
constexpr UInt64 PARTITIONS_DISCOVERY_INTERVAL_MS = 5'000;

/// The cursor tree splits its keys by dots, so a topic name is a single key, where `.` is written as `%2E` and `%` as `%25`.
String escapeTopicName(const String & topic)
{
    String result;
    result.reserve(topic.size());
    for (const char c : topic)
    {
        if (c == '.')
            result += "%2E";
        else if (c == '%')
            result += "%25";
        else
            result += c;
    }
    return result;
}

String unescapeTopicName(const String & key)
{
    String result;
    result.reserve(key.size());
    for (size_t i = 0; i < key.size(); ++i)
    {
        if (key[i] != '%')
        {
            result += key[i];
            continue;
        }
        const std::string_view escaped = std::string_view(key).substr(i, 3);
        if (escaped == "%2E" || escaped == "%2e")
            result += '.';
        else if (escaped == "%25")
            result += '%';
        else
            throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP,
                "Invalid escape sequence in the topic name '{}' in the cursor: a dot is written as %2E and a percent sign as %25", key);
        i += 2;
    }
    return result;
}

[[noreturn]] void throwInvalidCursorShape(const String & key)
{
    throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP,
        "The cursor of a Kafka table must map a topic to a map from a partition number to an offset, "
        "{{'<topic>': {{'<partition>': <offset of the last read message>}}}}, where a dot in the topic name is written as %2E, "
        "but it has the key '{}'", key);
}

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
    /// The same lookup as in `StorageSnapshot::getSampleBlockForColumns`, which the header of the source is built with.
    const auto & columns_description = storage_snapshot->metadata->getColumns();
    requested_columns.reserve(columns.size());
    for (const auto & name : columns)
    {
        if (auto column = columns_description.tryGetColumnOrSubcolumn(GetColumnsOptions::All, name))
            requested_columns.push_back(std::move(*column));
        else
            requested_columns.emplace_back(name, getPort().getHeader().getByName(name).type);
    }
}

KafkaStreamSource::~KafkaStreamSource()
{
    /// Stop writing to the pipe before it is closed.
    if (consumer)
        rd_kafka_queue_io_event_enable(consumer->get_consumer_queue().get_handle(), -1, nullptr, 0);
}

void KafkaStreamSource::start()
{
    if (stream_settings.cursor)
    {
        for (const auto & [key, topic_entry] : *stream_settings.cursor)
        {
            const auto * topic_cursor = std::get_if<CursorTreeNodePtr>(&topic_entry);
            if (!topic_cursor)
                throwInvalidCursorShape(key);
            const String topic = unescapeTopicName(key);

            for (const auto & [partition_key, partition_entry] : **topic_cursor)
            {
                const auto * offset = std::get_if<Int64>(&partition_entry);
                Int32 partition = 0;
                if (!offset || !tryParse(partition, partition_key) || partition < 0)
                    throwInvalidCursorShape(key + "." + partition_key);

                /// -1 means that nothing was read. The next offset must be representable.
                if (*offset < -1 || *offset == std::numeric_limits<Int64>::max())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Invalid offset {} in the cursor for partition {} of topic {}", *offset, partition, topic);

                cursor_offsets[TopicPartition{topic, partition}] = *offset;
            }
        }
    }

    /// The consumer reads the partitions assigned explicitly, without a subscription. With the end of a partition
    /// reported as an event, a bounded stream knows when it has read everything.
    params.consumer_configuration.set("enable.partition.eof", "true");
    /// An offset outside of a partition (e.g. a cursor beyond its end, or before its beginning after the retention)
    /// is an error, instead of reading the partition again from the beginning or skipping to its end.
    params.consumer_configuration.set("auto.offset.reset", "error");
    consumer = std::make_unique<cppkafka::Consumer>(params.consumer_configuration);

    wakeup_pipe.open();
    wakeup_pipe.setNonBlockingReadWrite();
    static constexpr char wakeup_payload = 1;
    rd_kafka_queue_io_event_enable(consumer->get_consumer_queue().get_handle(), wakeup_pipe.fds_rw[1], &wakeup_payload, 1);

    discoverPartitions();
    assignPartitions();
    since_partitions_discovery.restart();
}

bool KafkaStreamSource::discoverPartitions()
{
    const auto metadata = consumer->get_metadata(/* all_topics = */ true, METADATA_TIMEOUT);

    std::map<String, const cppkafka::TopicMetadata *> topics_metadata;
    const auto & all_topics_metadata = metadata.get_topics();
    for (const auto & topic_metadata : all_topics_metadata)
        topics_metadata.emplace(topic_metadata.get_name(), &topic_metadata);

    bool added = false;
    for (const auto & topic : params.topics)
    {
        /// The metadata of all topics does not have the topics that do not exist or that the consumer is not allowed
        /// to describe: reading only the rest of the topics would be a silently incomplete result.
        auto metadata_it = topics_metadata.find(topic);
        if (metadata_it == topics_metadata.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Topic {} does not exist or is not authorized to describe", topic);
        const auto & topic_metadata = *metadata_it->second;
        if (topic_metadata.get_error())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot get the metadata of topic {}: {}", topic, topic_metadata.get_error().to_string());

        for (const auto & partition_metadata : topic_metadata.get_partitions())
        {
            const auto partition = static_cast<Int32>(partition_metadata.get_id());
            if (partitions.contains(TopicPartition{topic, partition}))
                continue;

            PartitionState state;
            if (auto it = cursor_offsets.find(TopicPartition{topic, partition}); it != cursor_offsets.end())
                state.last_offset = it->second;

            /// A bounded stream reads the messages that exist at its start: up to the high watermark, unless it gets
            /// the end of the partition event first (e.g. when the last offsets are taken by transaction markers).
            if (!stream_settings.subscribe_for_updates)
            {
                const auto [low, high] = consumer->query_offsets(cppkafka::TopicPartition(topic, partition), METADATA_TIMEOUT);
                const Int64 next_offset = state.last_offset + 1;
                if (state.last_offset >= 0 && (next_offset > high || next_offset < low))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "The offset {} in the cursor for partition {} of topic {} is outside of the partition, which has the offsets from {} to {}",
                        state.last_offset, partition, topic, low, high - 1);

                state.end_offset = high;
                if (high <= low || next_offset >= high)
                    state.reached_end = true;
            }

            partitions.emplace(TopicPartition{topic, partition}, state);
            added = true;
        }
    }
    return added;
}

void KafkaStreamSource::assignPartitions()
{
    /// The assignment replaces the previous one, so it has the positions of all partitions: after the last read message.
    cppkafka::TopicPartitionList assignment;
    for (const auto & [topic_partition, state] : partitions)
    {
        const auto & [topic, partition] = topic_partition;
        /// A bounded stream does not read a partition that it finished.
        if (state.reached_end)
            continue;

        const Int64 start_offset = state.last_offset >= 0 ? state.last_offset + 1 : cppkafka::TopicPartition::OFFSET_BEGINNING;
        assignment.emplace_back(topic, partition, start_offset);

        LOG_DEBUG(log, "Streaming topic {} partition {} from offset {}", topic, partition,
            state.last_offset >= 0 ? toString(start_offset) : "beginning");
    }

    consumer->assign(assignment);
}

void KafkaStreamSource::drainWakeupPipe()
{
    char buf[64];
    while (true)
    {
        const ssize_t res = ::read(wakeup_pipe.fds_rw[0], buf, sizeof(buf));
        if (res > 0)
            continue;
        if (res == 0 || errno == EAGAIN || errno == EWOULDBLOCK)
            return;
        if (errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from the wakeup pipe of the Kafka consumer");
    }
}

bool KafkaStreamSource::allPartitionsReachedEnd() const
{
    for (const auto & [_, state] : partitions)
        if (!state.reached_end)
            return false;
    return true;
}

void KafkaStreamSource::finishIfAllPartitionsReachedEnd()
{
    if (!stream_settings.subscribe_for_updates && !stream_finished && allPartitionsReachedEnd())
    {
        reportFinalCursor();
        stream_finished = true;
    }
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
            cursor->getSubtreeOrCreate(escapeTopicName(topic_partition.first))->setValue(toString(topic_partition.second), state.last_offset);
    streaming_cursor->merge(cursor);
}

IProcessor::Status KafkaStreamSource::prepare()
{
    auto status = ISource::prepare();
    /// Nothing to read now: wait until the consumer queue gets an event, see `scheduleForEvent`.
    if (status == Status::Ready && idle && !isCancelled())
        return Status::Async;
    return status;
}

void KafkaStreamSource::work()
{
    idle = false;
    ISource::work();
}

int KafkaStreamSource::schedule()
{
    return wakeup_pipe.fds_rw[0];
}

#if defined(OS_LINUX) || defined(OS_DARWIN)
std::tuple<int, uint32_t, Int64> KafkaStreamSource::scheduleForEvent()
{
    /// Poll again after `poll_timeout_ms` anyway: an unbounded stream checks for new partitions.
    return {wakeup_pipe.fds_rw[0], EPOLLIN | EPOLLERR, static_cast<Int64>(std::max<size_t>(params.poll_timeout_ms, 1))};
}
#endif

std::optional<Chunk> KafkaStreamSource::tryGenerate()
{
    /// An idle stream returns an empty chunk, and then waits for the consumer asynchronously, see `prepare`.
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

    finishIfAllPartitionsReachedEnd();
    if (stream_finished || isCancelled())
        return {};

    if (!bounded && since_partitions_discovery.elapsedMilliseconds() >= PARTITIONS_DISCOVERY_INTERVAL_MS)
    {
        if (discoverPartitions())
            assignPartitions();
        since_partitions_discovery.restart();
    }

    /// Drain the pipe before the poll: the consumer queue writes to it again only after the poll finds it empty.
    drainWakeupPipe();
    auto messages = consumer->poll_batch(params.poll_max_batch_size, std::chrono::milliseconds(0));
    if (messages.empty())
        idle = true;

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

        /// The offsets after the last message can be taken by transaction markers, which are not returned, so a message
        /// appended after the start of the read can come before the end of the partition event.
        if (bounded && message.get_offset() >= it->second.end_offset)
        {
            it->second.reached_end = true;
            consumer->pause_partitions({cppkafka::TopicPartition(message.get_topic(), message.get_partition())});
            continue;
        }

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

    /// The rows are returned before the stream finishes, see `tryGenerate`.
    finishIfAllPartitionsReachedEnd();

    if (!total_rows)
        return {};

    auto result_block = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));
    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    /// The query can request subcolumns (e.g. an element of a `Tuple` or a path of a `JSON`), while the format parses
    /// the whole columns, so the requested columns are extracted from the parsed block.
    const size_t num_rows = result_block.rows();
    Columns result_columns;
    result_columns.reserve(requested_columns.size());
    for (const auto & requested_column : requested_columns)
        result_columns.push_back(getColumnFromBlock(result_block, requested_column));

    return Chunk(std::move(result_columns), num_rows);
}

}
