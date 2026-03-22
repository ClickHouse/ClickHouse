#include <Storages/Kafka/Kafka2Source.h>

#include <Storages/Kafka/StorageKafka2.h>
#include <Storages/Kafka/KafkaConsumer2.h>
#include <Storages/Kafka/KafkaSettings.h>

#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/DeadLetterQueue.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Common/DateLUT.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event KafkaMessagesRead;
extern const Event KafkaMessagesFailed;
extern const Event KafkaRowsRead;
}

namespace DB
{

// with default poll timeout (500ms) it will give about 5 sec delay for doing 10 retries
// when selecting from empty topic
const auto MAX_FAILED_POLL_ATTEMPTS = 10;

Kafka2Source::Kafka2Source(
    StorageKafka2 & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    LoggerPtr log_,
    size_t max_block_size_,
    size_t consumer_index_,
    bool commit_in_suffix_)
    : ISource(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(columns)))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns)
    , log(log_)
    , max_block_size(max_block_size_)
    , consumer_index(consumer_index_)
    , commit_in_suffix(commit_in_suffix_)
{
}

Kafka2Source::~Kafka2Source()
{
    if (!consumer)
        return;

    // If broken (not committed), the OffsetGuard destructor will rollback
    offset_guard.reset();
    storage.releaseConsumer(std::move(consumer));
}

bool Kafka2Source::checkTimeLimit() const
{
    if (max_execution_time != 0)
    {
        auto elapsed_ns = total_stopwatch.elapsed();

        if (elapsed_ns > static_cast<UInt64>(max_execution_time.totalMicroseconds()) * 1000)
            return false;
    }

    return true;
}

Chunk Kafka2Source::generate()
{
    auto chunk = generateImpl();
    if (!chunk && commit_in_suffix)
        commit();

    return chunk;
}

Chunk Kafka2Source::generateImpl()
{
    if (!consumer)
    {
        consumer = storage.acquireConsumer(consumer_index);

        if (consumer->needsNewKeeper())
            consumer->setKeeper(storage.getZooKeeperAndAssertActive());

        if (const auto cannot_poll_reason = consumer->prepareToPoll(); cannot_poll_reason.has_value())
        {
            LOG_DEBUG(log, "Cannot poll consumer for direct read");
            stalled = true;
            return {};
        }

        broken = true;
    }

    if (is_finished)
        return {};

    is_finished = true;

    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();
    auto virtual_header = storage.getVirtualsHeader();

    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        storage.getFormatName(),
        empty_buf,
        non_virtual_header,
        context,
        max_block_size,
        std::nullopt,
        FormatParserSharedResources::singleThreaded(context->getSettingsRef()));

    std::optional<std::string> exception_message;
    size_t total_rows = 0;
    size_t failed_poll_attempts = 0;
    bool is_dead_letter = false;
    bool consumed_any_messages = false;
    auto handle_error_mode = storage.getHandleKafkaErrorMode();

    const KeeperHandlingConsumer::MessageInfo * current_msg_info = nullptr;

    auto on_error = [&](const MutableColumns & result_columns, const ColumnCheckpoints & checkpoints, Exception & e)
    {
        ProfileEvents::increment(ProfileEvents::KafkaMessagesFailed);

        switch (handle_error_mode)
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
                e.addMessage(
                    "while parsing Kafka message (topic: {}, partition: {}, offset: {})",
                    current_msg_info->currentTopic(),
                    current_msg_info->currentPartition(),
                    current_msg_info->currentOffset());
                throw std::move(e);
            }
        }
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, std::move(on_error));

    KeeperHandlingConsumer::MessageSinkFunction msg_sink
        = [&](ReadBufferPtr buf, const KeeperHandlingConsumer::MessageInfo & msg_info, bool has_more_polled_messages, bool msg_stalled) mutable
    {
        size_t new_rows = 0;
        exception_message.reset();
        is_dead_letter = false;

        if (buf)
        {
            consumed_any_messages = true;
            current_msg_info = &msg_info;
            ProfileEvents::increment(ProfileEvents::KafkaMessagesRead);
            new_rows = executor.execute(*buf);
        }

        if (new_rows || is_dead_letter)
        {
            ProfileEvents::increment(ProfileEvents::KafkaRowsRead, new_rows);

            const auto & header_list = msg_info.currentHeaderList();

            Array headers_names;
            Array headers_values;

            if (!header_list.empty())
            {
                headers_names.reserve(header_list.size());
                headers_values.reserve(header_list.size());
                for (const auto & header : header_list)
                {
                    headers_names.emplace_back(header.get_name());
                    headers_values.emplace_back(static_cast<std::string>(header.get_value()));
                }
            }

            for (size_t i = 0; i < new_rows; ++i)
            {
                virtual_columns[0]->insert(msg_info.currentTopic());
                virtual_columns[1]->insert(msg_info.currentKey());
                virtual_columns[2]->insert(msg_info.currentOffset());
                virtual_columns[3]->insert(msg_info.currentPartition());

                auto timestamp_raw = msg_info.currentTimestamp();
                if (timestamp_raw)
                {
                    auto ts = timestamp_raw->get_timestamp();
                    virtual_columns[4]->insert(std::chrono::duration_cast<std::chrono::seconds>(ts).count());
                    virtual_columns[5]->insert(
                        DecimalField<Decimal64>(std::chrono::duration_cast<std::chrono::milliseconds>(ts).count(), 3));
                }
                else
                {
                    virtual_columns[4]->insertDefault();
                    virtual_columns[5]->insertDefault();
                }
                virtual_columns[6]->insert(headers_names);
                virtual_columns[7]->insert(headers_values);

                if (handle_error_mode == StreamingHandleErrorMode::STREAM)
                {
                    if (exception_message)
                    {
                        virtual_columns[8]->insert(msg_info.currentPayload());
                        virtual_columns[9]->insert(*exception_message);
                    }
                    else
                    {
                        virtual_columns[8]->insertDefault();
                        virtual_columns[9]->insertDefault();
                    }
                }
            }

            if (is_dead_letter)
            {
                assert(exception_message);
                const auto time_now = std::chrono::system_clock::now();
                auto storage_id = storage.getStorageID();

                auto dead_letter_queue = context->getDeadLetterQueue();
                if (!dead_letter_queue)
                    LOG_WARNING(log, "Table system.dead_letter_queue is not configured, skipping message");
                else
                    dead_letter_queue->add(
                        DeadLetterQueueElement{
                            .table_engine = DeadLetterQueueElement::StreamType::Kafka,
                            .event_time = timeInSeconds(time_now),
                            .event_time_microseconds = timeInMicroseconds(time_now),
                            .database = storage_id.database_name,
                            .table = storage_id.table_name,
                            .raw_message = msg_info.currentPayload(),
                            .error = exception_message.value(),
                            .details = DeadLetterQueueElement::KafkaDetails{
                                .topic_name = msg_info.currentTopic(),
                                .partition = msg_info.currentPartition(),
                                .offset = msg_info.currentOffset(),
                                .key = msg_info.currentKey()}});
            }

            total_rows = total_rows + new_rows;
        }
        else if (msg_stalled)
        {
            ++failed_poll_attempts;
        }
        else
        {
            LOG_DEBUG(
                log,
                "Parsing of message (topic: {}, partition: {}, offset: {}) return no rows.",
                msg_info.currentTopic(),
                msg_info.currentPartition(),
                msg_info.currentOffset());
        }

        if (!has_more_polled_messages
            && (total_rows >= max_block_size || !checkTimeLimit() || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS))
        {
            return true;
        }
        return false;
    };

    auto maybe_guard = consumer->poll(msg_sink);

    if (!maybe_guard.has_value() || (!consumed_any_messages && total_rows == 0))
    {
        stalled = true;
        return {};
    }

    offset_guard.emplace(std::move(*maybe_guard));

    if (total_rows == 0)
    {
        /// Messages were consumed but produced no rows (e.g. all went to dead-letter queue).
        /// We still keep the offset guard so that commit can advance offsets.
        stalled = true;
        return {};
    }

    auto result_block = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    auto converting_dag = ActionsDAG::makeConvertingActions(
        result_block.cloneEmpty().getColumnsWithTypeAndName(),
        getPort().getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    converting_actions->execute(result_block);

    return Chunk(result_block.getColumns(), result_block.rows());
}

void Kafka2Source::commit()
{
    if (!consumer)
        return;

    if (offset_guard.has_value())
    {
        offset_guard->commit();
        offset_guard.reset();
    }

    broken = false;
}

}
