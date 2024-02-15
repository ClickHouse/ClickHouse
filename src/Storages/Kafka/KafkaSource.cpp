#include <Storages/Kafka/KafkaSource.h>

#include <Formats/FormatFactory.h>
#include <IO/EmptyReadBuffer.h>
#include <Storages/Kafka/KafkaConsumer.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event KafkaMessagesRead;
    extern const Event KafkaMessagesFailed;
    extern const Event KafkaRowsRead;
    extern const Event KafkaRowsRejected;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

// with default poll timeout (500ms) it will give about 5 sec delay for doing 10 retries
// when selecting from empty topic
const auto MAX_FAILED_POLL_ATTEMPTS = 10;

KafkaSource::KafkaSource(
    StorageKafka & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    LoggerPtr log_,
    size_t max_block_size_,
    bool commit_in_suffix_)
    : ISource(storage_snapshot_->getSampleBlockForColumns(columns))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , context(context_)
    , column_names(columns)
    , log(log_)
    , max_block_size(max_block_size_)
    , commit_in_suffix(commit_in_suffix_)
    , non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized())
    , virtual_header(storage_snapshot->getSampleBlockForColumns(storage.getVirtualColumnNames()))
    , handle_error_mode(storage.getStreamingHandleErrorMode())
{
}

KafkaSource::~KafkaSource()
{
    if (!consumer)
        return;

    if (broken)
        consumer->unsubscribe();

    storage.pushConsumer(consumer);
}

bool KafkaSource::checkTimeLimit() const
{
    if (max_execution_time != 0)
    {
        auto elapsed_ns = total_stopwatch.elapsed();

        if (elapsed_ns > static_cast<UInt64>(max_execution_time.totalMicroseconds()) * 1000)
            return false;
    }

    return true;
}

Chunk KafkaSource::generateImpl()
{
    if (!consumer)
    {
        auto timeout = std::chrono::milliseconds(context->getSettingsRef().kafka_max_wait_ms.totalMilliseconds());
        consumer = storage.popConsumer(timeout);

        if (!consumer)
            return {};

        consumer->subscribe();

        broken = true;
    }

    if (is_finished)
        return {};

    is_finished = true;
    // now it's one-time usage InputStream
    // one block of the needed size (or with desired flush timeout) is formed in one internal iteration
    // otherwise external iteration will reuse that and logic will became even more fuzzy
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        storage.getFormatName(), empty_buf, non_virtual_header, context, max_block_size, std::nullopt, 1);

    std::vector<std::string> exception_messages;
    size_t total_rows = 0;
    size_t failed_poll_attempts = 0;

    size_t input_format_allow_errors_num = context->getSettingsRef().input_format_allow_errors_num;
    size_t num_rows_with_errors = 0;
    auto on_error = [&](std::exception_ptr exception_ptr)
    {
        ProfileEvents::increment(ProfileEvents::KafkaMessagesFailed);

        if (handle_error_mode != StreamingHandleErrorMode::STREAM)
        {
            if (input_format_allow_errors_num >= ++num_rows_with_errors)
                return;

            if (Exception * e = exception_cast<Exception *>(exception_ptr))
                e->addMessage("while parsing Kafka message (topic: {}, partition: {}, offset: {})",
                    consumer->currentTopic(), consumer->currentPartition(), consumer->currentOffset());
            std::rethrow_exception(exception_ptr);
        }
        else
            exception_messages.emplace_back(getExceptionMessage(exception_ptr, false));
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, std::move(on_error));

    while (true)
    {
        size_t new_rows = 0;
        exception_messages.clear();
        if (auto buf = consumer->consume())
        {
            ProfileEvents::increment(ProfileEvents::KafkaMessagesRead);
            new_rows = executor.execute(*buf);
        }

        if (new_rows || !exception_messages.empty())
        {
            // In read_kafka_message(), KafkaConsumer::nextImpl()
            // will be called, that may make something unusable, i.e. clean
            // KafkaConsumer::messages, which is accessed from
            // KafkaConsumer::currentTopic() (and other helpers).
            if (consumer->isStalled())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Polled messages became unusable");

            ProfileEvents::increment(ProfileEvents::KafkaRowsRead, new_rows);

            consumer->storeLastReadMessageOffset();

            size_t new_rows_with_errors = new_rows + exception_messages.size();
            for (size_t i = 0; i < new_rows_with_errors; ++i)
            {
                /// FIXME: one kafka message may have multiple output rows, and will cause UB in this case
                ///
                /// This can be fixed by i.e. providing some on_message callback,
                /// but the problem is that it should be implemented for IRowInputFormat and for each format that are not row-based.
                auto topic         = consumer->topic(i);
                auto key           = consumer->key(i);
                auto offset        = consumer->offset(i);
                auto partition     = consumer->partition(i);
                auto timestamp_raw = consumer->timestamp(i);
                auto header_list   = consumer->headerList(i);

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

                virtual_columns[0]->insert(topic);
                virtual_columns[1]->insert(key);
                virtual_columns[2]->insert(offset);
                virtual_columns[3]->insert(partition);
                if (timestamp_raw)
                {
                    auto ts = timestamp_raw->get_timestamp();
                    virtual_columns[4]->insert(std::chrono::duration_cast<std::chrono::seconds>(ts).count());
                    virtual_columns[5]->insert(DecimalField<Decimal64>(std::chrono::duration_cast<std::chrono::milliseconds>(ts).count(),3));
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
                    /// FIXME: errors could be in the middle of the batch... Sigh.
                    /// this can be fixed by using current-messages.begin() in the on_error handler, but it is a very dirty hack.
                    if (i >= new_rows)
                    {
                        const auto & payload = consumer->payload(i);
                        virtual_columns[8]->insert(Field(reinterpret_cast<const char *>(payload.get_data()), payload.get_size()));
                        const auto & exception_message = exception_messages[i - new_rows];
                        virtual_columns[9]->insertData(exception_message.data(), exception_message.size());
                    }
                    else
                    {
                        virtual_columns[8]->insertDefault();
                        virtual_columns[9]->insertDefault();
                    }
                }
            }

            total_rows += new_rows_with_errors;
        }
        else if (consumer->polledDataUnusable())
        {
            break;
        }
        else if (consumer->isStalled())
        {
            ++failed_poll_attempts;
        }
        else
        {
            // We came here in case of tombstone (or sometimes zero-length) messages, and it is not something abnormal
            // TODO: it seems like in case of StreamingHandleErrorMode::STREAM we may need to process those differently
            // currently we just skip them with note in logs.
            consumer->storeLastReadMessageOffset();
            LOG_DEBUG(log, "Parsing of message (topic: {}, partition: {}, offset: {}) return no rows.",
                consumer->currentTopic(), consumer->currentPartition(), consumer->currentOffset());
        }

        if (!consumer->hasMorePolledMessages()
            && (total_rows >= max_block_size || !checkTimeLimit() || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS))
        {
            break;
        }
    }

    if (total_rows == 0)
    {
        return {};
    }
    else if (consumer->polledDataUnusable())
    {
        // the rows were counted already before by KafkaRowsRead,
        // so let's count the rows we ignore separately
        // (they will be retried after the rebalance)
        ProfileEvents::increment(ProfileEvents::KafkaRowsRejected, total_rows);
        return {};
    }

    /// MATERIALIZED columns can be added here, but I think
    // they are not needed here:
    // and it's misleading to use them here,
    // as columns 'materialized' that way stays 'ephemeral'
    // i.e. will not be stored anythere
    // If needed any extra columns can be added using DEFAULT they can be added at MV level if needed.

    auto result_block  = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    size_t result_block_rows = result_block.rows();
    size_t virtual_block_rows = virtual_block.rows();
    size_t errors = virtual_block_rows - result_block_rows;
    if (errors)
    {
        auto result_columns = result_block.mutateColumns();
        for (auto & column : result_columns)
            column->insertManyDefaults(errors);
        result_block.setColumns(std::move(result_columns));
    }

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    auto converting_dag = ActionsDAG::makeConvertingActions(
        result_block.cloneEmpty().getColumnsWithTypeAndName(),
        getPort().getHeader().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    converting_actions->execute(result_block);

    return Chunk(result_block.getColumns(), result_block.rows());
}

Chunk KafkaSource::generate()
{
    auto chunk = generateImpl();
    if (!chunk && commit_in_suffix)
        commit();

    return chunk;
}

void KafkaSource::commit()
{
    if (!consumer)
        return;

    consumer->commit();

    broken = false;
}

}
