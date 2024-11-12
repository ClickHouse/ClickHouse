#include <Storages/Kafka/KafkaSource.h>

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/EmptyReadBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/Kafka/KafkaConsumer.h>
#include <Common/logger_useful.h>

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
namespace Setting
{
    extern const SettingsMilliseconds kafka_max_wait_ms;
}

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
    , virtual_header(storage.getVirtualsHeader())
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
        auto timeout = std::chrono::milliseconds(context->getSettingsRef()[Setting::kafka_max_wait_ms].totalMilliseconds());
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

    auto put_error_to_stream = handle_error_mode == StreamingHandleErrorMode::STREAM;

    EmptyReadBuffer empty_buf;
    auto input_format = FormatFactory::instance().getInput(
        storage.getFormatName(), empty_buf, non_virtual_header, context, max_block_size, std::nullopt, 1);

    std::optional<std::string> exception_message;
    size_t total_rows = 0;
    size_t failed_poll_attempts = 0;

    auto on_error = [&](const MutableColumns & result_columns, Exception & e)
    {
        ProfileEvents::increment(ProfileEvents::KafkaMessagesFailed);

        if (put_error_to_stream)
        {
            exception_message = e.message();
            for (const auto & column : result_columns)
            {
                // read_kafka_message could already push some rows to result_columns
                // before exception, we need to fix it.
                auto cur_rows = column->size();
                if (cur_rows > total_rows)
                    column->popBack(cur_rows - total_rows);

                // all data columns will get default value in case of error
                column->insertDefault();
            }

            return 1;
        }
        else
        {
            e.addMessage("while parsing Kafka message (topic: {}, partition: {}, offset: {})'",
                consumer->currentTopic(), consumer->currentPartition(), consumer->currentOffset());
            consumer->setExceptionInfo(e.message());
            throw std::move(e);
        }
    };

    StreamingFormatExecutor executor(non_virtual_header, input_format, std::move(on_error));

    while (true)
    {
        size_t new_rows = 0;
        exception_message.reset();
        if (auto buf = consumer->consume())
        {
            ProfileEvents::increment(ProfileEvents::KafkaMessagesRead);
            new_rows = executor.execute(*buf);
        }

        if (new_rows)
        {
            // In read_kafka_message(), KafkaConsumer::nextImpl()
            // will be called, that may make something unusable, i.e. clean
            // KafkaConsumer::messages, which is accessed from
            // KafkaConsumer::currentTopic() (and other helpers).
            if (consumer->isStalled())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Polled messages became unusable");

            ProfileEvents::increment(ProfileEvents::KafkaRowsRead, new_rows);

            consumer->storeLastReadMessageOffset();

            auto topic         = consumer->currentTopic();
            auto key           = consumer->currentKey();
            auto offset        = consumer->currentOffset();
            auto partition     = consumer->currentPartition();
            auto timestamp_raw = consumer->currentTimestamp();
            auto header_list   = consumer->currentHeaderList();

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
                if (put_error_to_stream)
                {
                    if (exception_message)
                    {
                        const auto & payload = consumer->currentPayload();
                        virtual_columns[8]->insertData(reinterpret_cast<const char *>(payload.get_data()), payload.get_size());
                        virtual_columns[9]->insertData(exception_message->data(), exception_message->size());
                    }
                    else
                    {
                        virtual_columns[8]->insertDefault();
                        virtual_columns[9]->insertDefault();
                    }
                }
            }

            total_rows = total_rows + new_rows;
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
            // TODO: it seems like in case of put_error_to_stream=true we may need to process those differently
            // currently we just skip them with note in logs.
            consumer->storeLastReadMessageOffset();
            LOG_DEBUG(log, "Parsing of message (topic: {}, partition: {}, offset: {}) return no rows.", consumer->currentTopic(), consumer->currentPartition(), consumer->currentOffset());
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
    // i.e. will not be stored anywhere
    // If needed any extra columns can be added using DEFAULT they can be added at MV level if needed.

    auto result_block  = non_virtual_header.cloneWithColumns(executor.getResultColumns());
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

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
