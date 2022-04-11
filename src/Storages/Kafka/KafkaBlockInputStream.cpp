#include <Storages/Kafka/KafkaBlockInputStream.h>

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <common/logger_useful.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

// with default poll timeout (500ms) it will give about 5 sec delay for doing 10 retries
// when selecting from empty topic
const auto MAX_FAILED_POLL_ATTEMPTS = 10;

KafkaBlockInputStream::KafkaBlockInputStream(
    StorageKafka & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::shared_ptr<Context> & context_,
    const Names & columns,
    Poco::Logger * log_,
    size_t max_block_size_,
    bool commit_in_suffix_)
    : storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(columns)
    , log(log_)
    , max_block_size(max_block_size_)
    , commit_in_suffix(commit_in_suffix_)
    , non_virtual_header(metadata_snapshot->getSampleBlockNonMaterialized())
    , virtual_header(metadata_snapshot->getSampleBlockForColumns(storage.getVirtualColumnNames(), storage.getVirtuals(), storage.getStorageID()))
    , handle_error_mode(storage.getHandleKafkaErrorMode())
{
}

KafkaBlockInputStream::~KafkaBlockInputStream()
{
    if (!buffer)
        return;

    if (broken)
        buffer->unsubscribe();

    storage.pushReadBuffer(buffer);
}

Block KafkaBlockInputStream::getHeader() const
{
    return metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
}

void KafkaBlockInputStream::readPrefixImpl()
{
    auto timeout = std::chrono::milliseconds(context->getSettingsRef().kafka_max_wait_ms.totalMilliseconds());
    buffer = storage.popReadBuffer(timeout);

    if (!buffer)
        return;

    buffer->subscribe();

    broken = true;
}

Block KafkaBlockInputStream::readImpl()
{
    if (!buffer || finished)
        return Block();

    finished = true;
    // now it's one-time usage InputStream
    // one block of the needed size (or with desired flush timeout) is formed in one internal iteration
    // otherwise external iteration will reuse that and logic will became even more fuzzy
    MutableColumns result_columns  = non_virtual_header.cloneEmptyColumns();
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto put_error_to_stream = handle_error_mode == HandleKafkaErrorMode::STREAM;

    auto input_format = FormatFactory::instance().getInputFormat(
        storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size);

    InputPort port(input_format->getPort().getHeader(), input_format.get());
    connect(input_format->getPort(), port);
    port.setNeeded();

    std::optional<std::string> exception_message;
    auto read_kafka_message = [&]
    {
        size_t new_rows = 0;
        while (true)
        {
            auto status = input_format->prepare();

            switch (status)
            {
                case IProcessor::Status::Ready:
                    input_format->work();
                    break;

                case IProcessor::Status::Finished:
                    input_format->resetParser();
                    return new_rows;

                case IProcessor::Status::PortFull:
                {
                    auto chunk = port.pull();

                    // that was returning bad value before https://github.com/ClickHouse/ClickHouse/pull/8005
                    // if will be backported should go together with #8005
                    auto chunk_rows = chunk.getNumRows();
                    new_rows += chunk_rows;

                    auto columns = chunk.detachColumns();
                    for (size_t i = 0, s = columns.size(); i < s; ++i)
                    {
                        result_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
                    }
                    break;
                }
                case IProcessor::Status::NeedData:
                case IProcessor::Status::Async:
                case IProcessor::Status::ExpandPipeline:
                    throw Exception("Source processor returned status " + IProcessor::statusToName(status), ErrorCodes::LOGICAL_ERROR);
            }
        }
    };

    size_t total_rows = 0;
    size_t failed_poll_attempts = 0;

    while (true)
    {
        size_t new_rows = 0;
        exception_message.reset();
        if (buffer->poll())
        {
            try
            {
                new_rows = read_kafka_message();
            }
            catch (Exception & e)
            {
                if (put_error_to_stream)
                {
                    input_format->resetParser();
                    exception_message = e.message();
                    for (auto & column : result_columns)
                    {
                        // read_kafka_message could already push some rows to result_columns
                        // before exception, we need to fix it.
                        auto cur_rows = column->size();
                        if (cur_rows > total_rows)
                        {
                            column->popBack(cur_rows - total_rows);
                        }
                        // all data columns will get default value in case of error
                        column->insertDefault();
                    }
                    new_rows = 1;
                }
                else
                {
                    e.addMessage("while parsing Kafka message (topic: {}, partition: {}, offset: {})'", buffer->currentTopic(), buffer->currentPartition(), buffer->currentOffset());
                    throw;
                }
            }
        }

        if (new_rows)
        {
            // In read_kafka_message(), ReadBufferFromKafkaConsumer::nextImpl()
            // will be called, that may make something unusable, i.e. clean
            // ReadBufferFromKafkaConsumer::messages, which is accessed from
            // ReadBufferFromKafkaConsumer::currentTopic() (and other helpers).
            if (buffer->isStalled())
                throw Exception("Polled messages became unusable", ErrorCodes::LOGICAL_ERROR);

            buffer->storeLastReadMessageOffset();

            auto topic         = buffer->currentTopic();
            auto key           = buffer->currentKey();
            auto offset        = buffer->currentOffset();
            auto partition     = buffer->currentPartition();
            auto timestamp_raw = buffer->currentTimestamp();
            auto header_list   = buffer->currentHeaderList();

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
                        auto payload = buffer->currentPayload();
                        virtual_columns[8]->insert(payload);
                        virtual_columns[9]->insert(*exception_message);
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
        else if (buffer->polledDataUnusable())
        {
            break;
        }
        else if (buffer->isStalled())
        {
            ++failed_poll_attempts;
        }
        else
        {
            // We came here in case of tombstone (or sometimes zero-length) messages, and it is not something abnormal
            // TODO: it seems like in case of put_error_to_stream=true we may need to process those differently
            // currently we just skip them with note in logs.
            buffer->storeLastReadMessageOffset();
            LOG_DEBUG(log, "Parsing of message (topic: {}, partition: {}, offset: {}) return no rows.", buffer->currentTopic(), buffer->currentPartition(), buffer->currentOffset());
        }

        if (!buffer->hasMorePolledMessages()
            && (total_rows >= max_block_size || !checkTimeLimit() || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS))
        {
            break;
        }
    }

    if (buffer->polledDataUnusable() || total_rows == 0)
        return Block();

    /// MATERIALIZED columns can be added here, but I think
    // they are not needed here:
    // and it's misleading to use them here,
    // as columns 'materialized' that way stays 'ephemeral'
    // i.e. will not be stored anythere
    // If needed any extra columns can be added using DEFAULT they can be added at MV level if needed.

    auto result_block  = non_virtual_header.cloneWithColumns(std::move(result_columns));
    auto virtual_block = virtual_header.cloneWithColumns(std::move(virtual_columns));

    for (const auto & column : virtual_block.getColumnsWithTypeAndName())
        result_block.insert(column);

    return ConvertingBlockInputStream(
               std::make_shared<OneBlockInputStream>(result_block),
               getHeader(),
               ConvertingBlockInputStream::MatchColumnsMode::Name)
        .read();
}

void KafkaBlockInputStream::readSuffixImpl()
{
    if (commit_in_suffix)
        commit();
}

void KafkaBlockInputStream::commit()
{
    if (!buffer)
        return;

    buffer->commit();

    broken = false;
}

}
