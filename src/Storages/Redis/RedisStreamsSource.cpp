#include <Storages/Redis/RedisStreamsSource.h>

#include <Formats/FormatFactory.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <base/logger_useful.h>
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

RedisStreamsSource::RedisStreamsSource(
    StorageRedis & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    Poco::Logger * log_,
    size_t max_block_size_)
    : SourceWithProgress(metadata_snapshot_->getSampleBlockForColumns(columns, storage_.getVirtuals(), storage_.getStorageID()))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(columns)
    , log(log_)
    , max_block_size(max_block_size_)
    , non_virtual_header(metadata_snapshot->getSampleBlockNonMaterialized())
    , virtual_header(metadata_snapshot->getSampleBlockForColumns(storage.getVirtualColumnNames(), storage.getVirtuals(), storage.getStorageID()))
{
}

RedisStreamsSource::~RedisStreamsSource()
{
    if (!buffer)
        return;

    storage.pushReadBuffer(buffer);
}

Chunk RedisStreamsSource::generateImpl()
{
    if (!buffer)
    {
        auto timeout = std::chrono::milliseconds(context->getSettingsRef().kafka_max_wait_ms.totalMilliseconds());
        buffer = storage.popReadBuffer(timeout);

        if (!buffer)
            return {};
    }

    if (is_finished)
        return {};

    is_finished = true;
    // now it's one-time usage InputStream
    // one block of the needed size (or with desired flush timeout) is formed in one internal iteration
    // otherwise external iteration will reuse that and logic will became even more fuzzy
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto input_format = FormatFactory::instance().getInputFormat(
        storage.getFormatName(), *buffer, non_virtual_header, context, max_block_size);

    std::optional<std::string> exception_message;
    size_t total_rows = 0;
    size_t failed_poll_attempts = 0;

    StreamingFormatExecutor executor(non_virtual_header, input_format);

    while (true)
    {
        size_t new_rows = 0;
        exception_message.reset();
        if (buffer->poll())
            new_rows = executor.execute();

        if (new_rows)
        {
            read_nothing = false;

            auto topic = buffer->currentTopic();
            auto key = buffer->currentKey();
            auto timestamp = buffer->currentTimestamp();
            auto sequence_number = buffer->currentSequenceNumber();

            Array headers_names;
            Array headers_values;

            for (size_t i = 0; i < new_rows; ++i)
            {
                virtual_columns[0]->insert(topic);
                virtual_columns[1]->insert(key);
                virtual_columns[2]->insert(timestamp);
                virtual_columns[3]->insert(sequence_number);
            }

            total_rows = total_rows + new_rows;
        }
        else
        {
            ++failed_poll_attempts;
        }

        if (!buffer->hasMorePolledMessages()
            && (total_rows >= max_block_size || !checkTimeLimit() || failed_poll_attempts >= MAX_FAILED_POLL_ATTEMPTS))
        {
            break;
        }
    }

    if (total_rows == 0)
        return {};

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

Chunk RedisStreamsSource::generate()
{
    auto chunk = generateImpl();

    return chunk;
}

void RedisStreamsSource::commit()
{
    if (!buffer)
        return;

    buffer->ack();
}

}
