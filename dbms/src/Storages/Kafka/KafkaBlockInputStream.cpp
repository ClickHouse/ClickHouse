#include <Storages/Kafka/KafkaBlockInputStream.h>

#include <Formats/FormatFactory.h>
#include <Storages/Kafka/StorageKafka.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
} // namespace ErrorCodes

KafkaBlockInputStream::KafkaBlockInputStream(
    StorageKafka & storage_, const Context & context_, const String & schema, size_t max_block_size_)
    : storage(storage_), context(context_), max_block_size(max_block_size_)
{
    // Always skip unknown fields regardless of the context (JSON or TSKV)
    context.setSetting("input_format_skip_unknown_fields", 1u);

    // We don't use ratio since the number of Kafka messages may vary from stream to stream.
    // Thus, ratio is meaningless.
    context.setSetting("input_format_allow_errors_ratio", 0.);
    context.setSetting("input_format_allow_errors_num", storage.skip_broken);

    if (schema.size() > 0)
        context.setSetting("format_schema", schema);
}

KafkaBlockInputStream::~KafkaBlockInputStream()
{
    if (!hasClaimed())
        return;

    // An error was thrown during the stream or it did not finish successfully
    // The read offsets weren't comitted, so consumer must rejoin the group from the original starting point
    if (!finalized)
    {
        LOG_TRACE(storage.log, "KafkaBlockInputStream did not finish successfully, unsubscribing from assignments and rejoining");
        consumer->unsubscribe();
        consumer->subscribe(storage.topics);
    }

    // Return consumer for another reader
    storage.pushConsumer(consumer);
    consumer = nullptr;
}

String KafkaBlockInputStream::getName() const
{
    return storage.getName();
}

Block KafkaBlockInputStream::readImpl()
{
    if (isCancelledOrThrowIfKilled() || !hasClaimed())
        return {};

    if (!reader)
        throw Exception("Logical error: reader is not initialized", ErrorCodes::LOGICAL_ERROR);

    return reader->read();
}

Block KafkaBlockInputStream::getHeader() const
{
    return storage.getSampleBlock();
}

void KafkaBlockInputStream::readPrefixImpl()
{
    if (!hasClaimed())
    {
        // Create a formatted reader on Kafka messages
        LOG_TRACE(storage.log, "Creating formatted reader");
        consumer = storage.tryClaimConsumer(context.getSettingsRef().queue_max_wait_ms.totalMilliseconds());
        if (consumer == nullptr)
            throw Exception("Failed to claim consumer: ", ErrorCodes::TIMEOUT_EXCEEDED);

        read_buf = std::make_unique<DelimitedReadBuffer>(new ReadBufferFromKafkaConsumer(consumer, storage.log), storage.row_delimiter);
        reader = FormatFactory::instance().getInput(storage.format_name, *read_buf, storage.getSampleBlock(), context, max_block_size);
    }

    // Start reading data
    finalized = false;
    reader->readPrefix();
}

void KafkaBlockInputStream::readSuffixImpl()
{
    if (hasClaimed())
    {
        reader->readSuffix();
        read_buf->subBufferAs<ReadBufferFromKafkaConsumer>()->commit(); // Store offsets read in this stream
    }

    // Mark as successfully finished
    finalized = true;
}

} // namespace DB
