#include <Storages/Kafka/KafkaBlockInputStream.h>

#include <Formats/FormatFactory.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>

namespace DB
{

KafkaBlockInputStream::KafkaBlockInputStream(
    StorageKafka & storage_, const Context & context_, const String & schema, size_t max_block_size_)
    : storage(storage_), context(context_), max_block_size(max_block_size_)
{
    context.setSetting("input_format_skip_unknown_fields", 1u); // Always skip unknown fields regardless of the context (JSON or TSKV)
    context.setSetting("input_format_allow_errors_ratio", 0.);
    context.setSetting("input_format_allow_errors_num", storage.skip_broken);

    if (!schema.empty())
        context.setSetting("format_schema", schema);
}

KafkaBlockInputStream::~KafkaBlockInputStream()
{
    if (!claimed)
        return;

    if (broken)
        buffer->subBufferAs<ReadBufferFromKafkaConsumer>()->unsubscribe();

    storage.pushBuffer(buffer);
}

void KafkaBlockInputStream::readPrefixImpl()
{
    buffer = storage.tryClaimBuffer(context.getSettingsRef().queue_max_wait_ms.totalMilliseconds());
    claimed = !!buffer;

    if (!buffer)
        buffer = storage.createBuffer();

    buffer->subBufferAs<ReadBufferFromKafkaConsumer>()->subscribe(storage.topics);

    // Set the internal block size to a such value that allows to break the read approximately every `stream_flush_interval_ms`
    // and call `checkTimeLimit()` for the top-level stream.
    // FIXME: looks like the better solution would be to propagate limits from top-level streams to inferior ones.
    const size_t poll_timeout = buffer->subBufferAs<ReadBufferFromKafkaConsumer>()->poll_timeout;
    size_t block_size = std::min(max_block_size, context.getSettingsRef().stream_flush_interval_ms.totalMilliseconds() / poll_timeout);
    block_size = std::max(block_size, 1ul);

    addChild(FormatFactory::instance().getInput(storage.format_name, *buffer, storage.getSampleBlock(), context, block_size));

    broken = true;
}

void KafkaBlockInputStream::readSuffixImpl()
{
    buffer->subBufferAs<ReadBufferFromKafkaConsumer>()->commit();

    broken = false;
}

}
