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

    addChild(FormatFactory::instance().getInput(storage.format_name, *buffer, storage.getSampleBlock(), context, max_block_size));

    broken = true;
}

void KafkaBlockInputStream::readSuffixImpl()
{
    buffer->subBufferAs<ReadBufferFromKafkaConsumer>()->commit();

    broken = false;
}

}
