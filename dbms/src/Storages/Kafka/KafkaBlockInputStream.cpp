#include <Storages/Kafka/KafkaBlockInputStream.h>

#include <Formats/FormatFactory.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>

namespace DB
{

KafkaBlockInputStream::KafkaBlockInputStream(
    StorageKafka & storage_, const Context & context_, const String & schema, UInt64 max_block_size_)
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
    {
        LOG_TRACE(storage.log, "Re-joining claimed consumer after failure");
        consumer->unsubscribe();
    }

    storage.pushConsumer(consumer);
}

void KafkaBlockInputStream::readPrefixImpl()
{
    consumer = storage.tryClaimConsumer(context.getSettingsRef().queue_max_wait_ms.totalMilliseconds());
    claimed = !!consumer;

    if (!consumer)
        consumer = std::make_shared<cppkafka::Consumer>(storage.createConsumerConfiguration());

    // While we wait for an assignment after subscribtion, we'll poll zero messages anyway.
    // If we're doing a manual select then it's better to get something after a wait, then immediate nothing.
    if (consumer->get_subscription().empty())
    {
        using namespace std::chrono_literals;

        consumer->pause(); // don't accidentally read any messages
        consumer->subscribe(storage.topics);
        consumer->poll(5s);
        consumer->resume();
    }

    buffer = std::make_unique<DelimitedReadBuffer>(
        new ReadBufferFromKafkaConsumer(consumer, storage.log, max_block_size), storage.row_delimiter);
    addChild(FormatFactory::instance().getInput(storage.format_name, *buffer, storage.getSampleBlock(), context, max_block_size));

    broken = true;
}

void KafkaBlockInputStream::readSuffixImpl()
{
    buffer->subBufferAs<ReadBufferFromKafkaConsumer>()->commit();

    broken = false;
}

} // namespace DB
