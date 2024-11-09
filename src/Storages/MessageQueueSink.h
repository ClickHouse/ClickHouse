#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/IMessageProducer.h>
#include <Interpreters/Context.h>

namespace DB
{

class IOutputFormat;
class IRowOutputFormat;
using IOutputFormatPtr = std::shared_ptr<IOutputFormat>;


/// Storage sink for streaming engines like Kafka/RabbitMQ/NATS.
/// It implements formatting input data into messages.
/// For row-based formats like TSV, CSV, JSONEachRow, etc, each message
/// contains from 1 to max_rows_per_message rows.
/// For block-based formats like Native, Arrow, Parquet, the whole block is formatted into one message.
/// Each message is created independently, so it contains all format
/// prefixes/suffixes and can fully parsed back by corresponding input format.
/// After formatting, created message is propagated to IMessageProducer::produce() method.
/// To use MessageQueueSink for specific streaming engine, you should implement
/// IMessageProducer for it.
class MessageQueueSink : public SinkToStorage
{
public:
    MessageQueueSink(
        const Block & header,
        const String & format_name_,
        size_t max_rows_per_message_,
        std::unique_ptr<IMessageProducer> producer_,
        const String & storage_name_,
        const ContextPtr & context_);

    String getName() const override { return storage_name + "Sink"; }

    void consume(Chunk & chunk) override;

    void onStart() override;
    void onFinish() override;
    void onCancel() noexcept override;
    void onException(std::exception_ptr /* exception */) override { onFinish(); }

protected:
    /// Do some specific initialization before consuming data.
    virtual void initialize() {}

private:
    const String format_name;
    size_t max_rows_per_message;

    std::unique_ptr<WriteBufferFromOwnString> buffer;
    IOutputFormatPtr format;
    IRowOutputFormat * row_format;
    std::unique_ptr<IMessageProducer> producer;

    const String storage_name;
    const ContextPtr context;
};

}
