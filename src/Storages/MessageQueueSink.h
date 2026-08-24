#pragma once

#include <IO/WriteBufferFromString.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/IMessageProducer.h>
#include <Interpreters/Context_fwd.h>

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
///
/// If `format_header` differs from `header`, only a subset of input columns is
/// written into the formatted payload. The remaining input columns are still
/// passed to the producer (e.g. so a storage can turn them into message
/// metadata — Kafka key, timestamp, headers). `format_column_indices`
/// describes which columns of `header` map onto `format_header`, in order.
class MessageQueueSink : public SinkToStorage
{
public:
    MessageQueueSink(
        SharedHeader header,
        const String & format_name_,
        size_t max_rows_per_message_,
        std::unique_ptr<IMessageProducer> producer_,
        const String & storage_name_,
        const ContextPtr & context_);

    MessageQueueSink(
        SharedHeader header,
        SharedHeader format_header_,
        std::vector<size_t> format_column_indices_,
        const String & format_name_,
        size_t max_rows_per_message_,
        std::unique_ptr<IMessageProducer> producer_,
        const String & storage_name_,
        const ContextPtr & context_);

    ~MessageQueueSink() override;

    String getName() const override { return storage_name + "Sink"; }

    void consume(Chunk & chunk) override;

    void onStart() override;
    void onFinish() override;
    void onException(std::exception_ptr /* exception */) override;

protected:
    /// Do some specific initialization before consuming data.
    virtual void initialize() {}

private:
    const String format_name;
    size_t max_rows_per_message;

    SharedHeader format_header;
    std::vector<size_t> format_column_indices;

    std::unique_ptr<WriteBufferFromOwnString> buffer;
    IOutputFormatPtr format;
    IRowOutputFormat * row_format;
    std::unique_ptr<IMessageProducer> producer;

    const String storage_name;
    const ContextPtr context;

    /// Given the full set of input columns, returns the columns used by the formatter.
    Columns extractFormatColumns(const Columns & columns) const;
};

}
