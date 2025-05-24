#pragma once

#include <Interpreters/Context.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/kinesis/KinesisClient.h>

namespace DB
{

class KinesisSink : public SinkToStorage
{
public:
    KinesisSink(
        const StorageMetadataPtr & metadata_snapshot,
        const Aws::Kinesis::KinesisClient & client,
        const String & stream_name,
        const String & format_name,
        size_t max_rows_per_message,
        ContextPtr context);

    std::string getName() const override { return "KinesisSink"; }
    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    const Aws::Kinesis::KinesisClient & client;
    const String stream_name;
    String format_name;
    size_t max_rows_per_message;
    size_t total_rows = 0;
    ContextPtr context;
    Block sample_block;
    std::unique_ptr<WriteBuffer> write_buf;
    std::unique_ptr<IRowOutputFormat> row_output_format;

    void sendMessage(const String & message);

    String generatePartitionKey(const String & message);
};

}

#endif // USE_AWS_KINESIS
