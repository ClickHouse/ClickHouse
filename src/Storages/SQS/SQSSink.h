#pragma once

#include <memory>
#include <Processors/Sinks/SinkToStorage.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <IO/WriteBuffer.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Interpreters/Context.h>
#include <Storages/StorageInMemoryMetadata.h>

#include "config.h"

#if USE_AWS_SQS

#include <aws/sqs/SQSClient.h>

namespace DB
{

class SQSSink : public SinkToStorage
{
public:
    SQSSink(
        const StorageMetadataPtr & metadata_snapshot,
        const Aws::SQS::SQSClient & client,
        const String & queue_url,
        const String & format_name,
        size_t max_rows_per_message,
        ContextPtr context);

    std::string getName() const override { return "SQSSink"; }
    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    const Aws::SQS::SQSClient & client;
    const String queue_url;
    String format_name;
    size_t max_rows_per_message;
    size_t total_rows = 0;
    ContextPtr context;
    Block sample_block;
    std::unique_ptr<WriteBuffer> write_buf;
    std::unique_ptr<IRowOutputFormat> row_output_format;
    
    void sendMessage(const String & message);
};

}

#endif // USE_AWS_SQS
