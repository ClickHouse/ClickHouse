#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Core/Block.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Storages/StorageInMemoryMetadata.h>

#include "config.h"

#if USE_AWS_SQS

#include <aws/sqs/SQSClient.h>

namespace DB
{

class SQSSink final : public SinkToStorage
{
public:
    SQSSink(
        StorageMetadataPtr metadata_snapshot,
        const Aws::SQS::SQSClient & client,
        const String & queue_url,
        const String & format_name,
        size_t max_rows_per_message,
        ContextPtr context);

    std::string getName() const override { return "SQSSink"; }
    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    void sendMessage(const String & message);

    const Aws::SQS::SQSClient & client;
    const String queue_url;
    const String format_name;
    const size_t max_rows_per_message;
    size_t total_rows = 0;
    ContextPtr context;
    LoggerPtr log;
};

}

#endif // USE_AWS_SQS
