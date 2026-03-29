#pragma once

#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/kinesis/KinesisClient.h>

namespace DB
{

class KinesisSink final : public SinkToStorage
{
public:
    KinesisSink(
        StorageMetadataPtr metadata_snapshot,
        std::shared_ptr<Aws::Kinesis::KinesisClient> client,
        const String & stream_name,
        const String & format_name,
        size_t max_rows_per_message,
        ContextPtr context);

    std::string getName() const override { return "KinesisSink"; }
    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    void sendRecord(const String & data, const String & partition_key);

    static String generatePartitionKey(const String & data);

    std::shared_ptr<Aws::Kinesis::KinesisClient> client;
    const String stream_name;
    const String format_name;
    const size_t max_rows_per_message;
    size_t total_rows = 0;
    ContextPtr context;
    LoggerPtr log;
};

}

#endif // USE_AWS_KINESIS