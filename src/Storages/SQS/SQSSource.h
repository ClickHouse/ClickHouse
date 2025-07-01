#pragma once

#include <memory>
#include <Processors/ISource.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <IO/ReadBuffer.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/SQS/StorageSQS.h>

#include "config.h"

#if USE_AWS_SQS

namespace DB
{

class SQSConsumer;
using SQSConsumerPtr = std::shared_ptr<SQSConsumer>;


class SQSSource : public ISource
{
public:
    SQSSource(
        StorageSQS & storage_,
        const StorageSnapshotPtr & storage_snapshot,
        const String & format,
        const Block & sample_block,
        UInt64 max_block_size,
        UInt64 max_execution_time,
        ContextPtr context,
        bool skip_invalid_messages,
        String dead_letter_queue_url,
        bool is_read,
        bool auto_delete);

    String getName() const override { return "SQS"; }

    void deleteMessages();

    ~SQSSource() override;

protected:
    Chunk generate() override;

private:
    StorageSQS & storage;
    SQSConsumerPtr consumer;
    Block sample_block;
    ContextPtr context;
    String format;
    std::unique_ptr<ReadBuffer> read_buf;
    std::shared_ptr<IRowInputFormat> row_input_format;
    UInt64 max_block_size;
    bool skip_invalid_messages;
    String dead_letter_queue_url;

    bool is_read;
    bool auto_delete;

    bool is_finished = false;
    uint64_t max_execution_time_ms = 0;
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

    ConcurrentBoundedQueue<SQSConsumer::Message> queue;
};


}

#endif // USE_AWS_SQS
