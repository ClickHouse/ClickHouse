#pragma once

#include <memory>
#include <Processors/ISource.h>
#include <IO/ReadBuffer.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Storages/StorageSnapshot.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Stopwatch.h>

#include "config.h"

#if USE_AWS_SQS

#include <Storages/SQS/SQSConsumer.h>

namespace DB
{

class StorageSQS;

class SQSSource final : public ISource
{
public:
    SQSSource(
        StorageSQS & storage,
        const StorageSnapshotPtr & storage_snapshot,
        const String & format,
        const Block & sample_block,
        UInt64 max_block_size,
        UInt64 max_execution_time_ms,
        ContextPtr context,
        bool skip_broken_messages,
        UInt64 skip_broken_messages_count,
        const String & dead_letter_queue_url,
        bool auto_delete);

    String getName() const override { return "SQSSource"; }

    /// Delete processed messages from SQS. Called after successful pipeline execution.
    void deleteProcessedMessages();

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
    UInt64 max_block_size;
    bool skip_broken_messages;
    UInt64 skip_broken_messages_count;
    String dead_letter_queue_url;
    bool auto_delete;
    UInt64 max_execution_time_ms;

    bool is_finished = false;
    Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};

    /// Messages pending acknowledgment after successful processing.
    ConcurrentBoundedQueue<SQSConsumer::Message> pending_ack;
};

}

#endif // USE_AWS_SQS
