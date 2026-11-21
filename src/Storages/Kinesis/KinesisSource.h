#pragma once

#include <memory>

#include <Common/Stopwatch.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>
#include <Storages/StorageSnapshot.h>

#include "config.h"

#if USE_AWS_KINESIS

#include <Storages/Kinesis/KinesisConsumer.h>

namespace DB
{

class StorageKinesis;

class KinesisSource final : public ISource
{
public:
    KinesisSource(
        StorageKinesis & storage,
        const StorageSnapshotPtr & storage_snapshot,
        const String & format,
        const Block & sample_block,
        UInt64 max_block_size,
        UInt64 max_execution_time_ms,
        ContextPtr context,
        bool skip_broken_messages,
        UInt64 skip_broken_messages_count);

    String getName() const override { return "KinesisSource"; }

    KinesisConsumerPtr getConsumer() const { return consumer; }

    ~KinesisSource() override;

protected:
    Chunk generate() override;

private:
    StorageKinesis & storage;
    KinesisConsumerPtr consumer;
    Block sample_block;
    ContextPtr context;
    String format;
    UInt64 max_block_size;
    UInt64 max_execution_time_ms;
    bool skip_broken_messages;
    UInt64 skip_broken_messages_count;

    bool is_finished = false;
    Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};
};

}

#endif // USE_AWS_KINESIS
