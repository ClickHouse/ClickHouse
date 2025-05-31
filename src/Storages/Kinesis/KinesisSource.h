#pragma once

#include <memory>
#include <Processors/ISource.h>
#include <Storages/Kinesis/StorageKinesis.h>
#include <Storages/StorageSnapshot.h>
#include <Interpreters/Context.h>
#include <IO/ReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>

#include "config.h"

#if USE_AWS_KINESIS

namespace DB
{

class KinesisConsumer;
using KinesisConsumerPtr = std::shared_ptr<KinesisConsumer>;

class KinesisSource : public ISource
{
public:
    KinesisSource(
        StorageKinesis & storage_,
        const StorageSnapshotPtr & storage_snapshot,
        const String & format,
        const Block & sample_block,
        UInt64 max_block_size,
        UInt64 max_execution_time,
        ContextPtr context,
        bool is_read);

    String getName() const override { return "KinesisSource"; }

    void commit();
    void rollback();

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
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

    std::unique_ptr<ReadBuffer> read_buf;
    std::shared_ptr<IRowInputFormat> row_input_format;

    bool is_finished = false;
    bool is_read = false;
};

}

#endif // USE_AWS_KINESIS
