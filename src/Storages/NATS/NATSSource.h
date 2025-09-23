#pragma once

#include <Core/StreamingHandleErrorMode.h>
#include <Processors/ISource.h>
#include <Storages/NATS/NATSConsumer.h>
#include <Storages/NATS/StorageNATS.h>


namespace DB
{

class NATSSource : public ISource
{
public:
    NATSSource(
        StorageNATS & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        ContextPtr context_,
        const Names & columns,
        size_t max_block_size_,
        StreamingHandleErrorMode handle_error_mode_);

    ~NATSSource() override;

    String getName() const override { return storage.getName(); }
    NATSConsumerPtr getConsumer() { return consumer; }

    Chunk generate() override;

    bool queueEmpty() const { return !consumer || consumer->queueEmpty(); }

    void setTimeLimit(Poco::Timespan max_execution_time_) { max_execution_time = max_execution_time_; }

private:
    bool checkTimeLimit() const;

    StorageNATS & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    const size_t max_block_size;
    StreamingHandleErrorMode handle_error_mode;

    bool is_finished = false;
    const Block non_virtual_header;
    const Block virtual_header;

    NATSConsumerPtr consumer;

    Poco::Timespan max_execution_time = 0;
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

    NATSSource(
        StorageNATS & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        std::pair<Block, Block> headers,
        ContextPtr context_,
        const Names & columns,
        size_t max_block_size_,
        StreamingHandleErrorMode handle_error_mode_);
};

}
