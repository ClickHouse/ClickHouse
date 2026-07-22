#pragma once

#include <Processors/ISource.h>
#include <Storages/Pulsar/StoragePulsar.h>

namespace DB
{

class PulsarSource final : public ISource
{
public:
    PulsarSource(
        StoragePulsar & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Names & columns_,
        size_t max_block_size_,
        LoggerPtr log_,
        UInt64 max_execution_time_,
        bool commit_in_suffix_ = false);

    ~PulsarSource() override;

    String getName() const override { return "PulsarSource"; }

    Chunk generate() override;

    /// Acknowledge all messages polled by this source. Must be called only after
    /// the blocks built from them have been durably written.
    void commit();
    /// Request redelivery of all polled but unacknowledged messages.
    void rollback();

    bool isStalled() const { return !consumer || consumer->isStalled(); }

private:
    StoragePulsar & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    const size_t max_block_size;
    LoggerPtr log;
    UInt64 max_execution_time;
    StreamingHandleErrorMode handle_error_mode;
    bool commit_in_suffix;

    bool is_finished = false;
    PulsarConsumerPtr consumer{nullptr};

    const Block non_virtual_header;
    const Block virtual_header;

    Chunk generateImpl();
};

}
