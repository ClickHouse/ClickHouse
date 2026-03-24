#pragma once

#include <Processors/ISource.h>
#include <Storages/Kafka/KeeperHandlingConsumer.h>
#include <Storages/StorageSnapshot.h>
#include <Common/Stopwatch.h>


namespace DB
{

class StorageKafka2;

class Kafka2Source : public ISource
{
public:
    Kafka2Source(
        StorageKafka2 & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        LoggerPtr log_,
        size_t max_block_size_,
        size_t consumer_index_,
        bool commit_in_suffix = false);
    ~Kafka2Source() override;

    String getName() const override { return "Kafka2Source"; }

    Chunk generate() override;

    void commit();
    bool isStalled() const { return stalled; }

private:
    StorageKafka2 & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    LoggerPtr log;
    UInt64 max_block_size;
    size_t consumer_index;
    bool commit_in_suffix;

    bool is_finished = false;
    bool broken = true;
    bool stalled = false;

    Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};

    std::shared_ptr<KeeperHandlingConsumer> consumer;
    std::optional<KeeperHandlingConsumer::OffsetGuard> offset_guard;

    Chunk generateImpl();
};

}
