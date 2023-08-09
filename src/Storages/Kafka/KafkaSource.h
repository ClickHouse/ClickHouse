#pragma once

#include <Processors/ISource.h>

#include <Storages/Kafka/StorageKafka.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>
#include <Common/Stopwatch.h>


namespace Poco
{
    class Logger;
}
namespace DB
{

class KafkaSource : public ISource
{
public:
    KafkaSource(
        StorageKafka & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        Poco::Logger * log_,
        size_t max_block_size_,
        bool commit_in_suffix = false);
    ~KafkaSource() override;

    String getName() const override { return storage.getName(); }

    Chunk generate() override;

    void commit();
    bool isStalled() const { return !buffer || buffer->isStalled(); }

    void setTimeLimit(Poco::Timespan max_execution_time_) { max_execution_time = max_execution_time_; }

private:
    StorageKafka & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    Poco::Logger * log;
    UInt64 max_block_size;

    ConsumerBufferPtr buffer;
    bool broken = true;
    bool is_finished = false;
    bool commit_in_suffix;

    const Block non_virtual_header;
    const Block virtual_header;
    const HandleKafkaErrorMode handle_error_mode;

    Poco::Timespan max_execution_time = 0;
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};

    bool checkTimeLimit() const;

    Chunk generateImpl();
};

}
