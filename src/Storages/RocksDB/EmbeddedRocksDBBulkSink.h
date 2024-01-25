#pragma once

#include <condition_variable>
#include <stdatomic.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/ThreadPool.h>
#include <Columns/ColumnString.h>
#include <IO/WriteBufferFromVector.h>


namespace DB
{

class StorageEmbeddedRocksDB;
class EmbeddedRocksDBBulkSink;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Optimized for bulk importing into StorageEmbeddedRocksDB:
/// 1. No mem-table: an SST file is built from chunk, then import to rocksdb
/// 2. Overlap compute and IO: one thread prepare rocksdb data from chunk, and another thread to write the data to SST file
class EmbeddedRocksDBBulkSink : public SinkToStorage, public WithContext
{
public:
    EmbeddedRocksDBBulkSink(
        ContextPtr context_,
        StorageEmbeddedRocksDB & storage_,
        const StorageMetadataPtr & metadata_snapshot_);

    ~EmbeddedRocksDBBulkSink() override;

    void consume(Chunk chunk) override;

    String getName() const override { return "EmbeddedRocksDBBulkSink"; }

private:
    /// Get a unique path to write temporary SST file
    String getTemporarySSTFilePath();

    std::atomic_size_t file_counter = 0;
    StorageEmbeddedRocksDB & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t primary_key_pos = 0;
    Serializations serializations;
    String insert_directory_queue;
};

}
