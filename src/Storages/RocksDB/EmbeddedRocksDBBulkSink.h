#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/ThreadPool.h>
#include <Columns/ColumnString.h>
#include <Processors/Chunk.h>


namespace DB
{
namespace fs = std::filesystem;

class StorageEmbeddedRocksDB;
class EmbeddedRocksDBBulkSink;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Optimized for bulk importing into StorageEmbeddedRocksDB:
/// 1. No mem-table: an SST file is built from chunk, then import to rocksdb
/// 2. Squash chunks to reduce the number of SST files
class EmbeddedRocksDBBulkSink : public SinkToStorage, public WithContext
{
public:
    EmbeddedRocksDBBulkSink(
        ContextPtr context_,
        StorageEmbeddedRocksDB & storage_,
        const StorageMetadataPtr & metadata_snapshot_);

    ~EmbeddedRocksDBBulkSink() override;

    void consume(Chunk & chunk) override;

    void onFinish() override;

    String getName() const override { return "EmbeddedRocksDBBulkSink"; }

private:
    /// Get a unique path to write temporary SST file
    String getTemporarySSTFilePath();

    /// Squash chunks to a minimum size
    std::vector<Chunk> squash(Chunk chunk);
    bool isEnoughSize(const std::vector<Chunk> & input_chunks) const;
    bool isEnoughSize(const Chunk & chunk) const;
    /// Serialize chunks to rocksdb key-value pairs
    template<bool with_timestamp>
    std::pair<ColumnString::Ptr, ColumnString::Ptr> serializeChunks(std::vector<Chunk> && input_chunks) const;

    StorageEmbeddedRocksDB & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t primary_key_pos = 0;
    Serializations serializations;

    /// For squashing chunks
    std::vector<Chunk> chunks;
    size_t min_block_size_rows = 0;

    /// For writing SST files
    size_t file_counter = 0;
    static constexpr auto TMP_INSERT_PREFIX = "tmp_insert_";
    String insert_directory_queue;
};

}
