#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/CompressedHostColumn.h>
#include <GPU/GPUTypes.cuh>

#include <base/types.h>

#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

namespace DB::GPU
{

/// The streams of a column of a GPU read, as MergeTree stores it: its values, and of a `String` its bytes and the sizes
/// of its rows, as `string_serialization_version = 'with_size_stream'` writes them.
enum class GPUStream : uint8_t
{
    Values,
    Sizes,
};

/// A column of a batch of a reader on the host: a run of each of its streams, of exactly the rows of the batch, as the
/// marks of the streams delimit them. The bytes of a `String` are as long as its sizes say, and may go past its rows.
struct GPUHostColumn
{
    GPUElementType type = GPUElementType::UInt8;
    CompressedHostColumn values;
    /// Of a `String` only.
    CompressedHostColumn sizes;

    void clear();
};

/// A batch of a reader on the host: a run of each column the reader reads, of exactly `rows` rows.
struct GPUHostBatch
{
    std::vector<GPUHostColumn> columns;
    size_t rows = 0;

    void clear();
};

/// Batches to fill again, so that pinned memory is allocated once per stream.
class GPUHostBatchPool
{
public:
    std::unique_ptr<GPUHostBatch> take(size_t num_columns);
    void give(std::unique_ptr<GPUHostBatch> batch);

private:
    std::mutex mutex;
    std::vector<std::unique_ptr<GPUHostBatch>> free_batches;
};

/// Stages the blocks a reader reads of columns of `column_types` into batches, each of the rows of a range of marks that
/// the reader reads: of each stream, a run of blocks from the one the first row is in, which skips the bytes before it,
/// to the one the last row is in, which is cut after it.
class GPUBlockStager
{
public:
    GPUBlockStager(std::vector<GPUElementType> column_types_, std::shared_ptr<GPUHostBatchPool> pool_);

    /// The stream takes `kept_bytes` of what the block expands to, after `skipped_bytes`; only the first block of a run
    /// skips bytes, and only the last is cut.
    void addCompressedBlock(
        size_t column,
        GPUStream stream,
        UInt8 method,
        std::string_view payload,
        size_t decompressed_bytes,
        size_t expected_bytes,
        size_t skipped_bytes,
        size_t kept_bytes);

    /// The range ends: the blocks staged since the last batch make the batch of its `rows` rows.
    void finishBatch(size_t rows);

    /// The next finished batch, the oldest first, if there is one.
    std::unique_ptr<GPUHostBatch> takeFinished();

private:
    CompressedHostColumn & streamOrThrow(size_t column, GPUStream stream);
    void startBatch();

    const std::vector<GPUElementType> column_types;
    const std::shared_ptr<GPUHostBatchPool> pool;
    std::unique_ptr<GPUHostBatch> current;
    std::deque<std::unique_ptr<GPUHostBatch>> finished;
};

}

#endif
