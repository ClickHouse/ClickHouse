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

/// A column of a batch of a reader on the host: a run of each of its streams.
struct GPUHostColumn
{
    CompressedHostColumn values;
    /// Of a `String` only.
    CompressedHostColumn sizes;

    void clear();
};

/// A batch of a reader on the host: a run of each column the reader reads.
struct GPUHostBatch
{
    std::vector<GPUHostColumn> columns;
    /// Set on the last batch of a part: the rows of the part. The uploader then sends every row it holds.
    std::optional<size_t> part_rows;

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

/// Stages the blocks a reader reads of columns of `column_types` into batches. A batch is finished when every column has
/// about `batch_bytes` of rows, a stream fills its pinned memory, a block cannot follow the staged ones, or a part ends.
class GPUBlockStager
{
public:
    static constexpr size_t max_column_bytes = 32UL << 20;

    GPUBlockStager(std::vector<GPUElementType> column_types_, size_t batch_bytes, std::shared_ptr<GPUHostBatchPool> pool_);

    /// The stream takes `kept_bytes` of what the block expands to, after `skipped_bytes`.
    void addCompressedBlock(
        size_t column,
        GPUStream stream,
        UInt8 method,
        std::string_view payload,
        size_t decompressed_bytes,
        size_t expected_bytes,
        size_t skipped_bytes,
        size_t kept_bytes);

    /// The part ends, of `num_rows` rows: finishes the batch being staged, even an empty one.
    void finishPart(size_t num_rows);

    /// The next finished batch, the oldest first, if there is one.
    std::unique_ptr<GPUHostBatch> takeFinished();

private:
    CompressedHostColumn & streamOrThrow(size_t column, GPUStream stream);
    bool isFull() const;
    /// Queues the batch being staged as finished, and starts a new one.
    void finishBatch(std::optional<size_t> part_rows = {});

    const std::vector<GPUElementType> column_types;
    const size_t batch_rows;
    const std::shared_ptr<GPUHostBatchPool> pool;
    std::unique_ptr<GPUHostBatch> current;
    std::deque<std::unique_ptr<GPUHostBatch>> finished;
};

}

#endif
