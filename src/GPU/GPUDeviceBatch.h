#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUDecompression.h>
#include <GPU/GPUFuture.h>
#include <GPU/GPUHostBatch.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.cuh>

#include <memory>
#include <optional>
#include <vector>

namespace DB::GPU
{

/// A column on the device: of the rows of a batch, or of those carried to the next one. It holds the memory of its
/// values, or of the bytes and the offsets of the rows of a `String`, and the view of its rows. The uploader makes it:
/// the rows carried, then those of the staged blocks, which expand in its memory, of which it keeps those of the batch
/// and carries the rest to the next.
class GPUDeviceColumn
{
public:
    /// Of the rows of a batch.
    const DeviceColumnView & view() const;

private:
    friend class GPUBatchUploader;

    /// Of no rows: what is carried to the first batch.
    GPUDeviceColumn(GPUElementType type_, cuda::stream_ref stream);

    /// The rows of `carried`, then those of the blocks of `staged`, which it adds to `decompression` to expand in its
    /// memory.
    GPUDeviceColumn(const GPUDeviceColumn & carried, const GPUHostColumn & staged, DecompressionBatch & decompression, cuda::stream_ref stream);

    /// Once its blocks are expanded: the carried bytes before them, and of a `String` the offsets of its sizes.
    void finish(const GPUDeviceColumn & carried, cuda::stream_ref stream);

    /// The rows whose values are all there: of a `String`, those whose bytes are.
    size_t completeRows(cuda::stream_ref stream);

    /// Keeps its first `rows` and returns the rest, the column carried to the next batch. Of the end of a part nothing
    /// may be left; `index` names the column in what is thrown.
    GPUDeviceColumn splitAt(size_t rows, bool part_ends, size_t index, cuda::stream_ref stream);

    void checkPartEnds(size_t rows, size_t taken_bytes, size_t index) const;

    bool isVariable() const;

    GPUElementType type;
    DeviceBuffer values_memory;
    /// Of a `String`: the offsets of its rows, and the bytes of a size that the last block of sizes cut, fewer than 8.
    DeviceBuffer offsets_memory;
    DeviceBuffer tail_memory;

    /// The values, or the bytes of a `String`, from its first row, in `values_memory`.
    char * values = nullptr;
    size_t values_bytes = 0;

    /// Of a `String`: the offsets of the rows with a size, one more than them, of which the first `carried_rows` were
    /// carried; the bytes of a size cut short after them; and the rows whose bytes are all there, and their bytes.
    uint64_t * offsets = nullptr;
    size_t carried_rows = 0;
    size_t sized_rows = 0;
    const char * tail = nullptr;
    size_t tail_bytes = 0;
    size_t covered_rows = 0;
    size_t covered_bytes = 0;

    /// Once `splitAt` has kept the rows of the batch.
    std::optional<DeviceColumnView> column_view;
};

/// The columns of a batch on the device, as the reader reads them, as `GPUHostBatch` is on the host: the future of
/// `GPUBatchUploader::sendHostToDevice`, see `takeOn`. Freed in the order of the uploader's stream, after the stream it was
/// taken on is done with it.
class GPUDeviceBatch
{
public:
    GPUDeviceBatch(GPUDeviceBatch &&) noexcept = default;
    GPUDeviceBatch & operator=(GPUDeviceBatch &&) noexcept = delete;
    GPUDeviceBatch(const GPUDeviceBatch &) = delete;
    GPUDeviceBatch & operator=(const GPUDeviceBatch &) = delete;
    ~GPUDeviceBatch();

    size_t rows() const { return num_rows; }
    const std::vector<GPUDeviceColumn> & columns() const { return device_columns; }

private:
    friend class GPUBatchUploader;
    friend GPUDeviceBatch takeOn(GPUFuture<GPUDeviceBatch> future, cuda::stream_ref stream);

    /// The stream that reads it, whose promise the uploader's stream waits for before it frees the memory.
    void useOn(cuda::stream_ref stream);

    explicit GPUDeviceBatch(std::shared_ptr<StreamPtr> upload_stream_);

    /// Shared, so the memory is freed in the uploader's stream order even after the uploader is gone.
    std::shared_ptr<StreamPtr> upload_stream;
    std::vector<GPUDeviceColumn> device_columns;
    size_t num_rows = 0;
    /// That the stream it was taken on is done with it.
    std::optional<GPUPromise<void>> released;
};

/// Sends the staged batches of one reader to the device and expands them, on a stream of its own. A batch has the rows
/// that every column has; the rest of each column waits on the device for the next one.
class GPUBatchUploader
{
public:
    explicit GPUBatchUploader(std::vector<GPUElementType> column_types_);

    GPUBatchUploader(const GPUBatchUploader &) = delete;
    GPUBatchUploader & operator=(const GPUBatchUploader &) = delete;

    /// The batch on the device, ready once the work queued on the uploader's stream is done; nothing where no row is
    /// complete in every column. The host batch is free once it returns, on an exception too, after which the uploader
    /// takes no more batches.
    std::optional<GPUFuture<GPUDeviceBatch>> sendHostToDevice(const GPUHostBatch & staged);

private:
    std::optional<GPUFuture<GPUDeviceBatch>> expand(const GPUHostBatch & staged);

    /// Declared first, so that the buffers below are freed in its order before it goes.
    const std::shared_ptr<StreamPtr> stream;
    const std::vector<GPUElementType> column_types;
    /// Of each column, the rows the batches so far did not take.
    std::vector<GPUDeviceColumn> carried;
    DecompressionBatch decompression;
    size_t part_rows_sent = 0;
    bool failed = false;
};

/// The batch, for the work queued on `stream` from now on, which waits on the device until the batch is ready.
GPUDeviceBatch takeOn(GPUFuture<GPUDeviceBatch> future, cuda::stream_ref stream);

}

#endif
