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

/// The columns of a batch on the device, as the reader reads them, which `GPUBatchUploader::sendHostToDevice` makes ready
/// on its stream, see `takeOn`. Freed in the order of the uploader's stream, after the stream they were taken on is done
/// with them.
class GPUDeviceBatch
{
public:
    GPUDeviceBatch(GPUDeviceBatch &&) noexcept = default;
    GPUDeviceBatch & operator=(GPUDeviceBatch &&) noexcept = delete;
    GPUDeviceBatch(const GPUDeviceBatch &) = delete;
    GPUDeviceBatch & operator=(const GPUDeviceBatch &) = delete;
    ~GPUDeviceBatch();

    size_t rows() const { return num_rows; }
    const std::vector<DeviceColumnView> & columns() const { return column_views; }

private:
    friend class GPUBatchUploader;
    friend GPUDeviceBatch takeOn(GPUFuture<GPUDeviceBatch> future, cuda::stream_ref stream);

    /// The stream that reads it, which the uploader's waits for before it frees the memory.
    void useOn(cuda::stream_ref stream);

    explicit GPUDeviceBatch(std::shared_ptr<StreamPtr> upload_stream_);

    /// Shared, so the memory is freed in the uploader's stream order even after the uploader is gone.
    std::shared_ptr<StreamPtr> upload_stream;
    std::vector<DeviceBuffer> buffers;
    std::vector<DeviceColumnView> column_views;
    size_t num_rows = 0;
    EventPtr consumed;
    cudaStream_t consumer = nullptr;
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
    /// What the batches so far did not take of a column: its bytes, and of a `String` the offsets of its rows left,
    /// from 0, and the bytes of a size that a block cut, fewer than 8.
    struct Carried
    {
        DeviceBuffer values;
        DeviceBuffer offsets;
        DeviceBuffer size_tail;
    };

    class BatchUpload;
    class ColumnOnDevice;

    std::optional<GPUFuture<GPUDeviceBatch>> expand(const GPUHostBatch & staged);

    /// Declared first, so that the buffers below are freed in its order before it goes.
    const std::shared_ptr<StreamPtr> stream;
    const std::vector<GPUElementType> column_types;
    std::vector<Carried> carried;
    DecompressionBatch decompression;
    EventPtr staged_sent = createEvent();
    size_t part_rows_sent = 0;
    bool failed = false;
};

/// The batch, for the work queued on `stream` from now on, which waits on the device until the batch is ready.
GPUDeviceBatch takeOn(GPUFuture<GPUDeviceBatch> future, cuda::stream_ref stream);

}

#endif
