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

class GPUDeviceBatch;

/// The rows of `host` on the device, expanded on `stream`, ready once the work queued there now is done. The host
/// memory of `host` is read until it returns; the memory of the batch is freed in the order of `stream`.
GPUFuture<GPUDeviceBatch> copyHostToDevice(const GPUHostBatch & host, std::shared_ptr<StreamPtr> stream);

/// A column of a batch on the device: the memory of its values, or of the bytes and the offsets of the rows of a
/// `String`, and the view of its rows.
class GPUDeviceColumn
{
public:
    const DeviceColumnView & view() const;

private:
    friend GPUFuture<GPUDeviceBatch> copyHostToDevice(const GPUHostBatch & host, std::shared_ptr<StreamPtr> stream);

    /// Places the column in memory of its own, and adds its blocks to `decompression` to expand there.
    GPUDeviceColumn(const GPUHostColumn & host, DecompressionBatch & decompression, cuda::stream_ref stream);

    /// Once its blocks are expanded: of `rows` rows, of a `String` its offsets from its sizes.
    void finish(size_t rows, cuda::stream_ref stream);

    bool isVariable() const;

    GPUElementType type;
    DeviceBuffer values_memory;
    /// Of a `String`: a zero, then the sizes of its rows, which become their offsets.
    DeviceBuffer offsets_memory;

    /// The values, or the bytes of a `String`, from its first row, in `values_memory`, and how many there are.
    char * values = nullptr;
    size_t values_bytes = 0;
    /// Of a `String`: its offsets in `offsets_memory`, one more than its rows, and the bytes of its sizes after the first.
    uint64_t * offsets = nullptr;
    size_t sizes_bytes = 0;

    std::optional<DeviceColumnView> column_view;
};

/// The columns of a batch on the device, as `GPUHostBatch` is on the host: the future of `copyHostToDevice`, see `takeOn`.
/// Freed in the order of the stream it was copied on, after the stream it was taken on is done with it.
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
    friend GPUFuture<GPUDeviceBatch> copyHostToDevice(const GPUHostBatch & host, std::shared_ptr<StreamPtr> stream);
    friend GPUDeviceBatch takeOn(GPUFuture<GPUDeviceBatch> future, cuda::stream_ref stream);

    explicit GPUDeviceBatch(std::shared_ptr<StreamPtr> copy_stream_);

    /// The stream that reads it, whose promise the stream it was copied on waits for before it frees the memory.
    void useOn(cuda::stream_ref stream);

    /// Shared, so the memory is freed in the order of the stream after the one who made it is gone.
    std::shared_ptr<StreamPtr> copy_stream;
    std::vector<GPUDeviceColumn> device_columns;
    size_t num_rows = 0;
    /// That the stream it was taken on is done with it.
    std::optional<GPUPromise<void>> released;
};

/// The batch, for the work queued on `stream` from now on, which waits on the device until the batch is ready.
GPUDeviceBatch takeOn(GPUFuture<GPUDeviceBatch> future, cuda::stream_ref stream);

}

#endif
