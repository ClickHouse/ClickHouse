#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.cuh>

#include <Processors/DeviceChunk.h>

#include <cuda/stream_ref>

#include <memory>
#include <vector>

namespace DB::GPU
{

/** Columns of a chunk on the GPU, see `DeviceChunkInfo`: views of the device memory that `owners` keep, there once
  * `ready` has happened in the stream that made them.
  */
class GPUDeviceColumns final : public IDeviceColumns
{
public:
    /// Records `ready` in `stream`, after what makes the columns.
    GPUDeviceColumns(std::vector<DeviceColumnView> views_, size_t rows_, std::vector<std::shared_ptr<const void>> owners_, cuda::stream_ref stream);

    size_t rows() const override { return num_rows; }

    Columns materialize(const Block & header) const override;

    DeviceColumnsPtr selectColumns(const std::vector<size_t> & positions) const override { return select(positions); }

    const std::vector<DeviceColumnView> & views() const { return column_views; }
    const std::vector<std::shared_ptr<const void>> & owners() const { return memory_owners; }

    /// Makes `stream` wait until the columns are there.
    void waitIn(cuda::stream_ref stream) const;

    /// The columns at `positions`, in that order, of the same memory and there at the same point.
    std::shared_ptr<const GPUDeviceColumns> select(const std::vector<size_t> & positions) const;

    /// Rows `offset` to `offset + rows` of every column, which are all of a fixed width, of the same memory and there at
    /// the same point.
    std::shared_ptr<const GPUDeviceColumns> slice(size_t offset, size_t rows) const;

private:
    using SharedEvent = std::shared_ptr<std::remove_pointer_t<cudaEvent_t>>;

    GPUDeviceColumns(std::vector<DeviceColumnView> views_, size_t rows_, std::vector<std::shared_ptr<const void>> owners_, SharedEvent ready_);

    std::vector<DeviceColumnView> column_views;
    size_t num_rows;
    std::vector<std::shared_ptr<const void>> memory_owners;
    SharedEvent ready;
};

/// The device columns of a chunk, or nothing for a chunk in host memory or on another device.
std::shared_ptr<const GPUDeviceColumns> gpuColumnsOf(const Chunk & chunk);

}

#endif
