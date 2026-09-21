#pragma once

#include <GPU/GPUTypes.h>

#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/default_stream.hpp>

#include <rmm/cuda_stream_view.hpp>
#include <rmm/device_buffer.hpp>
#include <rmm/mr/cuda_async_memory_resource.hpp>
#include <rmm/mr/per_device_resource.hpp>

#include <cuda_runtime_api.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace DB::GPU
{

inline void writeError(char * error, size_t error_size, const std::string & message)
{
    if (error == nullptr || error_size == 0)
        return;

    const size_t length = std::min(message.size(), error_size - 1);
    std::memcpy(error, message.data(), length);
    error[length] = '\0';
}

inline int handleException(char * error, size_t error_size, const std::string & message)
{
    writeError(error, error_size, message);
    return 1;
}

inline void setUpDeviceMemoryResourceOnce()
{
    static std::once_flag once;
    std::call_once(once, [] { rmm::mr::set_current_device_resource(rmm::mr::cuda_async_memory_resource{}); });
}

struct ElementLayout
{
    cudf::data_type type;
    size_t size;
};

inline ElementLayout elementLayoutOf(GPUElementType element_type)
{
    switch (element_type)
    {
        case GPUElementType::UInt8: return {cudf::data_type{cudf::type_id::UINT8}, 1};
        case GPUElementType::UInt16: return {cudf::data_type{cudf::type_id::UINT16}, 2};
        case GPUElementType::UInt32: return {cudf::data_type{cudf::type_id::UINT32}, 4};
        case GPUElementType::UInt64: return {cudf::data_type{cudf::type_id::UINT64}, 8};
        case GPUElementType::Int8: return {cudf::data_type{cudf::type_id::INT8}, 1};
        case GPUElementType::Int16: return {cudf::data_type{cudf::type_id::INT16}, 2};
        case GPUElementType::Int32: return {cudf::data_type{cudf::type_id::INT32}, 4};
        case GPUElementType::Int64: return {cudf::data_type{cudf::type_id::INT64}, 8};
        case GPUElementType::Float32: return {cudf::data_type{cudf::type_id::FLOAT32}, 4};
        case GPUElementType::Float64: return {cudf::data_type{cudf::type_id::FLOAT64}, 8};
        default: throw std::logic_error("unknown element type " + std::to_string(static_cast<int>(element_type)));
    }
}

inline bool isIntegerElementType(GPUElementType element_type)
{
    switch (element_type)
    {
        case GPUElementType::UInt8:
        case GPUElementType::UInt16:
        case GPUElementType::UInt32:
        case GPUElementType::UInt64:
        case GPUElementType::Int8:
        case GPUElementType::Int16:
        case GPUElementType::Int32:
        case GPUElementType::Int64:
            return true;
        default:
            return false;
    }
}

inline void checkNoNulls(const cudf::column_view & column, const std::string & what)
{
    if (column.null_count() != 0)
        throw std::logic_error(
            "the device returned " + std::to_string(column.null_count()) + " nulls in " + what
            + ", where the input had no null mask at all");
}

struct GPUBufferState
{
    ElementLayout element;
    rmm::device_buffer values;
    size_t used_bytes = 0;
};

inline rmm::cuda_stream_view stream_of(const GPUBufferState &)
{
    return cudf::get_default_stream();
}

inline void checkRowCountFitsCudf(size_t num_rows, const std::string & what)
{
    if (num_rows > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max()))
        throw std::logic_error(what + " of " + std::to_string(num_rows) + " rows is too large for cuDF");
}

inline void reserveColumnBuffer(rmm::device_buffer & buffer, size_t bytes, rmm::cuda_stream_view stream)
{
    if (bytes > buffer.capacity())
        buffer.reserve(std::max(bytes, buffer.capacity() * 2), stream);

    if (bytes > buffer.size())
        buffer.resize(bytes, stream);
}

inline void appendToColumnBuffer(rmm::device_buffer & buffer, const void * host_data, size_t bytes, rmm::cuda_stream_view stream)
{
    const size_t old_size = buffer.size();

    reserveColumnBuffer(buffer, old_size + bytes, stream);

    if (const cudaError_t status = cudaMemcpyAsync(
            static_cast<char *>(buffer.data()) + old_size, host_data, bytes, cudaMemcpyHostToDevice, stream.value());
        status != cudaSuccess)
        throw std::runtime_error(std::string("cannot copy a column of the right table to the device: ") + cudaGetErrorString(status));
}

struct CompressedTotals
{
    size_t compressed = 0;
    size_t decompressed = 0;
    size_t max_decompressed = 0;
};

inline CompressedTotals compressedTotalsOf(
    const size_t * compressed_offsets, const size_t * compressed_bytes, const size_t * decompressed_bytes, size_t num_blocks)
{
    CompressedTotals totals;
    for (size_t i = 0; i < num_blocks; ++i)
    {
        totals.compressed = std::max(totals.compressed, compressed_offsets[i] + compressed_bytes[i]);
        totals.decompressed += decompressed_bytes[i];
        totals.max_decompressed = std::max(totals.max_decompressed, decompressed_bytes[i]);
    }
    return totals;
}


void decompressBlocksIntoDevice(
    GPUCodec codec,
    const void * host_data,
    const size_t * compressed_offsets,
    const size_t * compressed_bytes,
    const size_t * decompressed_bytes,
    size_t num_blocks,
    size_t compressed_total,
    size_t decompressed_total,
    size_t max_decompressed,
    void * destination,
    rmm::cuda_stream_view stream);

}
