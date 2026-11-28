#pragma once

#include <GPU/GPUExpression.cuh>
#include <GPU/GPUResources.cuh>
#include <GPU/GPUTypes.cuh>

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>

#include <rmm/device_buffer.hpp>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

#include <exception>
#include <memory>
#include <string>

extern "C" void * __cxa_current_primary_exception() noexcept;

namespace DB::GPU
{

/// cuDF and rmm are of libstdc++: their `what` would bind to libc++'s.
__host__ std::string messageOfDeviceException(const std::exception & exception);

template <typename Body>
__host__ auto guarded(const std::string & doing, Body && body)
{
    try
    {
        return body();
    }
    catch (const std::exception & exception)
    {
        if (isClickHouseException(exception))
            throw;

        /// Leaked: its destructor would bind to libc++'s too.
        static_cast<void>(__cxa_current_primary_exception());
        throwGPUError((doing + ": " + messageOfDeviceException(exception)).c_str());
    }
}

__host__ cudf::data_type cudfTypeOf(GPUElementType element_type);

__host__ cudf::column_view columnViewOf(const DeviceColumnView & column, GPUElementType expected_type);

__host__ cudf::column_view columnViewOf(const DeviceFixedColumn & column, GPUElementType expected_type);

__host__ cudf::column_view columnViewOf(const DeviceVariableColumn & column);

__host__ void checkNoNulls(const cudf::column_view & column);

__host__ DeviceFixedColumn deviceViewOf(const cudf::column_view & column, GPUElementType expected_type);

/// cuDF sums unsigned integers into signed ones.
__host__ DeviceFixedColumn deviceViewByBits(const cudf::column_view & column, GPUElementType type);

__host__ DeviceBitmask evaluateMask(
    const GPUExpression & filter, const GPUExpressionInputs & inputs, size_t rows, rmm::device_buffer & mask, cuda::stream_ref stream);

/// Waits for the stream.
__host__ cudf::size_type leftOutBy(DeviceBitmask kept, cuda::stream_ref stream);

__host__ std::unique_ptr<cudf::table> keptRows(
    const cudf::table_view & table, DeviceBitmask kept, cudf::size_type left_out, rmm::device_buffer & all_true, cuda::stream_ref stream);

struct DeviceVariableColumnOfCudf
{
    DeviceVariableColumn view;
    std::unique_ptr<cudf::column> widened_offsets;
};

__host__ DeviceVariableColumnOfCudf deviceViewOfVariable(const cudf::column_view & column, cuda::stream_ref stream);

}
