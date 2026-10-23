#pragma once

#include <GPU/GPUResources.cuh>
#include <GPU/GPUTypes.cuh>

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/error.hpp>

#include <rmm/error.hpp>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

#include <exception>
#include <memory>
#include <string>

namespace DB::GPU
{

/// Runs `body`, and rethrows what cuDF or rmm throw as a ClickHouse exception that says what was being done. The
/// exceptions of cuDF and rmm are caught by their types, whose `what` is theirs to read; of any other, `what` is not
/// read: the binary holds two standard libraries, and an exception of the other one is not safe to look into. What
/// the device reports is said instead, as a fault on it is what such an exception usually comes of.
template <typename Body>
__host__ auto guarded(const std::string & doing, Body && body)
{
    try
    {
        return body();
    }
    catch (const rmm::out_of_memory & exception)
    {
        throwGPUError((doing + ": out of device memory: " + exception.what()).c_str());
    }
    catch (const cudf::cuda_error & exception)
    {
        throwGPUError((doing + ": CUDA error: " + exception.what()).c_str());
    }
    catch (const cudf::logic_error & exception)
    {
        throwGPUError((doing + ": " + exception.what()).c_str());
    }
    catch (const std::exception & exception)
    {
        if (isClickHouseException(exception))
            throw;

        const cudaError_t last = cudaPeekAtLastError();
        std::string message = doing + ": an exception the device library does not know";
        if (last != cudaSuccess)
            message += std::string(", the device reports: ") + cudaGetErrorString(last);
        throwGPUError(message.c_str());
    }
}

__host__ cudf::data_type cudfTypeOf(GPUElementType element_type);

__host__ cudf::column_view columnViewOf(const DeviceColumnView & column, GPUElementType expected_type);

__host__ cudf::column_view columnViewOf(const DeviceFixedColumn & column, GPUElementType expected_type);

__host__ cudf::column_view columnViewOf(const DeviceVariableColumn & column);

__host__ void checkNoNulls(const cudf::column_view & column);

__host__ DeviceFixedColumn deviceViewOf(const cudf::column_view & column, GPUElementType expected_type);

struct DeviceVariableColumnOfCudf
{
    DeviceVariableColumn view;
    std::unique_ptr<cudf::column> widened_offsets;
};

__host__ DeviceVariableColumnOfCudf deviceViewOfVariable(const cudf::column_view & column, cuda::stream_ref stream);

}
