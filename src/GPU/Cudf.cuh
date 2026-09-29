#pragma once

#include <GPU/GPUStreams.cuh>
#include <GPU/GPUTypes.cuh>

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>

#include <rmm/cuda_stream_view.hpp>

#include <cuda_runtime_api.h>

#include <exception>
#include <memory>
#include <string>

namespace DB::GPU
{

[[noreturn]] inline void throwGPUError(const std::string & message)
{
    throwGPUError(message.c_str());
}

void initializeCudf();

std::string describeForeign(const std::exception & exception);

void releaseForeign(const std::exception & exception);

template <typename Body>
auto guarded(const std::string & doing, Body && body)
{
    try
    {
        return body();
    }
    catch (const std::exception & exception)
    {
        if (isClickHouseException(exception))
            throw;

        const std::string description = describeForeign(exception);
        releaseForeign(exception);
        throwGPUError(doing + ": " + description);
    }
}

void checkCuda(cudaError_t status, const std::string & what);

cudf::data_type cudfTypeOf(GPUElementType element_type);

cudf::column_view columnViewOf(const DeviceColumnView & column, GPUElementType expected_type, const std::string & what);

cudf::column_view columnViewOf(const DeviceFixedColumn & column, GPUElementType expected_type, const std::string & what);

cudf::column_view columnViewOf(const DeviceVariableColumn & column, const std::string & what);

void checkNoNulls(const cudf::column_view & column, const std::string & what);

DeviceFixedColumn deviceViewOf(const cudf::column_view & column, GPUElementType expected_type, const std::string & what);

DeviceVariableColumn deviceViewOfVariable(
    const cudf::column_view & column, std::unique_ptr<cudf::column> & widened_offsets, const std::string & what, rmm::cuda_stream_view stream);

}
