#pragma once

#include <GPU/GPUExpression.cuh>
#include <GPU/GPUResources.cuh>
#include <GPU/GPUTypes.cuh>

#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/error.hpp>

#include <rmm/device_buffer.hpp>
#include <rmm/error.hpp>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>

extern "C" void * __cxa_current_primary_exception() noexcept;

namespace DB::GPU
{

/// The message of a libstdc++ `std::logic_error` or `std::runtime_error`, read without `what`: the linker keeps the
/// libc++ `what` of the same symbol, which reads past a libstdc++ exception. libstdc++ keeps the message pointer right
/// after the vtable pointer.
template <typename StandardException>
__host__ const char * messageOf(const StandardException & exception)
{
#if !defined(__GLIBCXX__)
#    error "The device code is expected to be compiled with libstdc++"
#endif
    static_assert(std::is_same_v<StandardException, std::logic_error> || std::is_same_v<StandardException, std::runtime_error>);
    static_assert(sizeof(StandardException) == 2 * sizeof(void *));
    return *reinterpret_cast<const char * const *>(reinterpret_cast<const char *>(&exception) + sizeof(void *));
}

/// Runs `body` and rethrows what cuDF or rmm throw as a ClickHouse exception saying what was being done. Of other
/// exceptions `what` is not read, as they may be of the other standard library; the device's last error is said instead.
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

        std::string message = doing + ": ";
        if (dynamic_cast<const cudf::cuda_error *>(&exception))
            message += "CUDA error: ";

        /// rmm's own `what` is of its header, compiled here.
        if (const auto * out_of_memory = dynamic_cast<const rmm::out_of_memory *>(&exception))
            message += std::string("out of device memory: ") + out_of_memory->what();
        else if (const auto * runtime = dynamic_cast<const std::runtime_error *>(&exception))
            message += messageOf<std::runtime_error>(*runtime);
        else if (const auto * logic = dynamic_cast<const std::logic_error *>(&exception))
            message += messageOf<std::logic_error>(*logic);
        else
        {
            message += "an exception the device library does not know";
            if (const cudaError_t last = cudaPeekAtLastError(); last != cudaSuccess)
                message += std::string(", the device reports: ") + cudaGetErrorString(last);
        }
        throwGPUError(message.c_str());
    }
}

__host__ cudf::data_type cudfTypeOf(GPUElementType element_type);

__host__ cudf::column_view columnViewOf(const DeviceColumnView & column, GPUElementType expected_type);

__host__ cudf::column_view columnViewOf(const DeviceFixedColumn & column, GPUElementType expected_type);

__host__ cudf::column_view columnViewOf(const DeviceVariableColumn & column);

__host__ void checkNoNulls(const cudf::column_view & column);

__host__ DeviceFixedColumn deviceViewOf(const cudf::column_view & column, GPUElementType expected_type);

/// A column of a fixed width as `type`, by its bits, of a column that is checked elsewhere to be as wide: cuDF sums unsigned
/// integers into signed ones.
__host__ DeviceFixedColumn deviceViewByBits(const cudf::column_view & column, GPUElementType type);

/// Evaluates `filter`, a predicate, over `inputs` of `rows` rows into `mask`, a bit per row, and returns how many of the
/// rows it leaves out.
__host__ cudf::size_type evaluateMask(
    const GPUExpression & filter, const GPUExpressionInputs & inputs, size_t rows, rmm::device_buffer & mask, cuda::stream_ref stream);

/// The rows of `table` that `mask`, a bit per row, keeps, of which it leaves out `left_out`. `all_true` is filled with as
/// many `true` bytes as there are rows, the values of the column whose nulls are what the mask leaves out.
__host__ std::unique_ptr<cudf::table> keptRows(
    const cudf::table_view & table, const cudf::bitmask_type * mask, cudf::size_type left_out, rmm::device_buffer & all_true, cuda::stream_ref stream);

struct DeviceVariableColumnOfCudf
{
    DeviceVariableColumn view;
    std::unique_ptr<cudf::column> widened_offsets;
};

__host__ DeviceVariableColumnOfCudf deviceViewOfVariable(const cudf::column_view & column, cuda::stream_ref stream);

}
