#pragma once

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

namespace DB::GPU
{

/// The message of a `std::logic_error` or a `std::runtime_error` of the standard library this code is compiled with,
/// libstdc++, read without its `what`. The binary holds two standard libraries, whose `what` of these two has the same
/// symbol, and the linker keeps the one of libc++, which reads the message where the exceptions of libc++ keep it: a
/// `cudf::logic_error` or a `cudf::cuda_error`, which do not have a `what` of their own, would make it read past them.
/// In libstdc++ these two keep the message as a reference-counted string, a pointer to its characters, right after the
/// pointer to their virtual functions.
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

/// Keeps the exception being handled, one of cuDF, rmm or libstdc++, from being destroyed. Its destructor calls that of
/// `std::exception` and the like, whose symbols are the same in the two standard libraries of the binary, and which the
/// linker binds to libc++'s, which do not know the layout of libstdc++. The exception is of libc++abi, which the
/// linker binds `__cxa_throw` to as well, and is given a reference that is never dropped: an error of the device leaks it.
__host__ void keepCurrentException();

/// Runs `body`, and rethrows what cuDF or rmm throw as a ClickHouse exception that says what was being done. The
/// exceptions of cuDF and rmm are caught by their types, and their messages read: by the `what` of rmm's own, by
/// `messageOf` of cuDF's. Of any other exception, `what` is not read: the binary holds two standard libraries, and an
/// exception of the other one is not safe to look into. What the device reports is said instead, as a fault on it is
/// what such an exception usually comes of. None of them is destroyed, see `keepCurrentException`.
template <typename Body>
__host__ auto guarded(const std::string & doing, Body && body)
{
    try
    {
        return body();
    }
    catch (const rmm::out_of_memory & exception)
    {
        keepCurrentException();
        throwGPUError((doing + ": out of device memory: " + exception.what()).c_str());
    }
    catch (const cudf::cuda_error & exception)
    {
        keepCurrentException();
        throwGPUError((doing + ": CUDA error: " + messageOf<std::runtime_error>(exception)).c_str());
    }
    catch (const cudf::logic_error & exception)
    {
        keepCurrentException();
        throwGPUError((doing + ": " + messageOf<std::logic_error>(exception)).c_str());
    }
    catch (const std::exception & exception)
    {
        if (isClickHouseException(exception))
            throw;

        keepCurrentException();

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

/// A column of a fixed width as `type`, by its bits, checked to be as wide: cuDF sums unsigned integers into signed ones.
__host__ DeviceFixedColumn deviceViewByBits(const cudf::column_view & column, GPUElementType type);

/// The rows of `table` that `mask`, a bit per row, keeps, of which it leaves out `left_out`. `all_true` is filled with as
/// many `true` bytes as there are rows.
__host__ std::unique_ptr<cudf::table> keptRows(
    const cudf::table_view & table, const cudf::bitmask_type * mask, cudf::size_type left_out, rmm::device_buffer & all_true, cuda::stream_ref stream);

struct DeviceVariableColumnOfCudf
{
    DeviceVariableColumn view;
    std::unique_ptr<cudf::column> widened_offsets;
};

__host__ DeviceVariableColumnOfCudf deviceViewOfVariable(const cudf::column_view & column, cuda::stream_ref stream);

}
