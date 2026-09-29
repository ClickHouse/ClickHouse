#pragma once

#include <GPU/GPUStreams.cuh>
#include <GPU/GPUTypes.cuh>

#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>

#include <rmm/cuda_stream_view.hpp>

#include <cuda_runtime_api.h>

#include <exception>
#include <string>

/// What the cuDF side shares. Compiled by nvcc against libstdc++, and never included by
/// ClickHouse's own code: it pulls in cuDF.
namespace DB::GPU
{

[[noreturn]] inline void throwGPUError(const std::string & message)
{
    throwGPUError(message.c_str());
}

/// Points cuDF's allocations at the device's default memory pool, which the host side allocates
/// from as well. Runs once.
void initializeCudf();

/** What an exception cuDF or the island's standard library threw says, put together without
  * calling anything of the exception's own. The standard exception classes exist twice in the
  * binary, libc++'s and libstdc++'s, and the process binds the names both define - `what`, the
  * destructors - to libc++'s definitions, which do not know the libstdc++ layout of an exception
  * built here. So the message is read from that layout: a `std::logic_error` or
  * `std::runtime_error` of libstdc++ keeps its message as a pointer right after the vtable pointer.
  */
std::string describeForeign(const std::exception & exception);

/// Keeps the exception the handler caught alive for the rest of the process: were it destroyed,
/// as leaving the handler would, it would be by libc++'s destructor, which does not know it.
void keepForeignAlive();

/** Runs `body` and turns whatever cuDF or the island's standard library throws into a
  * `DB::Exception` of `GPU_ERROR` that says what was being done - `doing` - and what was thrown;
  * see `describeForeign` for why the exception is neither asked nor destroyed. A `DB::Exception`
  * passes through as it is.
  */
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
        keepForeignAlive();
        throwGPUError(doing + ": " + description);
    }
}

void checkCuda(cudaError_t status, const std::string & what);

cudf::data_type cudfTypeOf(GPUElementType element_type);

/// Views the column for cuDF, having checked that it is of the type expected of it and has no
/// null mask: a column of fixed-width values as one of `expected_type`, a variable-width column
/// as cuDF's strings, with `INT64` offsets.
cudf::column_view columnViewOf(const DeviceColumnView & column, GPUElementType expected_type, const std::string & what);

/// The same for a column of fixed-width values.
cudf::column_view columnViewOf(const DeviceFixedColumn & column, GPUElementType expected_type, const std::string & what);

cudf::column_view columnViewOf(const DeviceVariableColumn & column, const std::string & what);

void checkNoNulls(const cudf::column_view & column, const std::string & what);

/// Views a result column for the host side, having checked that it is a plain run of values of
/// `expected_type`, without nulls and from its first row. The column keeps the memory.
DeviceFixedColumn deviceViewOf(const cudf::column_view & column, GPUElementType expected_type, const std::string & what);

/// Views a result column of cuDF's strings as a variable-width column for the host side, with `offsets` as its offsets,
/// which are the column's own made `INT64` - cuDF may have left them `INT32`. Reads how many bytes
/// the values span on `stream`.
DeviceVariableColumn deviceViewOfVariable(
    const cudf::column_view & column, const cudf::column_view & offsets, const std::string & what, rmm::cuda_stream_view stream);

}
