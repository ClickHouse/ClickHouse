#pragma once

#include <GPU/GPUStreams.h>
#include <GPU/GPUTypes.h>

#include <cudf/column/column_view.hpp>
#include <cudf/types.hpp>

#include <rmm/cuda_stream_view.hpp>

#include <cuda_runtime_api.h>

#include <exception>
#include <string>
#include <utility>

/// What the cuDF side shares. Compiled by nvcc against libstdc++, and never included by
/// ClickHouse's own code: it pulls in cuDF.
namespace DB::GPU
{

/// What this side throws. Its message is a member of its own and so is its `what`: the standard
/// exception classes exist twice in the binary, libc++'s and libstdc++'s, and a `std::logic_error`
/// built here is torn down by libc++'s destructor, which does not know its layout.
class CudfError : public std::exception
{
public:
    explicit CudfError(std::string message_) : message(std::move(message_)) { }

    const char * what() const noexcept override { return message.c_str(); }

private:
    std::string message;
};

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

/** Runs `body` and turns whatever cuDF or the island's standard library throws into a `CudfError`
  * that says what was being done - `doing` - and what was thrown; see `describeForeign` for why
  * the exception is neither asked nor destroyed. A `CudfError` passes through as it is.
  */
template <typename Body>
auto guarded(const std::string & doing, Body && body)
{
    try
    {
        return body();
    }
    catch (const CudfError &)
    {
        throw;
    }
    catch (const std::exception & exception)
    {
        const std::string description = describeForeign(exception);
        keepForeignAlive();
        throw CudfError(doing + ": " + description);
    }
}

void checkCuda(cudaError_t status, const std::string & what);

cudf::data_type cudfTypeOf(GPUElementType element_type);

/// Views the column for cuDF, having checked that it is of the type expected of it: a column of
/// fixed-width values as one of `expected_type`, a variable-width column as cuDF's strings, with
/// `INT64` offsets.
cudf::column_view columnViewOf(DeviceColumnView column, GPUColumnType expected_type, const std::string & what);

/// The same for a column of fixed-width values.
cudf::column_view columnViewOf(DeviceColumnView column, GPUElementType expected_type, const std::string & what);

void checkNoNulls(const cudf::column_view & column, const std::string & what);

/// Views a result column for the host side, having checked that it is a plain run of values of
/// `expected_type`, without nulls and from its first row. The column keeps the memory.
DeviceColumnView deviceViewOf(const cudf::column_view & column, GPUElementType expected_type, const std::string & what);

/// Views a result column of cuDF's strings as a variable-width column for the host side, with its
/// offsets as `offsets`, which are the
/// column's own made `INT64` - cuDF may have left them `INT32`. The offsets start at 0. Reads how
/// many bytes the values span on `stream`.
DeviceColumnView deviceViewOfVariable(
    const cudf::column_view & column, const cudf::column_view & offsets, const std::string & what, rmm::cuda_stream_view stream);

}
