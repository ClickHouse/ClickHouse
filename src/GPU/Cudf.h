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

void checkCuda(cudaError_t status, const std::string & what);

cudf::data_type cudfTypeOf(GPUElementType element_type);

/// Views the column for cuDF, having checked that it is of the type expected of it.
cudf::column_view columnViewOf(DeviceColumnView column, GPUElementType expected_type, const std::string & what);

void checkNoNulls(const cudf::column_view & column, const std::string & what);

/// Queues a copy of a result column into host memory, having checked that it is the plain run of
/// values of the width and length the destination is sized for. The caller synchronizes.
void copyColumnToHost(const cudf::column_view & column, HostColumnView destination, const std::string & what);

}
