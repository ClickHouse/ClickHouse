#include <GPU/Cudf.h>

#include <cudf/utilities/default_stream.hpp>

#include <rmm/mr/cuda_async_view_memory_resource.hpp>
#include <rmm/mr/per_device_resource.hpp>

#include <limits>
#include <mutex>
#include <string>

namespace DB::GPU
{

rmm::cuda_stream_view cudfStream()
{
    return cudf::get_default_stream();
}

void checkCuda(cudaError_t status, const std::string & what)
{
    if (status != cudaSuccess)
        throw CudfError(what + ": " + cudaGetErrorString(status));
}

void initializeCudf()
{
    static std::once_flag once;
    std::call_once(once, []
    {
        if (!cudfStream().is_default())
            throw CudfError("cuDF runs on a stream other than the device's default one, where the host side queues its uploads");

        int device = 0;
        checkCuda(cudaGetDevice(&device), "cannot tell the current CUDA device");

        cudaMemPool_t pool = nullptr;
        checkCuda(cudaDeviceGetDefaultMemPool(&pool, device), "cannot get the device's default memory pool");

        rmm::mr::set_current_device_resource(rmm::mr::cuda_async_view_memory_resource{pool});
    });
}

cudf::data_type cudfTypeOf(GPUElementType element_type)
{
    switch (element_type)
    {
        case GPUElementType::UInt8: return cudf::data_type{cudf::type_id::UINT8};
        case GPUElementType::UInt16: return cudf::data_type{cudf::type_id::UINT16};
        case GPUElementType::UInt32: return cudf::data_type{cudf::type_id::UINT32};
        case GPUElementType::UInt64: return cudf::data_type{cudf::type_id::UINT64};
        case GPUElementType::Int8: return cudf::data_type{cudf::type_id::INT8};
        case GPUElementType::Int16: return cudf::data_type{cudf::type_id::INT16};
        case GPUElementType::Int32: return cudf::data_type{cudf::type_id::INT32};
        case GPUElementType::Int64: return cudf::data_type{cudf::type_id::INT64};
        case GPUElementType::Float32: return cudf::data_type{cudf::type_id::FLOAT32};
        case GPUElementType::Float64: return cudf::data_type{cudf::type_id::FLOAT64};
    }
    throw CudfError("unknown element type " + std::to_string(static_cast<int>(element_type)));
}

cudf::column_view columnViewOf(DeviceColumnView column, GPUElementType expected_type, const std::string & what)
{
    if (column.element_type != expected_type)
        throw CudfError(
            what + " arrived as element type " + std::to_string(static_cast<int>(column.element_type)) + ", expected "
            + std::to_string(static_cast<int>(expected_type)));

    if (column.rows > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max()))
        throw CudfError(what + " of " + std::to_string(column.rows) + " rows is too large for cuDF");

    return cudf::column_view(cudfTypeOf(column.element_type), static_cast<cudf::size_type>(column.rows), column.data, nullptr, 0);
}

void checkNoNulls(const cudf::column_view & column, const std::string & what)
{
    if (column.null_count() != 0)
        throw CudfError(
            "the device returned " + std::to_string(column.null_count()) + " nulls in " + what
            + ", where the input had no null mask at all");
}

void copyColumnToHost(const cudf::column_view & column, HostColumnView destination, const std::string & what)
{
    checkNoNulls(column, what);

    if (column.offset() != 0)
        throw CudfError("the device returned " + what + " as a slice at offset " + std::to_string(column.offset()));

    if (static_cast<size_t>(column.size()) != destination.rows)
        throw CudfError(
            "the device returned " + what + " of " + std::to_string(column.size()) + " rows into room for "
            + std::to_string(destination.rows));

    const size_t element_size = cudf::size_of(column.type());
    if (element_size != sizeOf(destination.element_type))
        throw CudfError(
            "the device returned " + what + " of " + std::to_string(element_size) + "-byte values into a column of "
            + std::to_string(sizeOf(destination.element_type)) + "-byte ones");

    checkCuda(
        cudaMemcpyAsync(destination.data, column.head<void>(), destination.rows * element_size, cudaMemcpyDeviceToHost, cudfStream().value()),
        "cannot copy " + what + " back");
}

}
