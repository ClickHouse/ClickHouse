#include <GPU/Cudf.cuh>

#include <cudf/utilities/default_stream.hpp>

#include <rmm/mr/cuda_async_view_memory_resource.hpp>
#include <rmm/mr/per_device_resource.hpp>

#include <cxxabi.h>

#include <cstdlib>
#include <exception>
#include <limits>
#include <mutex>
#include <string>
#include <vector>

namespace DB::GPU
{

namespace
{

std::string typeNameOf(const std::exception & exception)
{
    int status = 0;
    char * demangled = abi::__cxa_demangle(typeid(exception).name(), nullptr, nullptr, &status);
    std::string name = status == 0 && demangled ? demangled : typeid(exception).name();
    std::free(demangled);
    return name;
}

std::vector<std::exception_ptr> & foreignExceptions()
{
    static std::vector<std::exception_ptr> kept;
    return kept;
}

std::mutex foreign_exceptions_mutex;

}

std::string describeForeign(const std::exception & exception)
{
    const std::string type = typeNameOf(exception);

    /// Every exception cuDF and rmm throw derives from `std::logic_error` or `std::runtime_error`,
    /// whose libstdc++ layout is the vtable pointer and then the message's characters, behind one
    /// pointer; the standard exceptions without a message do not.
    static const char * const without_message[] = {"bad_alloc", "bad_cast", "bad_typeid", "bad_function_call", "bad_variant_access", "bad_optional_access", "bad_exception", "std::exception"};
    for (const char * bare : without_message)
    {
        if (type.find(bare) != std::string::npos)
            return type;
    }

    const char * message = *reinterpret_cast<const char * const *>(reinterpret_cast<const char *>(&exception) + sizeof(void *));
    return type + ": " + (message ? message : "");
}

void keepForeignAlive()
{
    std::lock_guard lock(foreign_exceptions_mutex);
    foreignExceptions().push_back(std::current_exception());
}

void checkCuda(cudaError_t status, const std::string & what)
{
    if (status != cudaSuccess)
        throw CudfError(what + ": " + cudaGetErrorString(status));
}

/// Not `std::call_once`: an exception out of its callable aborts the process in the island's
/// standard library instead of reaching the query, and a device that is missing or refuses its
/// memory pool is a query's error, not the server's.
void initializeCudf()
{
    static std::mutex mutex;
    static bool initialized = false;

    std::lock_guard lock(mutex);
    if (initialized)
        return;

    /// The island's allocations and cuDF's own calls without a stream go to cuDF's default stream,
    /// and the host side queues what the kernels read on the compute stream, so the two have to
    /// be the one default stream of the device, whichever handle spells it.
    if (!cudf::get_default_stream().is_default() || !rmm::cuda_stream_view{StreamRegistry::get().compute}.is_default())
        throw CudfError("cuDF's default stream and the compute stream are not both the device's default stream");

    int device = 0;
    checkCuda(cudaGetDevice(&device), "cannot tell the current CUDA device");

    cudaMemPool_t pool = nullptr;
    checkCuda(cudaDeviceGetDefaultMemPool(&pool, device), "cannot get the device's default memory pool");

    rmm::mr::set_current_device_resource(rmm::mr::cuda_async_view_memory_resource{pool});
    initialized = true;
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
        cudaMemcpyAsync(destination.data, column.head<void>(), destination.rows * element_size, cudaMemcpyDeviceToHost, StreamRegistry::get().compute),
        "cannot copy " + what + " back");
}

}
