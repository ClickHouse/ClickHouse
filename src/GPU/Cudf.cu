#include <GPU/Cudf.cuh>

#include <cudf/strings/strings_column_view.hpp>
#include <cudf/unary.hpp>
#include <cudf/utilities/default_stream.hpp>

#include <rmm/mr/cuda_async_view_memory_resource.hpp>
#include <rmm/mr/per_device_resource.hpp>

#include <cxxabi.h>

#include <cstdlib>
#include <exception>
#include <limits>
#include <mutex>
#include <string>

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

}

std::string describeForeign(const std::exception & exception)
{
    const std::string type = typeNameOf(exception);

    static const char * const without_message[] = {"bad_alloc", "bad_cast", "bad_typeid", "bad_function_call", "bad_variant_access", "bad_optional_access", "bad_exception", "std::exception"};
    for (const char * bare : without_message)
    {
        if (type.find(bare) != std::string::npos)
            return type;
    }

    const char * message = *reinterpret_cast<const char * const *>(reinterpret_cast<const char *>(&exception) + sizeof(void *));
    return type + ": " + (message ? message : "");
}

void checkCuda(cudaError_t status, const std::string & what)
{
    if (status != cudaSuccess)
        throwGPUError(what + ": " + cudaGetErrorString(status));
}

void initializeCudf()
{
    static std::mutex mutex;
    static bool initialized = false;

    std::lock_guard lock(mutex);
    if (initialized)
        return;

    if (!cudf::get_default_stream().is_default() || !StreamRegistry::get().compute.is_default())
        throwGPUError("cuDF's default stream and the compute stream are not both the device's default stream");

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
        case GPUElementType::String: return cudf::data_type{cudf::type_id::STRING};
    }
    throwGPUError("unknown element type " + std::to_string(static_cast<int>(element_type)));
}

namespace
{

cudf::size_type rowsForCudf(size_t rows, const std::string & what)
{
    if (rows > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max()))
        throwGPUError(what + " of " + std::to_string(rows) + " rows is too large for cuDF");
    return static_cast<cudf::size_type>(rows);
}

}

cudf::column_view columnViewOf(const DeviceColumnView & column, GPUElementType expected_type, const std::string & what)
{
    if (column.null_mask)
        throwGPUError(what + " arrived with a null mask, which is not taken yet");

    if (column.type() != expected_type)
        throwGPUError(
            what + " arrived as a column of element type " + std::to_string(static_cast<int>(column.type())) + ", expected "
            + std::to_string(static_cast<int>(expected_type)));

    switch (column.kind)
    {
        case GPUColumnKind::Fixed:
            return columnViewOf(column.fixed, expected_type, what);
        case GPUColumnKind::Variable:
            return columnViewOf(column.variable, what);
    }
    throwGPUError("unknown column kind " + std::to_string(static_cast<int>(column.kind)));
}

cudf::column_view columnViewOf(const DeviceFixedColumn & column, GPUElementType expected_type, const std::string & what)
{
    if (columnKindOf(column.element_type) != GPUColumnKind::Fixed)
        throwGPUError(what + " arrived as fixed-width values of a type of varying width");

    if (column.element_type != expected_type)
        throwGPUError(
            what + " arrived as element type " + std::to_string(static_cast<int>(column.element_type)) + ", expected "
            + std::to_string(static_cast<int>(expected_type)));

    return cudf::column_view(cudfTypeOf(column.element_type), rowsForCudf(column.rows, what), column.data, nullptr, 0);
}

cudf::column_view columnViewOf(const DeviceVariableColumn & column, const std::string & what)
{
    const cudf::size_type rows = rowsForCudf(column.rows, what);
    const cudf::column_view offsets(cudf::data_type{cudf::type_id::INT64}, rows + 1, column.offsets, nullptr, 0);
    return cudf::column_view(cudf::data_type{cudf::type_id::STRING}, rows, column.chars, nullptr, 0, 0, {offsets});
}

void checkNoNulls(const cudf::column_view & column, const std::string & what)
{
    if (column.null_count() != 0)
        throwGPUError(
            "the device returned " + std::to_string(column.null_count()) + " nulls in " + what
            + ", where the input had no null mask at all");
}

DeviceFixedColumn deviceViewOf(const cudf::column_view & column, GPUElementType expected_type, const std::string & what)
{
    checkNoNulls(column, what);

    if (column.offset() != 0)
        throwGPUError("the device returned " + what + " as a slice at offset " + std::to_string(column.offset()));

    if (column.type() != cudfTypeOf(expected_type))
        throwGPUError(
            "the device returned " + what + " of cuDF type " + std::to_string(static_cast<int32_t>(column.type().id()))
            + ", expected element type " + std::to_string(static_cast<int>(expected_type)));

    return {.element_type = expected_type, .data = column.head<char>(), .rows = static_cast<size_t>(column.size())};
}

DeviceVariableColumn deviceViewOfVariable(
    const cudf::column_view & column, std::unique_ptr<cudf::column> & widened_offsets, const std::string & what, rmm::cuda_stream_view stream)
{
    checkNoNulls(column, what);

    if (column.type().id() != cudf::type_id::STRING)
        throwGPUError("the device returned " + what + " of cuDF type " + std::to_string(static_cast<int32_t>(column.type().id())));

    if (column.offset() != 0)
        throwGPUError("the device returned " + what + " as a slice at offset " + std::to_string(column.offset()));

    widened_offsets.reset();
    if (column.size() == 0)
        return {};

    const cudf::strings_column_view strings(column);
    cudf::column_view offsets = strings.offsets();

    switch (offsets.type().id())
    {
        case cudf::type_id::INT64:
            break;
        case cudf::type_id::INT32:
            widened_offsets = guarded(
                "widening the offsets of " + what, [&] { return cudf::cast(offsets, cudf::data_type{cudf::type_id::INT64}, stream); });
            offsets = widened_offsets->view();
            break;
        default:
            throwGPUError("the offsets of " + what + " are of cuDF type " + std::to_string(static_cast<int32_t>(offsets.type().id())));
    }

    if (offsets.size() != column.size() + 1)
        throwGPUError(
            "the device returned " + std::to_string(offsets.size()) + " offsets of " + what + " of " + std::to_string(column.size())
            + " rows");

    return {
        .offsets = offsets.data<uint64_t>(),
        .chars = strings.chars_begin(stream),
        .rows = static_cast<size_t>(column.size()),
        .chars_bytes = static_cast<size_t>(strings.chars_size(stream)),
    };
}

}
