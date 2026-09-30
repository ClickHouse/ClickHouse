#include <GPU/Cudf.cuh>

#include <cudf/strings/strings_column_view.hpp>
#include <cudf/unary.hpp>

#include <rmm/error.hpp>

#include <cxxabi.h>

#include <cstdlib>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string>

namespace DB::GPU
{

namespace
{

__host__ std::string typeNameOf(const std::exception & exception)
{
    int status = 0;
    char * demangled = abi::__cxa_demangle(typeid(exception).name(), nullptr, nullptr, &status);
    std::string name = status == 0 && demangled ? demangled : typeid(exception).name();
    std::free(demangled);
    return name;
}

}

__host__ void checkCuda(cudaError_t status, const std::string & what)
{
    if (status != cudaSuccess)
        throwGPUError((what + ": " + cudaGetErrorString(status)).c_str());
}

__host__ cudf::data_type cudfTypeOf(GPUElementType element_type)
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
    throwGPUError(("unknown element type " + std::to_string(static_cast<int>(element_type))).c_str());
}

namespace
{

__host__ cudf::size_type rowsForCudf(size_t rows, const std::string & what)
{
    if (rows > static_cast<size_t>(std::numeric_limits<cudf::size_type>::max()))
        throwGPUError((what + " of " + std::to_string(rows) + " rows is too large for cuDF").c_str());
    return static_cast<cudf::size_type>(rows);
}

}

__host__ cudf::column_view columnViewOf(const DeviceColumnView & column, GPUElementType expected_type, const std::string & what)
{
    if (column.null_mask)
        throwGPUError((what + " arrived with a null mask, which is not taken yet").c_str());

    if (column.type() != expected_type)
        throwGPUError(
            (what + " arrived as a column of element type " + std::to_string(static_cast<int>(column.type())) + ", expected "
            + std::to_string(static_cast<int>(expected_type))).c_str());

    switch (column.kind)
    {
        case GPUColumnKind::Fixed:
            return columnViewOf(column.fixed, expected_type, what);
        case GPUColumnKind::Variable:
            return columnViewOf(column.variable, what);
    }
    throwGPUError(("unknown column kind " + std::to_string(static_cast<int>(column.kind))).c_str());
}

__host__ cudf::column_view columnViewOf(const DeviceFixedColumn & column, GPUElementType expected_type, const std::string & what)
{
    if (columnKindOf(column.element_type) != GPUColumnKind::Fixed)
        throwGPUError((what + " arrived as fixed-width values of a type of varying width").c_str());

    if (column.element_type != expected_type)
        throwGPUError(
            (what + " arrived as element type " + std::to_string(static_cast<int>(column.element_type)) + ", expected "
            + std::to_string(static_cast<int>(expected_type))).c_str());

    return cudf::column_view(cudfTypeOf(column.element_type), rowsForCudf(column.rows, what), column.data, nullptr, 0);
}

__host__ cudf::column_view columnViewOf(const DeviceVariableColumn & column, const std::string & what)
{
    const cudf::size_type rows = rowsForCudf(column.rows, what);

    /// An empty column may come without offsets, and cuDF's own empty strings column has no children either.
    if (rows == 0)
        return cudf::column_view(cudf::data_type{cudf::type_id::STRING}, 0, nullptr, nullptr, 0);

    const cudf::column_view offsets(cudf::data_type{cudf::type_id::INT64}, rows + 1, column.offsets, nullptr, 0);
    return cudf::column_view(cudf::data_type{cudf::type_id::STRING}, rows, column.chars, nullptr, 0, 0, {offsets});
}

__host__ void checkNoNulls(const cudf::column_view & column, const std::string & what)
{
    if (column.null_count() != 0)
        throwGPUError(
            ("the device returned " + std::to_string(column.null_count()) + " nulls in " + what
            + ", where the input had no null mask at all").c_str());
}

__host__ DeviceFixedColumn deviceViewOf(const cudf::column_view & column, GPUElementType expected_type, const std::string & what)
{
    checkNoNulls(column, what);

    if (column.offset() != 0)
        throwGPUError(("the device returned " + what + " as a slice at offset " + std::to_string(column.offset())).c_str());

    if (column.type() != cudfTypeOf(expected_type))
        throwGPUError(
            ("the device returned " + what + " of cuDF type " + std::to_string(static_cast<int32_t>(column.type().id()))
            + ", expected element type " + std::to_string(static_cast<int>(expected_type))).c_str());

    return {.element_type = expected_type, .data = column.head<char>(), .rows = static_cast<size_t>(column.size())};
}

__host__ DeviceVariableColumn deviceViewOfVariable(
    const cudf::column_view & column, std::unique_ptr<cudf::column> & widened_offsets, const std::string & what, rmm::cuda_stream_view stream)
{
    checkNoNulls(column, what);

    if (column.type().id() != cudf::type_id::STRING)
        throwGPUError(("the device returned " + what + " of cuDF type " + std::to_string(static_cast<int32_t>(column.type().id()))).c_str());

    if (column.offset() != 0)
        throwGPUError(("the device returned " + what + " as a slice at offset " + std::to_string(column.offset())).c_str());

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
            throwGPUError(("the offsets of " + what + " are of cuDF type " + std::to_string(static_cast<int32_t>(offsets.type().id()))).c_str());
    }

    if (offsets.size() != column.size() + 1)
        throwGPUError(
            ("the device returned " + std::to_string(offsets.size()) + " offsets of " + what + " of " + std::to_string(column.size())
            + " rows").c_str());

    return {
        .offsets = offsets.data<uint64_t>(),
        .chars = strings.chars_begin(stream),
        .rows = static_cast<size_t>(column.size()),
        .chars_bytes = static_cast<size_t>(strings.chars_size(stream)),
    };
}

}
