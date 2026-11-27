#include <GPU/Cudf.cuh>

#include <cudf/stream_compaction.hpp>
#include <cudf/strings/strings_column_view.hpp>
#include <cudf/unary.hpp>
#include <cudf/utilities/error.hpp>

#include <rmm/error.hpp>
#include <rmm/mr/cuda_async_view_memory_resource.hpp>
#include <rmm/mr/per_device_resource.hpp>

#include <cstdint>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace DB::GPU
{

__host__ void setCudfMemoryPool(cudaMemPool_t pool)
{
    guarded("making cuDF allocate from the device's memory pool", [&]
    {
        rmm::mr::set_current_device_resource(rmm::mr::cuda_async_view_memory_resource{pool});
    });
}

__host__ void resetCudfMemoryPool()
{
    guarded("resetting the memory resource of cuDF", [] { rmm::mr::reset_current_device_resource(); });
}

namespace
{

/// libstdc++ keeps the message of a `std::logic_error` or a `std::runtime_error` as a pointer after the vtable.
template <typename StandardException>
__host__ const char * messageOf(const StandardException & exception)
{
#if !defined(__GLIBCXX__)
#    error "The device code is expected to be compiled with libstdc++"
#endif
    static_assert(sizeof(StandardException) == 2 * sizeof(void *));
    return *reinterpret_cast<const char * const *>(reinterpret_cast<const char *>(&exception) + sizeof(void *));
}

}

__host__ std::string messageOfDeviceException(const std::exception & exception)
{
    if (const auto * out_of_memory = dynamic_cast<const rmm::out_of_memory *>(&exception))
        return std::string("out of device memory: ") + out_of_memory->what();
    if (const auto * cuda_error = dynamic_cast<const cudf::cuda_error *>(&exception))
        return std::string("CUDA error: ") + messageOf<std::runtime_error>(*cuda_error);
    if (const auto * logic_error = dynamic_cast<const cudf::logic_error *>(&exception))
        return messageOf<std::logic_error>(*logic_error);

    std::string message = "an exception the device library does not know";
    if (const cudaError_t last = cudaPeekAtLastError(); last != cudaSuccess)
        message += std::string(", the device reports: ") + cudaGetErrorString(last);
    return message;
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

/// `max_rows` is below the largest `cudf::size_type` for a column that also needs a count of offsets one larger.
__host__ cudf::size_type rowsForCudf(size_t rows, size_t max_rows)
{
    if (rows > max_rows)
        throwGPUError(("a column of " + std::to_string(rows) + " rows is too large for cuDF").c_str());
    return static_cast<cudf::size_type>(rows);
}

constexpr size_t max_cudf_rows = std::numeric_limits<cudf::size_type>::max();

}

__host__ cudf::column_view columnViewOf(const DeviceColumnView & column, GPUElementType expected_type)
{
    if (column.null_mask)
        throwGPUError("a column for cuDF arrived with a null mask, which is not taken yet");

    if (column.type() != expected_type)
        throwGPUError(
            ("a column for cuDF arrived of element type " + std::to_string(static_cast<int>(column.type())) + ", expected "
            + std::to_string(static_cast<int>(expected_type))).c_str());

    switch (column.kind)
    {
        case GPUColumnKind::Fixed:
            return columnViewOf(column.fixed, expected_type);
        case GPUColumnKind::Variable:
            return columnViewOf(column.variable);
    }
    throwGPUError(("unknown column kind " + std::to_string(static_cast<int>(column.kind))).c_str());
}

__host__ cudf::column_view columnViewOf(const DeviceFixedColumn & column, GPUElementType expected_type)
{
    if (columnKindOf(column.element_type) != GPUColumnKind::Fixed)
        throwGPUError("a column for cuDF arrived as fixed-width values of a type of varying width");

    if (column.element_type != expected_type)
        throwGPUError(
            ("a column for cuDF arrived of element type " + std::to_string(static_cast<int>(column.element_type)) + ", expected "
            + std::to_string(static_cast<int>(expected_type))).c_str());

    /// cuDF reads the values as of their type, which takes them aligned to their size.
    if (reinterpret_cast<uintptr_t>(column.data) % sizeOf(column.element_type) != 0)
        throwGPUError(
            ("a column for cuDF arrived at an address not aligned to its values of " + std::to_string(sizeOf(column.element_type))
            + " bytes").c_str());

    return cudf::column_view(cudfTypeOf(column.element_type), rowsForCudf(column.rows, max_cudf_rows), column.data, nullptr, 0);
}

__host__ cudf::column_view columnViewOf(const DeviceVariableColumn & column)
{
    const cudf::size_type rows = rowsForCudf(column.rows, max_cudf_rows - 1);

    /// An empty column may come without offsets, and cuDF's own empty strings column has no children either.
    if (rows == 0)
        return cudf::column_view(cudf::data_type{cudf::type_id::STRING}, 0, nullptr, nullptr, 0);

    /// cuDF takes signed offsets, and ClickHouse's unsigned ones are the same bytes, as no column reaches 2^63 bytes.
    const cudf::column_view offsets(cudf::data_type{cudf::type_id::INT64}, rows + 1, column.offsets, nullptr, 0);
    return cudf::column_view(cudf::data_type{cudf::type_id::STRING}, rows, column.chars, nullptr, 0, 0, {offsets});
}

__host__ void checkNoNulls(const cudf::column_view & column)
{
    if (column.null_count() != 0)
        throwGPUError(
            ("cuDF returned a column with " + std::to_string(column.null_count())
            + " nulls, where the input had no null mask at all").c_str());
}

__host__ DeviceFixedColumn deviceViewOf(const cudf::column_view & column, GPUElementType expected_type)
{
    checkNoNulls(column);

    if (column.offset() != 0)
        throwGPUError(("cuDF returned a column as a slice at offset " + std::to_string(column.offset())).c_str());

    if (column.type() != cudfTypeOf(expected_type))
        throwGPUError(
            ("cuDF returned a column of type " + std::to_string(static_cast<int32_t>(column.type().id()))
            + ", expected element type " + std::to_string(static_cast<int>(expected_type))).c_str());

    return {.element_type = expected_type, .data = column.head<char>(), .rows = static_cast<size_t>(column.size())};
}

__host__ DeviceFixedColumn deviceViewByBits(const cudf::column_view & column, GPUElementType type)
{
    checkNoNulls(column);
    if (column.offset() != 0)
        throwGPUError(("cuDF returned a column as a slice at offset " + std::to_string(column.offset())).c_str());
    if (cudf::size_of(column.type()) != sizeOf(type))
        throwGPUError(
            ("cuDF returned a column of type " + std::to_string(static_cast<int32_t>(column.type().id()))
            + ", which is not as wide as element type " + std::to_string(static_cast<int>(type))).c_str());
    return {.element_type = type, .data = column.head<char>(), .rows = static_cast<size_t>(column.size())};
}

__host__ std::unique_ptr<cudf::table> keptRows(
    const cudf::table_view & table, DeviceBitmask kept, cudf::size_type left_out, rmm::device_buffer & all_true, cuda::stream_ref stream)
{
    const auto rows = static_cast<size_t>(table.num_rows());
    if (kept.rows != rows)
        throwGPUError(("a filter of " + std::to_string(kept.rows) + " rows over " + std::to_string(rows)).c_str());
    if (all_true.size() < rows)
    {
        all_true.resize(rows, stream);
        checkCuda(cudaMemsetAsync(all_true.data(), 1, rows, stream.get()), "Cannot fill a mask of the device");
    }

    const cudf::column_view mask(cudf::data_type{cudf::type_id::BOOL8}, table.num_rows(), all_true.data(), kept.words, left_out);
    return guarded("compacting " + std::to_string(rows) + " rows to those a filter keeps", [&]
    {
        return cudf::apply_retention_mask(table, mask, stream);
    });
}

__host__ DeviceVariableColumnOfCudf deviceViewOfVariable(const cudf::column_view & column, cuda::stream_ref stream)
{
    checkNoNulls(column);

    if (column.type().id() != cudf::type_id::STRING)
        throwGPUError(
            ("cuDF returned a column of type " + std::to_string(static_cast<int32_t>(column.type().id())) + ", expected strings")
                .c_str());

    if (column.offset() != 0)
        throwGPUError(("cuDF returned a strings column as a slice at offset " + std::to_string(column.offset())).c_str());

    if (column.size() == 0)
        return {};

    const cudf::strings_column_view strings(column);
    cudf::column_view offsets = strings.offsets();
    std::unique_ptr<cudf::column> widened_offsets;

    switch (offsets.type().id())
    {
        case cudf::type_id::INT64:
            break;
        case cudf::type_id::INT32:
            widened_offsets = guarded(
                "widening the offsets of a strings column",
                [&] { return cudf::cast(offsets, cudf::data_type{cudf::type_id::INT64}, stream); });
            offsets = widened_offsets->view();
            /// cuDF returns once the cast is queued, and the offsets may be read on another stream or by the host.
            checkCuda(cudaStreamSynchronize(stream.get()), "Cannot wait for the offsets of a strings column to be widened");
            break;
        default:
            throwGPUError(
                ("cuDF returned a strings column with offsets of type " + std::to_string(static_cast<int32_t>(offsets.type().id())))
                    .c_str());
    }

    if (offsets.size() != column.size() + 1)
        throwGPUError(
            ("cuDF returned a strings column of " + std::to_string(column.size()) + " rows with " + std::to_string(offsets.size())
            + " offsets").c_str());

    /// The same bytes as the signed offsets of cuDF, see `columnViewOf`.
    return {
        .view = {
            .offsets = offsets.data<uint64_t>(),
            .chars = strings.chars_begin(stream),
            .rows = static_cast<size_t>(column.size()),
            .chars_bytes = static_cast<size_t>(strings.chars_size(stream)),
        },
        .widened_offsets = std::move(widened_offsets),
    };
}

}
