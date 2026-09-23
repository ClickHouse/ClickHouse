#include <GPU/CudfReduction.h>

#include <GPU/Cudf.h>

#include <cudf/column/column_view.hpp>
#include <cudf/reduction.hpp>
#include <cudf/scalar/scalar.hpp>

#include <algorithm>
#include <bit>
#include <string>

namespace DB::GPU
{

namespace
{

std::unique_ptr<cudf::reduce_aggregation> reduceAggregationFor(GPUAggregationKind aggregation)
{
    switch (aggregation)
    {
        case GPUAggregationKind::Sum: return cudf::make_sum_aggregation<cudf::reduce_aggregation>();
        case GPUAggregationKind::Min: return cudf::make_min_aggregation<cudf::reduce_aggregation>();
        case GPUAggregationKind::Max: return cudf::make_max_aggregation<cudf::reduce_aggregation>();
    }
    throw CudfError("unknown aggregation " + std::to_string(static_cast<int>(aggregation)));
}

template <typename Result>
Result scalarValueAs(const cudf::scalar & value)
{
    const rmm::cuda_stream_view stream = StreamRegistry::get().compute;

    switch (value.type().id())
    {
        case cudf::type_id::UINT8:
            return static_cast<Result>(static_cast<const cudf::numeric_scalar<uint8_t> &>(value).value(stream));
        case cudf::type_id::UINT16:
            return static_cast<Result>(static_cast<const cudf::numeric_scalar<uint16_t> &>(value).value(stream));
        case cudf::type_id::UINT32:
            return static_cast<Result>(static_cast<const cudf::numeric_scalar<uint32_t> &>(value).value(stream));
        case cudf::type_id::UINT64:
            return static_cast<Result>(static_cast<const cudf::numeric_scalar<uint64_t> &>(value).value(stream));
        case cudf::type_id::INT8:
            return static_cast<Result>(static_cast<const cudf::numeric_scalar<int8_t> &>(value).value(stream));
        case cudf::type_id::INT16:
            return static_cast<Result>(static_cast<const cudf::numeric_scalar<int16_t> &>(value).value(stream));
        case cudf::type_id::INT32:
            return static_cast<Result>(static_cast<const cudf::numeric_scalar<int32_t> &>(value).value(stream));
        case cudf::type_id::INT64:
            return static_cast<Result>(static_cast<const cudf::numeric_scalar<int64_t> &>(value).value(stream));
        case cudf::type_id::FLOAT32:
            return static_cast<Result>(static_cast<const cudf::numeric_scalar<float> &>(value).value(stream));
        case cudf::type_id::FLOAT64:
            return static_cast<Result>(static_cast<const cudf::numeric_scalar<double> &>(value).value(stream));
        default:
            throw CudfError(
                "the device returned a scalar of cuDF type " + std::to_string(static_cast<int32_t>(value.type().id()))
                + ", which is not one this path reduces into");
    }
}

uint64_t resultBitsOf(const cudf::scalar & value, GPUElementType result_type)
{
    switch (result_type)
    {
        case GPUElementType::UInt64: return scalarValueAs<uint64_t>(value);
        case GPUElementType::Int64: return static_cast<uint64_t>(scalarValueAs<int64_t>(value));
        case GPUElementType::Float64: return std::bit_cast<uint64_t>(scalarValueAs<double>(value));
        default:
            throw CudfError(
                "a reduction into element type " + std::to_string(static_cast<int>(result_type)) + ", which is not eight bytes wide");
    }
}

template <typename T>
const void * scalarDataAs(const cudf::scalar & value)
{
    return static_cast<const cudf::numeric_scalar<T> &>(value).data();
}

const void * scalarDataOf(const cudf::scalar & value)
{
    switch (value.type().id())
    {
        case cudf::type_id::UINT8: return scalarDataAs<uint8_t>(value);
        case cudf::type_id::UINT16: return scalarDataAs<uint16_t>(value);
        case cudf::type_id::UINT32: return scalarDataAs<uint32_t>(value);
        case cudf::type_id::UINT64: return scalarDataAs<uint64_t>(value);
        case cudf::type_id::INT8: return scalarDataAs<int8_t>(value);
        case cudf::type_id::INT16: return scalarDataAs<int16_t>(value);
        case cudf::type_id::INT32: return scalarDataAs<int32_t>(value);
        case cudf::type_id::INT64: return scalarDataAs<int64_t>(value);
        case cudf::type_id::FLOAT32: return scalarDataAs<float>(value);
        case cudf::type_id::FLOAT64: return scalarDataAs<double>(value);
        default:
            throw CudfError(
                "the device returned a scalar of cuDF type " + std::to_string(static_cast<int32_t>(value.type().id()))
                + ", which is not one this path reduces into");
    }
}

constexpr size_t min_partials_bytes = 4096;

}

CudfReduction::CudfReduction(GPUElementType element_type_, GPUElementType result_type_, GPUAggregationKind aggregation_kind)
    : element_type(element_type_)
    , result_type(result_type_)
    , output_type(cudfTypeOf(aggregation_kind == GPUAggregationKind::Sum ? result_type_ : element_type_))
    , output_size(cudf::size_of(output_type))
    , aggregation(reduceAggregationFor(aggregation_kind))
{
    if (sizeOf(result_type) != 8)
        throw CudfError(
            "a reduction into element type " + std::to_string(static_cast<int>(result_type)) + ", which is not eight bytes wide");

    initializeCudf();
}

void CudfReduction::addBatch(DeviceColumnView values)
{
    if (values.rows == 0)
        throw CudfError("nothing to reduce");

    const rmm::cuda_stream_view stream = StreamRegistry::get().compute;

    const std::unique_ptr<cudf::scalar> batch
        = cudf::reduce(columnViewOf(values, element_type, "a batch of values"), *aggregation, output_type, stream);

    if (batch->type() != output_type)
        throw CudfError(
            "the device reduced a batch into cuDF type " + std::to_string(static_cast<int32_t>(batch->type().id())) + ", expected "
            + std::to_string(static_cast<int32_t>(output_type.id())));

    if ((num_partials + 1) * output_size > partials.size())
        partials.resize(std::max({(num_partials + 1) * output_size, partials.size() * 2, min_partials_bytes}), stream);

    checkCuda(
        cudaMemcpyAsync(
            static_cast<char *>(partials.data()) + num_partials * output_size,
            scalarDataOf(*batch),
            output_size,
            cudaMemcpyDeviceToDevice,
            stream.value()),
        "cannot keep a batch's result on the device");

    ++num_partials;
}

uint64_t CudfReduction::finalize()
{
    if (num_partials == 0)
        return 0;

    const rmm::cuda_stream_view stream = StreamRegistry::get().compute;

    const cudf::column_view partial_column(output_type, static_cast<cudf::size_type>(num_partials), partials.data(), nullptr, 0);
    num_partials = 0;

    const std::unique_ptr<cudf::scalar> value = cudf::reduce(partial_column, *aggregation, output_type, stream);
    if (!value->is_valid(stream))
        throw CudfError("the device returned nothing for non-empty batches of values without nulls");

    return resultBitsOf(*value, result_type);
}

IReduction * IReduction::create(GPUElementType element_type, GPUElementType result_type, GPUAggregationKind aggregation)
{
    return new CudfReduction(element_type, result_type, aggregation);
}

}
