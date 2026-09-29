#include <GPU/CudfReduction.cuh>

#include <GPU/Cudf.cuh>

#include <cudf/column/column.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/column/column_view.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/reduction.hpp>
#include <cudf/scalar/scalar.hpp>

#include <memory>
#include <string>
#include <vector>

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
    throwGPUError(("unknown aggregation " + std::to_string(static_cast<int>(aggregation))).c_str());
}

/// How many batches' results are kept before they are reduced into one.
constexpr size_t max_partials = 1024;

}

struct CudfReduction::State
{
    const GPUElementType element_type;
    const GPUElementType result_type;
    const cudf::data_type output_type;
    const std::unique_ptr<cudf::reduce_aggregation> aggregation;

    /// The results of the batches, a row each.
    std::vector<std::unique_ptr<cudf::column>> partials;
    std::unique_ptr<cudf::column> result;
    std::unique_ptr<cudf::column> result_offsets;

    State(GPUElementType element_type_, GPUElementType result_type_, GPUAggregationKind aggregation_kind)
        : element_type(element_type_)
        , result_type(result_type_)
        , output_type(cudfTypeOf(result_type_))
        , aggregation(reduceAggregationFor(aggregation_kind))
    {
    }

    std::unique_ptr<cudf::column> reduce(const cudf::column_view & values, const std::string & what) const
    {
        const rmm::cuda_stream_view stream = StreamRegistry::get().compute;

        const std::unique_ptr<cudf::scalar> value = guarded(what, [&] { return cudf::reduce(values, *aggregation, output_type, stream); });
        if (value->type() != output_type)
            throwGPUError(
                ("the device reduced into cuDF type " + std::to_string(static_cast<int32_t>(value->type().id())) + ", expected "
                + std::to_string(static_cast<int32_t>(output_type.id()))).c_str());
        if (!value->is_valid(stream))
            throwGPUError("the device returned nothing for non-empty batches of values without nulls");

        return guarded("keeping a reduction on the device", [&] { return cudf::make_column_from_scalar(*value, 1, stream); });
    }

    void foldPartials()
    {
        if (partials.size() <= 1)
            return;

        std::vector<cudf::column_view> views;
        views.reserve(partials.size());
        for (const auto & partial : partials)
            views.push_back(partial->view());

        const std::unique_ptr<cudf::column> gathered
            = guarded("gathering " + std::to_string(views.size()) + " batches' results", [&] { return cudf::concatenate(views, StreamRegistry::get().compute); });
        partials.clear();
        partials.push_back(reduce(gathered->view(), "reducing the batches' results"));
    }
};

CudfReduction::CudfReduction(GPUElementType element_type, GPUElementType result_type, GPUAggregationKind aggregation)
{
    if (aggregation == GPUAggregationKind::Sum && sizeOf(result_type) != 8)
        throwGPUError(("a sum into element type " + std::to_string(static_cast<int>(result_type)) + ", which is not eight bytes wide").c_str());
    if (aggregation == GPUAggregationKind::Sum && columnKindOf(element_type) != GPUColumnKind::Fixed)
        throwGPUError("a sum of values of varying width");

    state = new State(element_type, result_type, aggregation);
}

CudfReduction::~CudfReduction()
{
    delete state;
}

void CudfReduction::addBatch(DeviceColumnView values)
{
    if (values.rows() == 0)
        throwGPUError("nothing to reduce");

    state->partials.push_back(state->reduce(
        columnViewOf(values, state->element_type, "a batch of values"), "reducing a batch of " + std::to_string(values.rows()) + " values"));

    if (state->partials.size() >= max_partials)
        state->foldPartials();
}

DeviceColumnView CudfReduction::finalize()
{
    state->result.reset();
    state->result_offsets.reset();

    if (state->partials.empty())
    {
        if (columnKindOf(state->result_type) == GPUColumnKind::Variable)
            return DeviceVariableColumn{};
        return DeviceFixedColumn{.element_type = state->result_type};
    }

    state->foldPartials();
    state->result = std::move(state->partials.front());
    state->partials.clear();

    const std::string what = "the result of a reduction";
    if (columnKindOf(state->result_type) == GPUColumnKind::Variable)
        return deviceViewOfVariable(state->result->view(), state->result_offsets, what, StreamRegistry::get().compute.value());
    return deviceViewOf(state->result->view(), state->result_type, what);
}

}
