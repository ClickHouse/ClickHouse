#include <GPU/GPUDeviceExpression.h>

#if USE_GPU

#include <GPU/GPUExpression.cuh>
#include <GPU/GPUFilterCompiler.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypeMapping.h>

#include <Interpreters/ExpressionActions.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

std::unique_ptr<DeviceExpression> DeviceExpression::tryCompile(
    const ExpressionActions & actions, const Block & input_header, const Block & output_header)
{
    const ActionsDAG & dag = actions.getActionsDAG();
    auto result = std::unique_ptr<DeviceExpression>(new DeviceExpression);
    result->outputs.reserve(output_header.columns());

    for (const auto & column : output_header)
    {
        const ActionsDAG::Node * node = dag.tryFindInOutputs(column.name);
        if (!node)
        {
            /// A column of the input the actions pass through as it is.
            if (!input_header.has(column.name))
                return nullptr;
            result->outputs.push_back({.input_position = input_header.getPositionByName(column.name), .derived = {}, .derived_inputs = {}});
            continue;
        }

        const ActionsDAG::Node * source = node;
        while (source->type == ActionsDAG::ActionType::ALIAS && source->children.size() == 1)
            source = source->children.front();

        if (source->type == ActionsDAG::ActionType::INPUT)
        {
            if (!input_header.has(source->result_name) || !input_header.getByName(source->result_name).type->equals(*column.type))
                return nullptr;
            result->outputs.push_back({.input_position = input_header.getPositionByName(source->result_name), .derived = {}, .derived_inputs = {}});
            continue;
        }

        String refusal;
        auto compiled = compileGPUExpression(dag, column.name, refusal);
        if (!compiled || compiled->columns.size() > max_filter_columns)
            return nullptr;

        Output output;
        GPUDerivedValue derived;
        derived.program = compiled->program;
        derived.num_columns = static_cast<uint32_t>(compiled->columns.size());
        for (size_t i = 0; i < compiled->columns.size(); ++i)
        {
            const auto & input = compiled->columns[i];
            if (!input_header.has(input.name) || !input_header.getByName(input.name).type->equals(*input.type))
                return nullptr;
            derived.columns[i] = static_cast<uint32_t>(i);
            output.derived_inputs.push_back(input_header.getPositionByName(input.name));
        }
        output.derived = derived;
        result->outputs.push_back(std::move(output));
    }

    return result;
}

std::shared_ptr<const GPUDeviceColumns> DeviceExpression::execute(const GPUDeviceColumns & input) const
{
    /// On a stream of the thread that runs the actions, so that the threads compute theirs at once.
    thread_local StreamPtr thread_stream = createStream();
    const cuda::stream_ref stream{thread_stream.get()};

    const size_t rows = input.rows();
    const auto & views = input.views();
    std::vector<DeviceColumnView> result;
    std::vector<std::shared_ptr<const void>> owners = input.owners();
    result.reserve(outputs.size());

    bool waited = false;
    for (const Output & output : outputs)
    {
        if (output.input_position)
        {
            result.push_back(views.at(*output.input_position));
            continue;
        }

        if (!waited)
        {
            input.waitIn(stream);
            waited = true;
        }

        std::vector<DeviceFixedColumn> values;
        values.reserve(output.derived_inputs.size());
        for (const size_t position : output.derived_inputs)
        {
            const DeviceColumnView & view = views.at(position);
            if (view.kind != GPUColumnKind::Fixed || view.fixed.element_type != GPUElementType::Float64)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A device expression computed from device column {} of other than `Float64`", position);
            values.push_back(view.fixed);
        }

        auto out = std::make_shared<DeviceBuffer>(stream);
        auto * const data = reinterpret_cast<double *>(out->grow(rows * sizeof(double)));
        evaluateDerivedValue(*output.derived, values, rows, data, stream);
        result.push_back(DeviceFixedColumn{.element_type = GPUElementType::Float64, .data = reinterpret_cast<const char *>(data), .rows = rows});
        owners.push_back(std::move(out));
    }

    /// Of columns passed through alone, they are there when the input's are, which `select` keeps.
    if (!waited)
    {
        std::vector<size_t> positions;
        positions.reserve(outputs.size());
        for (const Output & output : outputs)
            positions.push_back(*output.input_position);
        return input.select(positions);
    }
    return std::make_shared<GPUDeviceColumns>(std::move(result), rows, std::move(owners), stream);
}

}

#endif
