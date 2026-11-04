#include "config.h"

#if USE_GPU

#include <GPU/GPUExpression.cuh>

#include <GPU/GPUDevice.h>

#include <Common/Exception.h>

#include <cuda_runtime_api.h>

#include <string>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
}

namespace DB::GPU
{

namespace
{

void launch(const GPUExpression & expression, const GPUExpressionInputs & inputs, size_t rows, void * out, cuda::stream_ref stream)
{
    if (rows == 0)
        return;

    if (!expression.kernel || expression.block_size == 0 || expression.block_size % 32 != 0)
        throw Exception(ErrorCodes::GPU_ERROR, "Kernel {} not loaded, or of blocks of {} threads", expression.name, expression.block_size);
    if (inputs.count != expression.num_columns)
        throw Exception(ErrorCodes::GPU_ERROR, "Kernel {} of {} columns run over {}", expression.name, expression.num_columns, inputs.count);
    for (uint32_t i = 0; i < inputs.count; ++i)
    {
        if (inputs.columns[i].type != expression.column_types[i])
            throw Exception(ErrorCodes::GPU_ERROR, "Kernel {} run over column {} of another type than it was compiled for", expression.name, i);
    }

    const void * columns[max_expression_columns];
    uint64_t num_rows = rows;
    void * arguments[max_expression_columns + 2];
    for (uint32_t i = 0; i < inputs.count; ++i)
    {
        columns[i] = inputs.columns[i].data;
        arguments[i] = &columns[i];
    }
    arguments[inputs.count] = &out;
    arguments[inputs.count + 1] = &num_rows;

    const dim3 blocks(static_cast<unsigned>((rows + expression.block_size - 1) / expression.block_size));
    checkCuda(
        cudaLaunchKernel(reinterpret_cast<const void *>(expression.kernel), blocks, dim3(expression.block_size), arguments, 0, stream.get()),
        ("Cannot run kernel " + std::string(expression.name)).c_str());
}

}

void evaluatePredicateToBitmask(
    const GPUExpression & expression, const GPUExpressionInputs & inputs, size_t rows, uint32_t * mask, cuda::stream_ref stream)
{
    launch(expression, inputs, rows, mask, stream);
}

void evaluateDerivedValue(
    const GPUDerivedValue & derived, GPUSpan<DeviceFixedColumn> values, size_t rows, double * out, cuda::stream_ref stream)
{
    if (derived.num_columns > max_expression_columns)
        throw Exception(ErrorCodes::GPU_ERROR, "A derived value of {} columns", derived.num_columns);

    GPUExpressionInputs inputs;
    inputs.count = derived.num_columns;
    for (uint32_t i = 0; i < derived.num_columns; ++i)
    {
        const uint32_t column = derived.columns[i];
        if (column >= values.size())
            throw Exception(ErrorCodes::GPU_ERROR, "A derived value of value column {} of {}", column, values.size());
        if (values[column].rows < rows)
            throw Exception(ErrorCodes::GPU_ERROR, "A derived value of {} rows over a column of {}", rows, values[column].rows);
        inputs.columns[i] = {.data = values[column].data, .type = values[column].element_type};
    }

    launch(derived.expression, inputs, rows, out, stream);
}

}

#endif
