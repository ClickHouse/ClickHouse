#include "config.h"

#if USE_GPU

#include <GPU/GPUExpression.cuh>

#include <GPU/GPUDevice.h>

#include <Common/Exception.h>

#include <cuda_runtime_api.h>

#include <string>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
}

namespace DB::GPU
{

void evaluateGPUExpression(
    const GPUExpression & expression, const GPUExpressionInputs & inputs, size_t rows, void * result, cuda::stream_ref stream)
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

    std::vector<const void *> columns(inputs.count);
    uint64_t num_rows = rows;
    std::vector<void *> arguments(inputs.count + 2);
    for (uint32_t i = 0; i < inputs.count; ++i)
    {
        columns[i] = inputs.columns[i].data;
        arguments[i] = &columns[i];
    }
    arguments[inputs.count] = &result;
    arguments[inputs.count + 1] = &num_rows;

    const dim3 blocks(static_cast<unsigned>((rows + expression.block_size - 1) / expression.block_size));
    checkCuda(
        cudaLaunchKernel(reinterpret_cast<const void *>(expression.kernel), blocks, dim3(expression.block_size), arguments.data(), 0, stream.get()),
        ("Cannot run kernel " + std::string(expression.name)).c_str());
}

}

#endif
