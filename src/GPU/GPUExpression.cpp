#include "config.h"

#if USE_GPU

#include <GPU/GPUExpression.cuh>

#include <GPU/GPUDevice.h>

#include <Common/Exception.h>

#include <cuda_runtime_api.h>

#include <mutex>
#include <string>
#include <unordered_map>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
}

namespace DB::GPU
{

namespace
{

constexpr unsigned threads_per_block = 256;

/// A block is a whole number of warps, so that every thread of a warp reaches the vote of a predicate's kernel.
static_assert(threads_per_block % 32 == 0);

/// The kernel that `GPUExpression::kernel` names, from its PTX loaded once. A module is never unloaded, as the PTX it
/// is loaded from lives as long as the process.
cudaKernel_t kernelOf(const GPUExpression & expression)
{
    static std::mutex mutex;
    static std::unordered_map<std::string, cudaKernel_t> kernels;

    std::lock_guard lock(mutex);
    if (auto it = kernels.find(expression.kernel); it != kernels.end())
        return it->second;

    cudaLibrary_t library = nullptr;
    checkCuda(
        cudaLibraryLoadData(&library, expression.ptx, nullptr, nullptr, 0, nullptr, nullptr, 0),
        ("Cannot load the PTX of kernel " + std::string(expression.kernel)).c_str());

    cudaKernel_t kernel = nullptr;
    checkCuda(cudaLibraryGetKernel(&kernel, library, expression.kernel), ("Cannot find kernel " + std::string(expression.kernel)).c_str());
    kernels.emplace(expression.kernel, kernel);
    return kernel;
}

void launch(const GPUExpression & expression, const GPUExpressionInputs & inputs, size_t rows, void * out, cuda::stream_ref stream)
{
    if (rows == 0)
        return;

    if (inputs.count != expression.num_columns)
        throw Exception(ErrorCodes::GPU_ERROR, "Kernel {} of {} columns run over {}", expression.kernel, expression.num_columns, inputs.count);
    for (uint32_t i = 0; i < inputs.count; ++i)
    {
        if (inputs.columns[i].type != expression.column_types[i])
            throw Exception(ErrorCodes::GPU_ERROR, "Kernel {} run over column {} of another type than it was compiled for", expression.kernel, i);
    }

    const cudaKernel_t kernel = kernelOf(expression);

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

    const dim3 blocks(static_cast<unsigned>((rows + threads_per_block - 1) / threads_per_block));
    checkCuda(
        cudaLaunchKernel(reinterpret_cast<const void *>(kernel), blocks, dim3(threads_per_block), arguments, 0, stream.get()),
        ("Cannot run kernel " + std::string(expression.kernel)).c_str());
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
