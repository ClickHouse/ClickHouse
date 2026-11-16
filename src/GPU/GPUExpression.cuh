#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cstddef>
#include <cstdint>

namespace DB::GPU
{

enum class GPUExpressionResult : int
{
    // return the bitmask of the expression result, where each bit represents the result of the expression for a row
    Bitmask = 0,
    // return the value of the expression result for each row, where each value is stored in the result buffer
    // for example, if the expression is "column1 + column2", the result buffer will contain the sum of column1 and column2 for each row
    Value = 1,
};

struct GPUExpression
{
    cudaKernel_t kernel = nullptr;
    GPUExpressionResult result = GPUExpressionResult::Bitmask;
    GPUElementType value_type = GPUElementType::UInt8;
    uint32_t block_size = 0;
    const char * name = nullptr;
    const GPUElementType * column_types = nullptr;
    uint32_t num_columns = 0;
};

struct GPUExpressionInput
{
    const char * data = nullptr;
    GPUElementType type = GPUElementType::UInt8;
};

struct GPUExpressionInputs
{
    const GPUExpressionInput * columns = nullptr;
    uint32_t count = 0;
};

__host__ void evaluateGPUExpression(
    const GPUExpression & expression, const GPUExpressionInputs & inputs, size_t rows, void * result, cuda::stream_ref stream);

}
