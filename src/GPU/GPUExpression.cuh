#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cstddef>
#include <cstdint>

namespace DB::GPU
{

enum class GPUExpressionResult : int
{
    Bitmask = 0,
    Float64 = 1,
};

struct GPUExpression
{
    cudaKernel_t kernel = nullptr;
    GPUExpressionResult result = GPUExpressionResult::Bitmask;
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
