#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cstddef>
#include <cstdint>

/// The expressions that the device evaluates over the rows of columns, which `GPUExpressionCompiler` compiles into
/// kernels, and what runs them.
namespace DB::GPU
{

constexpr size_t max_expression_columns = 16;

/// What the kernel of a `GPUExpression` writes for the rows it runs over.
enum class GPUExpressionResult : int
{
    /// Of a predicate: a bit for each row that it holds for, a word of 32 bits for 32 rows, as a null mask of cuDF.
    Bitmask = 0,
    /// Of an expression into a `Float64`: its value for each row.
    Float64 = 1,
};

/// A predicate or an expression over the columns of a row that `GPUExpressionCompiler` compiled into a kernel of the
/// device, with the code that ClickHouse compiles the functions into for the CPU, and loaded: a thread for each row. It
/// is a view of the kernel, which `CompiledGPUExpression::kernel` keeps loaded.
struct GPUExpression
{
    cudaKernel_t kernel = nullptr;
    GPUExpressionResult result = GPUExpressionResult::Bitmask;
    /// Of the threads of a block that keep the most threads of the device's multiprocessors busy, a whole number of warps.
    uint32_t block_size = 0;
    /// For the messages of errors.
    const char * name = nullptr;
    GPUElementType column_types[max_expression_columns] = {};
    uint32_t num_columns = 0;
};

/// A column that an expression reads, in the memory of the device.
struct GPUExpressionInput
{
    const char * data = nullptr;
    GPUElementType type = GPUElementType::UInt8;
};

/// The columns that an expression reads: `columns[i]` is its column `i`, of the type it was compiled for.
struct GPUExpressionInputs
{
    GPUExpressionInput columns[max_expression_columns];
    uint32_t count = 0;
};

/// Queues the evaluation of `expression` over `rows` rows of `inputs` into `result`, in the memory of the device, of what
/// `expression.result` says: the words of a bit mask of `rows` bits, or `rows` doubles.
__host__ void evaluateGPUExpression(
    const GPUExpression & expression, const GPUExpressionInputs & inputs, size_t rows, void * result, cuda::stream_ref stream);

}
