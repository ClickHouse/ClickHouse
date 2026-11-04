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

/// A predicate or an expression over the columns of a row that `GPUExpressionCompiler` compiled into a kernel of the
/// device, with the code that ClickHouse compiles the functions into for the CPU, and loaded: a thread for each row. The
/// kernel takes a pointer to each column, a pointer to the result and the number of rows. Of a predicate, it sets a bit of
/// the result for each row that the predicate holds for, a word of 32 bits for 32 rows, as a null mask of cuDF does; of
/// an expression, it writes a `Float64` for each row. The compiler keeps the kernels for as long as the process runs.
struct GPUExpression
{
    cudaKernel_t kernel = nullptr;
    /// Of the threads of a block that keep the most threads of the device's multiprocessors busy, a whole number of warps.
    uint32_t block_size = 0;
    /// For the messages of errors.
    const char * name = nullptr;
    GPUElementType column_types[max_expression_columns] = {};
    uint32_t num_columns = 0;
};

/// A value column of a `GROUP BY` that the device computes from other value columns by `expression`, into a `Float64`:
/// the `columns[i]`-th value column is what it loads as its column `i`.
struct GPUDerivedValue
{
    GPUExpression expression;
    uint32_t columns[max_expression_columns];
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

/// Queues the evaluation of the predicate `expression` over `rows` rows of `inputs` into `mask`, a bit for a row that it
/// holds for, a word of 32 bits for 32 rows, as a null mask of cuDF.
__host__ void evaluatePredicateToBitmask(
    const GPUExpression & expression, const GPUExpressionInputs & inputs, size_t rows, uint32_t * mask, cuda::stream_ref stream);

/// Queues the computation of `derived` over the first `rows` rows of the value columns `values` into `out`, `rows`
/// doubles on the device.
__host__ void evaluateDerivedValue(
    const GPUDerivedValue & derived, GPUSpan<DeviceFixedColumn> values, size_t rows, double * out, cuda::stream_ref stream);

}
