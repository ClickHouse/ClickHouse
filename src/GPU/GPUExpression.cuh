#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cstddef>
#include <cstdint>

/// The expressions that the device evaluates over the rows of columns, which `GPUExpressionCompiler` compiles into
/// kernels, and what runs them. The PTX of a kernel is loaded once, by the first query that runs it.
namespace DB::GPU
{

constexpr size_t max_expression_columns = 16;

/// A predicate or an expression over the columns of a row that `GPUExpressionCompiler` compiles into PTX, with the code
/// that ClickHouse compiles the functions into for the CPU, and the device runs a thread for each row of. It is a view
/// of what the compiler keeps for as long as the process runs, see `GPUExpressionCompiler.h`.
struct GPUExpression
{
    const char * ptx = nullptr;
    /// The kernel of `ptx`, which takes a pointer to each column, a pointer to the result and the number of rows: of a
    /// predicate, it sets a bit of the result for each row that the predicate holds for, a word of 32 bits for 32 rows,
    /// as a null mask of cuDF does; of an expression, it writes a `Float64` for each row.
    const char * kernel = nullptr;
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

/// Queues the evaluation of the predicate `expression` over `rows` rows of `inputs` into `mask`, a bit for a row that it
/// holds for, a word of 32 bits for 32 rows, as a null mask of cuDF.
__host__ void evaluatePredicateToBitmask(
    const GPUExpression & expression, const GPUExpressionInputs & inputs, size_t rows, uint32_t * mask, cuda::stream_ref stream);

/// Queues the computation of the `Float64` expression `expression` over `rows` rows of `inputs` into `out`, `rows`
/// doubles on the device.
__host__ void evaluateExpressionToFloat64(
    const GPUExpression & expression, const GPUExpressionInputs & inputs, size_t rows, double * out, cuda::stream_ref stream);

}
