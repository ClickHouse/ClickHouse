#include <GPU/GPUExpression.cuh>

#include <GPU/GPUFilter.cuh>

#include <string>

namespace DB::GPU
{

namespace
{

constexpr int threads_per_block = 256;

__global__ void evaluateDerivedRows(GPUFilterProgram program, Filtering::FilterLayouts inputs, size_t rows, double * out)
{
    const size_t row = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
    if (row < rows)
        out[row] = Filtering::asDouble(Filtering::evaluateProgram(program, inputs, row));
}

}

__host__ void evaluateDerivedValue(
    const GPUDerivedValue & derived, GPUSpan<DeviceFixedColumn> values, size_t rows, double * out, cuda::stream_ref stream)
{
    if (rows == 0)
        return;

    if (derived.num_columns > max_filter_columns)
        throwGPUError(("a derived value of " + std::to_string(derived.num_columns) + " columns").c_str());

    Filtering::FilterLayouts inputs;
    inputs.present = true;
    inputs.count = derived.num_columns;
    for (uint32_t i = 0; i < derived.num_columns; ++i)
    {
        const uint32_t column = derived.columns[i];
        if (column >= values.size())
            throwGPUError(("a derived value of value column " + std::to_string(column) + " of " + std::to_string(values.size())).c_str());
        if (values[column].rows < rows)
            throwGPUError(("a derived value of " + std::to_string(rows) + " rows over a column of " + std::to_string(values[column].rows)).c_str());
        inputs.columns[i] = {.data = values[column].data, .type = values[column].element_type};
    }

    const auto blocks = static_cast<unsigned>((rows + threads_per_block - 1) / threads_per_block);
    evaluateDerivedRows<<<blocks, threads_per_block, 0, stream.get()>>>(derived.program, inputs, rows, out);
    checkCuda(cudaGetLastError(), "Cannot compute a derived value on the device");
}

}
