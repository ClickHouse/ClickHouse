#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/FunctionDocumentation.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>

#include <bit>
#include <cmath>
#include <utility>
#include <vector>

/** randomHadamardTransform(vector [, seed] [, output_dims])
  *
  * Applies a randomized Hadamard transform to a float vector:
  *     y = (1 / sqrt(k)) * (H * D * x_padded)[0 : k]
  * where D is a diagonal matrix of deterministic +-1 signs derived from `seed`, H is the
  * (unnormalized) Walsh-Hadamard matrix, and the input is zero-padded to m = the next power of
  * two of its length. This is an orthogonal, norm-preserving "rotation" that also spreads a
  * vector's energy across all coordinates, making the per-coordinate distribution approximately
  * Gaussian and data-independent -- useful as a preprocessing step before scalar quantization,
  * and (with truncation) as a Johnson-Lindenstrauss / subsampled-randomized-Hadamard projection.
  *
  * - seed (optional, default 0): selects the deterministic sign pattern; the same seed always
  *   produces the same transform.
  * - output_dims (optional, default m): keep only the first `output_dims` coordinates. The
  *   1/sqrt(output_dims) scaling makes the result norm-preserving for the full transform and
  *   norm-preserving in expectation when truncated. Must not exceed m.
  *
  * The result has the same element type as the input. An empty input array yields an empty array.
  */
namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

inline UInt64 splitmix64Next(UInt64 & state)
{
    state += 0x9E3779B97F4A7C15ULL;
    UInt64 z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/// Deterministic +-1 signs of length m derived from the seed (a splitmix64 stream).
/// Cached per length m within one function call (the seed is constant for the call). Distinct
/// lengths are rare (usually one), so a linear lookup is cheaper than a hash map.
template <typename T>
const PaddedPODArray<T> & getSigns(std::vector<std::pair<size_t, PaddedPODArray<T>>> & cache, UInt64 seed, size_t m)
{
    for (auto & entry : cache)
        if (entry.first == m)
            return entry.second;

    PaddedPODArray<T> signs(m);
    UInt64 state = seed;
    for (size_t i = 0; i < m; ++i)
        signs[i] = (splitmix64Next(state) >> 63) ? static_cast<T>(-1) : static_cast<T>(1);
    cache.emplace_back(m, std::move(signs));
    return cache.back().second;
}

class FunctionRandomHadamardTransform : public IFunction
{
public:
    static constexpr auto name = "randomHadamardTransform";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRandomHadamardTransform>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 1 to 3 arguments: vector [, seed] [, output_dims]", getName());

        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be an Array of floats, got {}", getName(), arguments[0].type->getName());

        WhichDataType which_nested(array_type->getNestedType());
        if (!which_nested.isFloat32() && !which_nested.isFloat64() && !which_nested.isBFloat16())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be Array(BFloat16|Float32|Float64), got Array({})",
                getName(), array_type->getNestedType()->getName());

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            WhichDataType which(arguments[i].type);
            if (!which.isNativeUInt() && !which.isNativeInt())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The {} argument of function {} must be an integer, got {}",
                    i == 1 ? "'seed'" : "'output_dims'", getName(), arguments[i].type->getName());
        }

        /// The result has the same element type as the input.
        return std::make_shared<DataTypeArray>(array_type->getNestedType());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        UInt64 seed = 0;
        size_t fixed_out_dims = 0; /// 0 means "full" (= m)
        if (arguments.size() >= 2)
        {
            if (!isColumnConst(*arguments[1].column))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The 'seed' argument of function {} must be a constant", getName());
            seed = arguments[1].column->getUInt(0);
        }
        if (arguments.size() >= 3)
        {
            if (!isColumnConst(*arguments[2].column))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The 'output_dims' argument of function {} must be a constant", getName());
            fixed_out_dims = arguments[2].column->getUInt(0);
            if (fixed_out_dims == 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The 'output_dims' argument of function {} must be positive", getName());
        }

        ColumnPtr arg0 = arguments[0].column->convertToFullColumnIfConst();
        const auto * col_array = checkAndGetColumn<ColumnArray>(arg0.get());
        if (!col_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be an Array column", getName());

        const IColumn & nested = col_array->getData();
        const ColumnArray::Offsets & offsets = col_array->getOffsets();
        WhichDataType which_nested(checkAndGetDataType<DataTypeArray>(arguments[0].type.get())->getNestedType());

        /// Compute in Float32 for Float32/BFloat16 inputs and in Float64 for Float64 inputs;
        /// the output keeps the input's element type.
        if (which_nested.isFloat64())
            return run<Float64, Float64, Float64>(nested, offsets, input_rows_count, seed, fixed_out_dims);
        if (which_nested.isFloat32())
            return run<Float32, Float32, Float32>(nested, offsets, input_rows_count, seed, fixed_out_dims);
        return run<BFloat16, Float32, BFloat16>(nested, offsets, input_rows_count, seed, fixed_out_dims);
    }

private:
    template <typename In, typename Compute, typename Out>
    static ColumnPtr run(
        const IColumn & nested, const ColumnArray::Offsets & offsets, size_t rows, UInt64 seed, size_t fixed_out_dims)
    {
        const auto & input = assert_cast<const ColumnVector<In> &>(nested).getData();

        auto result_column = ColumnVector<Out>::create();
        auto result_offsets_column = ColumnArray::ColumnOffsets::create();
        auto & result = result_column->getData();
        auto & result_offsets = result_offsets_column->getData();
        result_offsets.resize(rows);

        std::vector<std::pair<size_t, PaddedPODArray<Compute>>> sign_cache;
        PaddedPODArray<Compute> buffer;
        size_t written = 0;
        size_t start = 0;

        for (size_t row = 0; row < rows; ++row)
        {
            const size_t length = offsets[row] - start;
            if (length != 0)
            {
                const size_t m = std::bit_ceil(length);
                const size_t k = fixed_out_dims ? fixed_out_dims : m;
                if (k > m)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "output_dims ({}) of function {} exceeds the padded length {} (next power of two of {})",
                        k, name, m, length);

                const PaddedPODArray<Compute> & signs = getSigns(sign_cache, seed, m);
                const Compute scale = static_cast<Compute>(1.0 / std::sqrt(static_cast<double>(k)));
                const In * in = input.data() + start;
                result.resize(written + k);
                Out * out = result.data() + written;

                if (m == 1)
                {
                    /// A 1-element transform is the identity (times the sign and scale).
                    out[0] = static_cast<Out>(scale * static_cast<Compute>(in[0]) * signs[0]);
                }
                else
                {
                    /// First Hadamard stage (h = 1) fused with the input load: cast, apply the sign,
                    /// zero-pad past the input length, and write the h = 1 butterfly into the buffer.
                    buffer.resize(m);
                    /// Pairs fully inside the input: no padding branch in the hot loop.
                    const size_t even_length = length & ~static_cast<size_t>(1);
                    for (size_t j = 0; j < even_length; j += 2)
                    {
                        const Compute x = static_cast<Compute>(in[j])     * signs[j];
                        const Compute y = static_cast<Compute>(in[j + 1]) * signs[j + 1];
                        buffer[j]     = x + y;
                        buffer[j + 1] = x - y;
                    }
                    /// A straddling pair (one real element, one padding zero) when length is odd.
                    if (even_length < length)
                    {
                        const Compute x = static_cast<Compute>(in[even_length]) * signs[even_length];
                        buffer[even_length]     = x;
                        buffer[even_length + 1] = x;
                    }
                    /// Remaining pairs are pure padding; the h = 1 butterfly of (0, 0) is (0, 0).
                    for (size_t j = (length + 1) & ~static_cast<size_t>(1); j < m; j += 2)
                    {
                        buffer[j]     = 0;
                        buffer[j + 1] = 0;
                    }

                    if (m == 2)
                    {
                        /// h = 1 was the only stage; scale and cast the kept outputs.
                        for (size_t i = 0; i < k; ++i)
                            out[i] = static_cast<Out>(scale * buffer[i]);
                    }
                    else
                    {
                        /// Middle stages h = 2 .. m/4 (in place on the buffer).
                        for (size_t h = 2; h < m / 2; h <<= 1)
                            for (size_t i = 0; i < m; i += (h << 1))
                                for (size_t j = i; j < i + h; ++j)
                                {
                                    const Compute a = buffer[j];
                                    const Compute b = buffer[j + h];
                                    buffer[j] = a + b;
                                    buffer[j + h] = a - b;
                                }

                        /// Last stage (h = m/2) fused with the scale and output cast: write the kept
                        /// coordinates straight to the result, skipping the separate buffer round-trip.
                        const size_t half = m / 2;
                        if (k == m)
                        {
                            /// Full transform (no truncation): branch-free, both halves are kept.
                            for (size_t j = 0; j < half; ++j)
                            {
                                const Compute a = buffer[j];
                                const Compute b = buffer[j + half];
                                out[j]        = static_cast<Out>(scale * (a + b));
                                out[j + half] = static_cast<Out>(scale * (a - b));
                            }
                        }
                        else
                        {
                            /// Truncated to the first k coordinates.
                            for (size_t j = 0; j < half; ++j)
                            {
                                const Compute a = buffer[j];
                                const Compute b = buffer[j + half];
                                if (j < k)
                                    out[j] = static_cast<Out>(scale * (a + b));
                                if (j + half < k)
                                    out[j + half] = static_cast<Out>(scale * (a - b));
                            }
                        }
                    }
                }
                written += k;
            }
            result_offsets[row] = written;
            start = offsets[row];
        }

        return ColumnArray::create(std::move(result_column), std::move(result_offsets_column));
    }
};

}

REGISTER_FUNCTION(RandomHadamardTransform)
{
    FunctionDocumentation::Description description = R"(
Applies a randomized Hadamard transform to a float vector: `y = (1/sqrt(k)) * (H * D * x)`, where
`D` is a diagonal matrix of deterministic +/-1 signs chosen by `seed`, `H` is the Walsh-Hadamard
matrix, and the input is zero-padded to `m`, the next power of two of its length.

The transform is an orthogonal, **norm-preserving** rotation that spreads a vector's energy evenly
across coordinates, making the per-coordinate distribution approximately Gaussian and
data-independent. It is useful as a preprocessing step before scalar quantization, and -- when
truncated -- as a Johnson-Lindenstrauss / subsampled-randomized-Hadamard (SRHT) random projection.

The result has the same element type as the input; an empty input array returns an empty array.

- `seed` (optional, default `0`): selects the sign pattern; the same seed always yields the same
  transform.
- `output_dims` (optional, default `m`): keeps only the first `output_dims` coordinates. The
  `1/sqrt(output_dims)` scaling keeps the result norm-preserving for the full transform and
  norm-preserving in expectation when truncated. It must not exceed `m`.
)";
    FunctionDocumentation::Syntax syntax = "randomHadamardTransform(vector[, seed[, output_dims]])";
    FunctionDocumentation::Arguments arguments = {
        {"vector", "Vector to transform.", {"Array(BFloat16)", "Array(Float32)", "Array(Float64)"}},
        {"seed", "Optional. Seed for the deterministic +/-1 signs (default 0).", {"UInt*"}},
        {"output_dims", "Optional. Truncate the result to this many leading coordinates (default: next power of two of the input length).", {"UInt*"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"The transformed vector (same element type as the input), zero-padded to the next power of two and optionally truncated.", {"Array(BFloat16)", "Array(Float32)", "Array(Float64)"}};
    FunctionDocumentation::Examples examples = {
        {"Full transform (length padded to a power of two)",
         "SELECT length(randomHadamardTransform([1, 2, 3]::Array(Float32)))", "4"},
        {"Norm is preserved",
         "SELECT round(arraySum(x -> x * x, randomHadamardTransform([1, 2, 3, 4]::Array(Float32))) - 30, 4)", "0"},
        {"Truncated projection to 3 dimensions",
         "SELECT length(randomHadamardTransform([1, 2, 3, 4, 5, 6, 7, 8]::Array(Float32), 42, 3))", "3"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRandomHadamardTransform>(documentation);
}

}
