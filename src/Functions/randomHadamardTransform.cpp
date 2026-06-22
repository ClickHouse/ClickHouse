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
#include <cstdlib>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

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
  *
  * Implementation notes:
  * - The +-1 sign multiply (D) is applied as a sign-bit XOR of the IEEE float, not a multiply
  *   (x ^ sign_bit == x * -1 bit-for-bit); the Hadamard butterflies (H) are pure add/sub. So the
  *   only multiply left is the final 1/sqrt(k) scale, one per kept coordinate.
  * - The core transform is a swappable kernel: a portable scalar FWHT and a NEON + ILP kernel for
  *   float32 on AArch64. Both perform exactly the same butterflies in the same stage order, so
  *   they are bit-for-bit identical; the kernel can be selected for testing with the environment
  *   variable CLICKHOUSE_RHT_KERNEL = "scalar" | "neon" (default: neon on AArch64, scalar elsewhere).
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

/// The unsigned integer of the same width as the compute type, used to flip its sign bit.
template <typename Compute>
using SignMask = std::conditional_t<sizeof(Compute) == 4, UInt32, UInt64>;

/// Deterministic +-1 signs of length m derived from the seed (a splitmix64 stream), stored as a
/// sign-bit mask per element (the compute type's high bit for a negative sign, 0 otherwise) so the
/// sign can be applied as an XOR instead of a multiply. Cached per length m within one function
/// call (the seed is constant for the call). Distinct lengths are rare (usually one), so a linear
/// lookup is cheaper than a hash map.
template <typename Compute>
const PaddedPODArray<SignMask<Compute>> & getSignMasks(
    std::vector<std::pair<size_t, PaddedPODArray<SignMask<Compute>>>> & cache, UInt64 seed, size_t m)
{
    using Mask = SignMask<Compute>;
    for (auto & entry : cache)
        if (entry.first == m)
            return entry.second;

    static constexpr Mask sign_bit = Mask(1) << (sizeof(Mask) * 8 - 1);
    PaddedPODArray<Mask> masks(m);
    UInt64 state = seed;
    for (size_t i = 0; i < m; ++i)
        masks[i] = (splitmix64Next(state) >> 63) ? sign_bit : Mask(0);
    cache.emplace_back(m, std::move(masks));
    return cache.back().second;
}

/// Apply a +-1 sign as a sign-bit flip: bit-identical to multiplying by +1 or -1 (including +-0).
template <typename Compute>
inline Compute applySign(Compute x, SignMask<Compute> mask)
{
    using Mask = SignMask<Compute>;
    return std::bit_cast<Compute>(static_cast<Mask>(std::bit_cast<Mask>(x) ^ mask));
}

/// In-place unnormalized fast Walsh-Hadamard transform on m (a power of two) elements.
template <typename T>
void fwhtScalar(T * a, size_t m)
{
    for (size_t h = 1; h < m; h <<= 1)
        for (size_t i = 0; i < m; i += (h << 1))
            for (size_t j = i; j < i + h; ++j)
            {
                const T x = a[j];
                const T y = a[j + h];
                a[j] = x + y;
                a[j + h] = x - y;
            }
}

#if defined(__aarch64__)

/// 4-point Walsh-Hadamard transform (stages h = 1 then h = 2) within one float32x4 lane group.
/// The add/sub operands and their order match fwhtScalar exactly, so the result is bit-identical.
inline float32x4_t fourPointWHT(float32x4_t v)
{
    const float32x2_t lo = vget_low_f32(v);    /// [x0, x1]
    const float32x2_t hi = vget_high_f32(v);   /// [x2, x3]
    const float32x2_t ev = vuzp1_f32(lo, hi);  /// [x0, x2]
    const float32x2_t od = vuzp2_f32(lo, hi);  /// [x1, x3]
    const float32x2_t s1 = vadd_f32(ev, od);   /// [x0+x1, x2+x3]
    const float32x2_t d1 = vsub_f32(ev, od);   /// [x0-x1, x2-x3]
    const float32x2_t low2 = vzip1_f32(s1, d1);   /// [x0+x1, x0-x1]
    const float32x2_t high2 = vzip2_f32(s1, d1);  /// [x2+x3, x2-x3]
    return vcombine_f32(vadd_f32(low2, high2), vsub_f32(low2, high2));
}

/// In-place unnormalized FWHT on m (a power of two) float32 values using NEON + ILP. Performs the
/// same butterflies in the same stage order as fwhtScalar, so the result is bit-for-bit identical.
void fwhtNeon(float * a, size_t m)
{
    /// Block of 8 NEON vectors (32 floats): stages h = 1 .. 16 are done entirely in registers
    /// (they never cross a 32-element block), turning 5 memory passes into one.
    constexpr size_t vectors_per_block = 16;
    constexpr size_t block = vectors_per_block * 4;
    if (m < block)
    {
        fwhtScalar(a, m);
        return;
    }

    for (size_t i = 0; i < m; i += block)
    {
        float32x4_t v[vectors_per_block];
        for (size_t t = 0; t < vectors_per_block; ++t)
            v[t] = fourPointWHT(vld1q_f32(a + i + 4 * t));  /// stages h = 1, 2

        /// Vector-level stages h = 4, 8, 16 (a stride of hv vectors == 4*hv elements).
        for (size_t hv = 1; hv < vectors_per_block; hv <<= 1)
            for (size_t base = 0; base < vectors_per_block; base += (hv << 1))
                for (size_t t = 0; t < hv; ++t)
                {
                    const float32x4_t x = v[base + t];
                    const float32x4_t y = v[base + t + hv];
                    v[base + t] = vaddq_f32(x, y);
                    v[base + t + hv] = vsubq_f32(x, y);
                }

        for (size_t t = 0; t < vectors_per_block; ++t)
            vst1q_f32(a + i + 4 * t, v[t]);
    }

    /// High stages h = block, 2*block, ..., m/2: contiguous SIMD butterflies. Each h is a multiple
    /// of 8, so the inner loop processes two vectors per step for instruction-level parallelism.
    for (size_t h = block; h < m; h <<= 1)
        for (size_t i = 0; i < m; i += (h << 1))
            for (size_t j = i; j < i + h; j += 8)
            {
                const float32x4_t x0 = vld1q_f32(a + j);
                const float32x4_t x1 = vld1q_f32(a + j + 4);
                const float32x4_t y0 = vld1q_f32(a + j + h);
                const float32x4_t y1 = vld1q_f32(a + j + h + 4);
                vst1q_f32(a + j,         vaddq_f32(x0, y0));
                vst1q_f32(a + j + 4,     vaddq_f32(x1, y1));
                vst1q_f32(a + j + h,     vsubq_f32(x0, y0));
                vst1q_f32(a + j + h + 4, vsubq_f32(x1, y1));
            }
}

enum class FwhtKernel
{
    Scalar,
    Neon,
};

/// Read the kernel choice once from CLICKHOUSE_RHT_KERNEL (default: NEON on AArch64).
inline FwhtKernel selectKernel()
{
    static const FwhtKernel kernel = []
    {
        if (const char * env = std::getenv("CLICKHOUSE_RHT_KERNEL"))  /// NOLINT(concurrency-mt-unsafe)
        {
            if (std::string_view(env) == "scalar")
                return FwhtKernel::Scalar;
            if (std::string_view(env) == "neon")
                return FwhtKernel::Neon;
        }
        return FwhtKernel::Neon;
    }();
    return kernel;
}

#endif

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
        using Mask = SignMask<Compute>;

        const auto & input = assert_cast<const ColumnVector<In> &>(nested).getData();

        auto result_column = ColumnVector<Out>::create();
        auto result_offsets_column = ColumnArray::ColumnOffsets::create();
        auto & result = result_column->getData();
        auto & result_offsets = result_offsets_column->getData();
        result_offsets.resize(rows);

#if defined(__aarch64__)
        [[maybe_unused]] const FwhtKernel kernel = selectKernel();
#endif

        std::vector<std::pair<size_t, PaddedPODArray<Mask>>> sign_cache;
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

                const PaddedPODArray<Mask> & signs = getSignMasks<Compute>(sign_cache, seed, m);
                const Compute scale = static_cast<Compute>(1.0 / std::sqrt(static_cast<double>(k)));
                const In * in = input.data() + start;
                result.resize(written + k);
                Out * out = result.data() + written;

                /// Populate the working buffer: cast to the compute type and apply the +-1 sign as a
                /// sign-bit flip (no multiply); zero-pad the tail to the padded length m.
                buffer.resize(m);
                for (size_t i = 0; i < length; ++i)
                    buffer[i] = applySign<Compute>(static_cast<Compute>(in[i]), signs[i]);
                for (size_t i = length; i < m; ++i)
                    buffer[i] = 0;

                /// Core (unnormalized) fast Walsh-Hadamard transform via the selected kernel.
#if defined(__aarch64__)
                if constexpr (std::is_same_v<Compute, float>)
                {
                    if (kernel == FwhtKernel::Neon)
                        fwhtNeon(buffer.data(), m);
                    else
                        fwhtScalar(buffer.data(), m);
                }
                else
                    fwhtScalar(buffer.data(), m);
#else
                fwhtScalar(buffer.data(), m);
#endif

                /// Scale by 1/sqrt(k) (the only multiply) and cast to the output element type.
                for (size_t i = 0; i < k; ++i)
                    out[i] = static_cast<Out>(scale * buffer[i]);

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
