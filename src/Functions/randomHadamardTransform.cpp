#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/FunctionDocumentation.h>
#include <Common/HadamardTransform.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>

#include <bit>
#include <cmath>
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
  *
  * Implementation notes:
  * - The +-1 sign multiply (D) is applied as a sign-bit XOR of the IEEE float, not a multiply
  *   (x ^ sign_bit == x * -1 bit-for-bit); the Hadamard butterflies (H) are pure add/sub. So the
  *   only multiply left is the final 1/sqrt(k) scale, one per kept coordinate.
  * - The core transform is a swappable kernel: a portable scalar FWHT and a NEON + ILP kernel for
  *   float32 on AArch64. Both perform exactly the same butterflies in the same stage order, so
  *   they are bit-for-bit identical; the kernel can be selected for testing with the environment
  *   variable CLICKHOUSE_RHT_KERNEL = "scalar" | "neon" (default: neon on AArch64, scalar elsewhere).
  * - When the length is 2^k * m with m in {12, 20} (orders with a Hadamard matrix), the transform
  *   is the exact Kronecker product H_(2^k) (x) H_m applied without padding, so the output keeps the
  *   input dimension. H_m is built once via the Paley construction. See hadamardOrderFor / kronecker*.
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

using namespace HadamardTransform;

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
            /// Reject a non-positive constant up front (validated independently of the input data):
            /// a negative signed value would otherwise wrap to a huge UInt64 and only be caught per
            /// row via the `k > working_dim` check below, which is skipped for empty arrays.
            if (WhichDataType(arguments[2].type).isNativeInt() && arguments[2].column->getInt(0) <= 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The 'output_dims' argument of function {} must be positive", getName());
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

        /// Single-entry caches, regenerated only when the dimension changes (it is usually constant
        /// across a column). The seed is constant for the whole call, so the signs depend only on
        /// the working dimension.
        PaddedPODArray<Mask> sign_masks;
        size_t sign_dim = 0;
        HmMasks<Compute> hm;
        PaddedPODArray<Compute> buffer;
        size_t written = 0;
        size_t start = 0;

        for (size_t row = 0; row < rows; ++row)
        {
            const size_t length = offsets[row] - start;
            if (length != 0)
            {
                /// Exact Kronecker transform for length = 2^k * m (m in {12, 20}); otherwise zero-pad
                /// to the next power of two. The working dimension is the input length in the exact
                /// case and the padded length otherwise.
                const auto [kron_m, kron_blocks] = hadamardOrderFor(length);
                const size_t working_dim = kron_m ? length : std::bit_ceil(length);
                const size_t k = fixed_out_dims ? fixed_out_dims : working_dim;
                if (k > working_dim)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "output_dims ({}) of function {} exceeds the transform length {}",
                        k, name, working_dim);

                if (sign_dim != working_dim)
                {
                    generateSignMasks<Compute>(sign_masks, seed, working_dim);
                    sign_dim = working_dim;
                }
                const PaddedPODArray<Mask> & signs = sign_masks;
                const Compute scale = static_cast<Compute>(1.0 / std::sqrt(static_cast<double>(k)));
                const In * in = input.data() + start;
                result.resize(written + k);
                Out * out = result.data() + written;

                /// Populate the working buffer: cast to the compute type and apply the +-1 sign as a
                /// sign-bit flip (no multiply); zero-pad the tail to the working dimension.
                buffer.resize(working_dim);
                for (size_t i = 0; i < length; ++i)
                    buffer[i] = applySign<Compute>(static_cast<Compute>(in[i]), signs[i]);
                for (size_t i = length; i < working_dim; ++i)
                    buffer[i] = 0;

                /// Core transform via the selected kernel: an exact Kronecker product for the
                /// supported non-power-of-two dimensions, or a fast Walsh-Hadamard transform otherwise.
                if (kron_m)
                {
                    if (hm.order != kron_m)
                        buildHmMasks<Compute>(hm, kron_m);
#if defined(__aarch64__)
                    if constexpr (std::is_same_v<Compute, float>)
                    {
                        if (kernel == FwhtKernel::Neon)
                            kroneckerNeon(buffer.data(), kron_blocks, kron_m, hm);
                        else
                            kroneckerScalar<Compute>(buffer.data(), kron_blocks, kron_m, hm);
                    }
                    else
                        kroneckerScalar<Compute>(buffer.data(), kron_blocks, kron_m, hm);
#else
                    kroneckerScalar<Compute>(buffer.data(), kron_blocks, kron_m, hm);
#endif
                }
                else
                {
#if defined(__aarch64__)
                    if constexpr (std::is_same_v<Compute, float>)
                    {
                        if (kernel == FwhtKernel::Neon)
                            fwhtNeon(buffer.data(), working_dim);
                        else
                            fwhtScalar(buffer.data(), working_dim);
                    }
                    else
                        fwhtScalar(buffer.data(), working_dim);
#else
                    fwhtScalar(buffer.data(), working_dim);
#endif
                }

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
    FunctionDocumentation::Description description = R"DOCS_MD(
Applies a randomized Hadamard transform to a float vector: `y = (1/sqrt(k)) * (H * D * x)`, where
`D` is a diagonal matrix of deterministic +/-1 signs chosen by `seed`, `H` is the Walsh-Hadamard
matrix, and the input is zero-padded to `m`, the next power of two of its length.

The transform is an orthogonal, **norm-preserving** rotation that spreads a vector's energy evenly
across coordinates, making the per-coordinate distribution approximately Gaussian and
data-independent. It is useful as a preprocessing step before scalar quantization, and -- when
truncated -- as a Johnson-Lindenstrauss / subsampled-randomized-Hadamard (SRHT) random projection.

When the input length is `2^k * m` with `m` in `{12, 20}` (orders that have a Hadamard matrix, e.g.
`768 = 64 * 12`, `1536 = 128 * 12`, `3072 = 256 * 12`, `2560 = 128 * 20`), an exact Kronecker
transform `H_(2^k) (x) H_m` is used instead of padding, so the output keeps the input dimension
rather than growing to the next power of two. All other lengths are zero-padded to `m`, the next
power of two.

The result has the same element type as the input; an empty input array returns an empty array.

- `seed` (optional, default `0`): selects the sign pattern; the same seed always yields the same
  transform.
- `output_dims` (optional, default the transform length): keeps only the first `output_dims`
  coordinates. The `1/sqrt(output_dims)` scaling keeps the result norm-preserving for the full
  transform and norm-preserving in expectation when truncated. It must not exceed the transform
  length (the next power of two, or the input length for the exact Kronecker case).
)DOCS_MD";
    FunctionDocumentation::Syntax syntax = "randomHadamardTransform(vector[, seed[, output_dims]])";
    FunctionDocumentation::Arguments arguments = {
        {"vector", "Vector to transform.", {"Array(BFloat16)", "Array(Float32)", "Array(Float64)"}},
        {"seed", "Optional. Seed for the deterministic +/-1 signs (default 0).", {"UInt*"}},
        {"output_dims", "Optional. Truncate the result to this many leading coordinates (default: the full transform length, which is the input length for exact Kronecker dimensions and otherwise the next power of two).", {"UInt*"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"The transformed vector (same element type as the input). Its length is the transform length: the input length for exact Kronecker dimensions, otherwise the input padded to the next power of two; optionally truncated to output_dims.", {"Array(BFloat16)", "Array(Float32)", "Array(Float64)"}};
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
