#include <Functions/array/arrayRandomProjection.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/FunctionDocumentation.h>

#include <mutex>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

struct NormalMethodTag
{
    static constexpr auto name = "randomProjectionNormal";
};
struct OrthogonalMethodTag
{
    static constexpr auto name = "randomProjectionOrthogonal";
};
struct SparseMethodTag
{
    static constexpr auto name = "randomProjectionSparse";
};
struct HadamardMethodTag
{
    static constexpr auto name = "randomProjectionHadamard";
};

/// Traits: maps MethodTag to the corresponding ProjectionState types.
template <typename MethodTag>
struct ProjectionStateTypes;

template <>
struct ProjectionStateTypes<NormalMethodTag>
{
    template <typename T>
    using State = NormalProjectionState<T>;
};
template <>
struct ProjectionStateTypes<OrthogonalMethodTag>
{
    template <typename T>
    using State = OrthogonalProjectionState<T>;
};
template <>
struct ProjectionStateTypes<SparseMethodTag>
{
    template <typename T>
    using State = SparseProjectionState<T>;
};
template <>
struct ProjectionStateTypes<HadamardMethodTag>
{
    template <typename T>
    using State = HadamardProjectionState<T>;
};

/// Whether the method uses batched dense GEMM (Normal, Orthogonal) vs per-row apply (Sparse, Hadamard).
template <typename MethodTag>
struct UsesDenseGEMM : std::true_type
{
};
template <>
struct UsesDenseGEMM<SparseMethodTag> : std::false_type
{
};
template <>
struct UsesDenseGEMM<HadamardMethodTag> : std::false_type
{
};

/// Per-row projection application for non-GEMM methods.
/// `scratch` is a pre-allocated buffer (used by Hadamard, ignored by Sparse).
template <typename T, typename MethodTag>
void applyPerRow(
    const T * input, T * output, const typename ProjectionStateTypes<MethodTag>::template State<T> & state, T * scratch);

template <>
void applyPerRow<Float32, SparseMethodTag>(const Float32 * input, Float32 * output, const SparseProjectionState<Float32> & state, Float32 *)
{
    applySparseProjection<Float32>(input, output, state);
}
template <>
void applyPerRow<Float64, SparseMethodTag>(const Float64 * input, Float64 * output, const SparseProjectionState<Float64> & state, Float64 *)
{
    applySparseProjection<Float64>(input, output, state);
}
template <>
void applyPerRow<Float32, HadamardMethodTag>(
    const Float32 * input, Float32 * output, const HadamardProjectionState<Float32> & state, Float32 * scratch)
{
    applyHadamardProjection<Float32>(input, output, state, scratch);
}
template <>
void applyPerRow<Float64, HadamardMethodTag>(
    const Float64 * input, Float64 * output, const HadamardProjectionState<Float64> & state, Float64 * scratch)
{
    applyHadamardProjection<Float64>(input, output, state, scratch);
}

template <typename MethodTag>
class RandomProjectionExecutable;

template <typename MethodTag>
class RandomProjectionFunctionBase : public IFunctionBase
{
public:
    RandomProjectionFunctionBase(size_t target_dim_, UInt64 seed_, DataTypes argument_types_, DataTypePtr result_type_)
        : target_dim(target_dim_)
        , seed(seed_)
        , argument_types(std::move(argument_types_))
        , result_type(std::move(result_type_))
    {
    }

    String getName() const override { return MethodTag::name; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return result_type; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool isStateful() const override { return true; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<RandomProjectionExecutable<MethodTag>>(*this);
    }

private:
    friend class RandomProjectionExecutable<MethodTag>;

    using StateF32 = typename ProjectionStateTypes<MethodTag>::template State<Float32>;
    using StateF64 = typename ProjectionStateTypes<MethodTag>::template State<Float64>;

    size_t target_dim;
    UInt64 seed;
    DataTypes argument_types;
    DataTypePtr result_type;

    mutable std::once_flag init_flag_f32;
    mutable std::once_flag init_flag_f64;
    mutable StateF32 state_f32;
    mutable StateF64 state_f64;

    template <typename T>
    void ensureInitialized(size_t input_dim) const
    {
        if constexpr (std::is_same_v<T, Float32>)
        {
            std::call_once(init_flag_f32, [&]() { state_f32.generate(input_dim, target_dim, seed); });
            if (state_f32.input_dim != input_dim)
                throw Exception(
                    ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "All input arrays for {} must have the same dimension (expected {}, got {})",
                    MethodTag::name,
                    state_f32.input_dim,
                    input_dim);
        }
        else
        {
            std::call_once(init_flag_f64, [&]() { state_f64.generate(input_dim, target_dim, seed); });
            if (state_f64.input_dim != input_dim)
                throw Exception(
                    ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "All input arrays for {} must have the same dimension (expected {}, got {})",
                    MethodTag::name,
                    state_f64.input_dim,
                    input_dim);
        }
    }

    template <typename T>
    const auto & getState() const
    {
        if constexpr (std::is_same_v<T, Float32>)
            return state_f32;
        else
            return state_f64;
    }
};

template <typename MethodTag>
class RandomProjectionExecutable : public IExecutableFunction
{
public:
    explicit RandomProjectionExecutable(const RandomProjectionFunctionBase<MethodTag> & base_)
        : base(base_)
    {
    }

    String getName() const override { return MethodTag::name; }
    bool useDefaultImplementationForConstants() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto * array_type = checkAndGetDataType<DataTypeArray>(result_type.get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Result type must be Array");

        const auto & nested = array_type->getNestedType();
        WhichDataType nested_which(nested);

        if (nested_which.isFloat32())
            return executeTyped<Float32>(arguments, input_rows_count);
        else if (nested_which.isFloat64())
            return executeTyped<Float64>(arguments, input_rows_count);
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} only supports Float32 and Float64 element types", getName());
    }

private:
    const RandomProjectionFunctionBase<MethodTag> & base;

    template <typename T>
    ColumnPtr executeTyped(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        ColumnPtr col_input = arguments[0].column->convertToFullColumnIfConst();
        const auto & col_array = assert_cast<const ColumnArray &>(*col_input);
        const auto & data_in = typeid_cast<const ColumnVector<T> &>(col_array.getData()).getData();
        const auto & offsets = col_array.getOffsets();

        if (input_rows_count == 0)
            return ColumnArray::create(ColumnVector<T>::create(), ColumnArray::ColumnOffsets::create());

        size_t input_dim = offsets[0];
        base.template ensureInitialized<T>(input_dim);

        const auto & state = base.template getState<T>();
        size_t target_dim = state.target_dim;

        auto col_data_out = ColumnVector<T>::create(input_rows_count * target_dim);
        auto & out = col_data_out->getData();

        if constexpr (UsesDenseGEMM<MethodTag>::value)
        {
            /// Dense methods (Normal, Orthogonal): validate all rows first, then batch GEMM.
            ColumnArray::Offset prev = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t row_dim = offsets[i] - prev;
                if (row_dim != input_dim)
                    throw Exception(
                        ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                        "All input arrays for {} must have the same dimension (expected {}, got {} at row {})",
                        MethodTag::name,
                        input_dim,
                        row_dim,
                        i);
                prev = offsets[i];
            }

            applyDenseProjectionBatch<T>(data_in.data(), out.data(), input_rows_count, input_dim, target_dim, state.matrix.data());
        }
        else
        {
            /// Per-row methods (Sparse, Hadamard): validate and apply one row at a time.
            /// Allocate scratch buffer once for Hadamard FWHT; Sparse ignores it.
            std::vector<T> scratch;
            if constexpr (std::is_same_v<MethodTag, HadamardMethodTag>)
                scratch.resize(state.padded_dim);

            ColumnArray::Offset prev = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t row_dim = offsets[i] - prev;
                if (row_dim != input_dim)
                    throw Exception(
                        ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                        "All input arrays for {} must have the same dimension (expected {}, got {} at row {})",
                        MethodTag::name,
                        input_dim,
                        row_dim,
                        i);

                applyPerRow<T, MethodTag>(data_in.data() + prev, out.data() + i * target_dim, state, scratch.data());
                prev = offsets[i];
            }
        }

        auto col_offsets_out = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & out_offsets = col_offsets_out->getData();
        for (size_t i = 0; i < input_rows_count; ++i)
            out_offsets[i] = (i + 1) * target_dim;

        return ColumnArray::create(std::move(col_data_out), std::move(col_offsets_out));
    }
};

template <typename MethodTag>
class RandomProjectionOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = MethodTag::name;

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<RandomProjectionOverloadResolver<MethodTag>>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 3; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }
    bool isVariadic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        if (!array_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be an Array, got {}",
                getName(),
                arguments[0].type->getName());

        const auto & nested = array_type->getNestedType();
        WhichDataType nested_which(nested);
        if (!nested_which.isFloat32() && !nested_which.isFloat64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be Array(Float32) or Array(Float64), got Array({})",
                getName(),
                nested->getName());

        if (!WhichDataType(arguments[1].type).isUInt())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument (target_dim) of function {} must be an unsigned integer, got {}",
                getName(),
                arguments[1].type->getName());

        if (!WhichDataType(arguments[2].type).isUInt())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Third argument (seed) of function {} must be an unsigned integer, got {}",
                getName(),
                arguments[2].type->getName());

        return std::make_shared<DataTypeArray>(nested);
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (!arguments[1].column || !isColumnConst(*arguments[1].column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument (target_dim) of function {} must be constant", getName());

        size_t target_dim = arguments[1].column->getUInt(0);
        if (target_dim == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument (target_dim) of function {} must be positive", getName());

        if (!arguments[2].column || !isColumnConst(*arguments[2].column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Third argument (seed) of function {} must be constant", getName());

        UInt64 seed = arguments[2].column->getUInt(0);

        DataTypes arg_types;
        arg_types.reserve(arguments.size());
        for (const auto & arg : arguments)
            arg_types.push_back(arg.type);

        return std::make_shared<RandomProjectionFunctionBase<MethodTag>>(target_dim, seed, std::move(arg_types), return_type);
    }
};

REGISTER_FUNCTION(RandomProjectionNormal)
{
    FunctionDocumentation::Description description = R"(
Applies a random projection to the input vector using a dense Gaussian random matrix.

The projection matrix `P` of size (`target_dim` x `input_dim`) is filled with entries
drawn from N(0, 1/`target_dim`). By the Johnson-Lindenstrauss lemma, pairwise distances
are approximately preserved in the projected space.

The matrix is generated once per query from the given seed, ensuring deterministic results
across distributed nodes. Both downscaling (e.g. 1024 -> 256) and upscaling (e.g. 128 -> 512)
are supported. The computation is SIMD-accelerated (AVX2/AVX-512) when available.
)";

    FunctionDocumentation::Syntax syntax = "randomProjectionNormal(vector, target_dim, seed)";

    FunctionDocumentation::Arguments arguments = {
        {"vector", "Input vector as an array of floating-point values.", {"Array(Float32)", "Array(Float64)"}},
        {"target_dim", "Target dimensionality (must be a positive constant).", {"UInt*"}},
        {"seed", "Random seed for deterministic projection (must be a constant).", {"UInt*"}},
    };

    FunctionDocumentation::ReturnedValue returned_value
        = {"An array of the same element type with `target_dim` elements.", {"Array(Float32)", "Array(Float64)"}};

    FunctionDocumentation::Examples examples = {
        {"Downscale", "SELECT randomProjectionNormal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42);", ""},
        {"Upscale", "SELECT randomProjectionNormal([1.0, 2.0]::Array(Float32), 4, 42);", ""},
    };

    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<RandomProjectionOverloadResolver<NormalMethodTag>>(documentation);
}

REGISTER_FUNCTION(RandomProjectionOrthogonal)
{
    FunctionDocumentation::Description description = R"(
Applies a random projection to the input vector using an orthogonal random matrix.

A square Gaussian matrix of size `max(target_dim, input_dim)` is generated, then
QR-decomposed via Householder reflections. The leading (`target_dim` x `input_dim`) block of the
orthogonal factor `Q` is used as the projection matrix. Orthogonal projections preserve
distances better than purely Gaussian projections for finite sample sizes.

Both downscaling and upscaling are supported. The matrix is generated once per
query from the given seed. The computation is SIMD-accelerated (AVX2/AVX-512) when available.
)";

    FunctionDocumentation::Syntax syntax = "randomProjectionOrthogonal(vector, target_dim, seed)";

    FunctionDocumentation::Arguments arguments = {
        {"vector", "Input vector as an array of floating-point values.", {"Array(Float32)", "Array(Float64)"}},
        {"target_dim", "Target dimensionality (must be a positive constant).", {"UInt*"}},
        {"seed", "Random seed for deterministic projection (must be a constant).", {"UInt*"}},
    };

    FunctionDocumentation::ReturnedValue returned_value
        = {"An array of the same element type with `target_dim` elements.", {"Array(Float32)", "Array(Float64)"}};

    FunctionDocumentation::Examples examples = {
        {"Downscale", "SELECT randomProjectionOrthogonal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42);", ""},
    };

    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<RandomProjectionOverloadResolver<OrthogonalMethodTag>>(documentation);
}

REGISTER_FUNCTION(RandomProjectionSparse)
{
    FunctionDocumentation::Description description = R"(
Applies a random projection using a sparse Achlioptas matrix.

Each entry of the projection matrix is independently `+1` (probability 1/6),
`0` (probability 2/3), or `-1` (probability 1/6), scaled by `sqrt(3/target_dim)`.
The sparsity makes this method approximately 3x faster than a dense Gaussian
projection, as ~2/3 of entries require no computation.

Both downscaling and upscaling are supported. The matrix is generated once per
query from the given seed.
)";

    FunctionDocumentation::Syntax syntax = "randomProjectionSparse(vector, target_dim, seed)";

    FunctionDocumentation::Arguments arguments = {
        {"vector", "Input vector as an array of floating-point values.", {"Array(Float32)", "Array(Float64)"}},
        {"target_dim", "Target dimensionality (must be a positive constant).", {"UInt*"}},
        {"seed", "Random seed for deterministic projection (must be a constant).", {"UInt*"}},
    };

    FunctionDocumentation::ReturnedValue returned_value
        = {"An array of the same element type with `target_dim` elements.", {"Array(Float32)", "Array(Float64)"}};

    FunctionDocumentation::Examples examples = {
        {"Downscale", "SELECT randomProjectionSparse([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42);", ""},
    };

    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<RandomProjectionOverloadResolver<SparseMethodTag>>(documentation);
}

REGISTER_FUNCTION(RandomProjectionHadamard)
{
    FunctionDocumentation::Description description = R"(
Applies a random projection using the Subsampled Randomized Hadamard Transform (SRHT).

The transform computes `y = S * H * D * x`, where `D` is a diagonal matrix of random
signs, `H` is the Walsh-Hadamard transform (computed in O(n log n) via the Fast
Walsh-Hadamard Transform), and `S` is a row-sampling operator. The input is
zero-padded to the next power of 2 internally.

For upscaling (`target_dim` > `input_dim`), multiple independent SRHT blocks are
concatenated. Both downscaling and upscaling are supported. The matrix is
generated once per query from the given seed.
)";

    FunctionDocumentation::Syntax syntax = "randomProjectionHadamard(vector, target_dim, seed)";

    FunctionDocumentation::Arguments arguments = {
        {"vector", "Input vector as an array of floating-point values.", {"Array(Float32)", "Array(Float64)"}},
        {"target_dim", "Target dimensionality (must be a positive constant).", {"UInt*"}},
        {"seed", "Random seed for deterministic projection (must be a constant).", {"UInt*"}},
    };

    FunctionDocumentation::ReturnedValue returned_value
        = {"An array of the same element type with `target_dim` elements.", {"Array(Float32)", "Array(Float64)"}};

    FunctionDocumentation::Examples examples = {
        {"Downscale", "SELECT randomProjectionHadamard([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42);", ""},
    };

    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<RandomProjectionOverloadResolver<HadamardMethodTag>>(documentation);
}

}
