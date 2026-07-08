#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnQBit.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationQBit.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/LloydMaxQuantization.h>

#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>

#include <IO/WriteHelpers.h>

#include <Common/TargetSpecific.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>

/// Include immintrin. Otherwise `simsimd` fails to build: `unknown type name '__bfloat16'`
#if USE_SIMSIMD
#    if defined(__x86_64__) || defined(__i386__)
#        include <immintrin.h>
#    endif
#    include <simsimd/simsimd.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

/// Base kernel pattern for distance functions.
/// Each kernel must provide:
///   - static constexpr auto name: function name
///   - static constexpr simsimd_metric_kind_t metric_kind (under USE_SIMSIMD): SimSIMD metric to resolve and call
///   - static void distance<T>(...): scalar distance, used when no SimSIMD kernel is available
///   - static void distanceScalar<InputType, AccumulatorType>(...): fallback scalar implementation

struct L2DistanceTransposed
{
    static constexpr auto name = "L2DistanceTransposed";
#if USE_SIMSIMD
    static constexpr simsimd_metric_kind_t metric_kind = simsimd_metric_l2_k;
    /// Largest term the SimSIMD i8 kernel adds per element (max (a - b)^2 = 255^2). Used to bound the
    /// dimension before that kernel's int32 accumulator overflows; beyond it we use the scalar path.
    static constexpr UInt64 max_int8_simd_term = 255 * 255;
#endif

    template <typename T>
    static void distance(const T * __restrict x, const T * __restrict y, std::size_t array_size, Float64 * result)
    {
        if constexpr (std::is_same_v<T, Int8>)
            distanceScalar<Int8, Float64>(x, y, array_size, result);
        else if constexpr (std::is_same_v<T, BFloat16>)
            distanceScalar<BFloat16, Float32>(x, y, array_size, result);
        else if constexpr (std::is_same_v<T, Float32>)
            distanceScalar<Float32, Float32>(x, y, array_size, result);
        else if constexpr (std::is_same_v<T, Float64>)
            distanceScalar<Float64, Float64>(x, y, array_size, result);
    }

    template <typename InputType, typename AccumulatorType>
    static void distanceScalar(const InputType * __restrict x, const InputType * __restrict y, std::size_t array_size, Float64 * result)
    {
        /// This could be vectorized, but we consider this a fallback code path, so no need to optimize it heavily
        AccumulatorType d2 = 0;
        for (size_t i = 0; i != array_size; ++i)
        {
            AccumulatorType xi = static_cast<AccumulatorType>(*(x + i));
            AccumulatorType yi = static_cast<AccumulatorType>(*(y + i));
            d2 += (xi - yi) * (xi - yi);
        }
        *result = static_cast<Float64>(std::sqrt(d2));
    }
};

struct CosineDistanceTransposed
{
    static constexpr auto name = "cosineDistanceTransposed";
#if USE_SIMSIMD
    static constexpr simsimd_metric_kind_t metric_kind = simsimd_metric_cos_k;
    /// Largest term the SimSIMD i8 kernel adds per element (max a^2 = 128^2). Used to bound the
    /// dimension before that kernel's int32 accumulator overflows; beyond it we use the scalar path.
    static constexpr UInt64 max_int8_simd_term = 128 * 128;
#endif

    template <typename T>
    static void distance(const T * __restrict x, const T * __restrict y, std::size_t array_size, Float64 * result)
    {
        if constexpr (std::is_same_v<T, Int8>)
            distanceScalar<Int8, Float64>(x, y, array_size, result);
        else if constexpr (std::is_same_v<T, BFloat16>)
            distanceScalar<BFloat16, Float32>(x, y, array_size, result);
        else if constexpr (std::is_same_v<T, Float32>)
            distanceScalar<Float32, Float32>(x, y, array_size, result);
        else if constexpr (std::is_same_v<T, Float64>)
            distanceScalar<Float64, Float64>(x, y, array_size, result);
    }

    template <typename InputType, typename AccumulatorType>
    static void distanceScalar(const InputType * __restrict x, const InputType * __restrict y, std::size_t array_size, Float64 * result)
    {
        /// This could be vectorized, but we consider this a fallback code path, so no need to optimize it heavily
        AccumulatorType ab = 0;
        AccumulatorType a2 = 0;
        AccumulatorType b2 = 0;
        for (size_t i = 0; i != array_size; ++i)
        {
            AccumulatorType xi = static_cast<AccumulatorType>(*(x + i));
            AccumulatorType yi = static_cast<AccumulatorType>(*(y + i));
            ab += xi * yi;
            a2 += xi * xi;
            b2 += yi * yi;
        }
        if (a2 == 0 && b2 == 0)
        {
            *result = 0;
        }
        else if (ab == 0)
        {
            *result = 1;
        }
        else
        {
            const auto unclipped_result = AccumulatorType(1) - ab / (std::sqrt(a2) * std::sqrt(b2));
            *result = unclipped_result > 0 ? static_cast<Float64>(unclipped_result) : Float64{0};
        }
    }
};

struct DotProductTransposed
{
    static constexpr auto name = "dotProductTransposed";
#if USE_SIMSIMD
    static constexpr simsimd_metric_kind_t metric_kind = simsimd_metric_dot_k;
    /// Largest term the SimSIMD i8 kernel adds per element (max |a * b| = 128 * 128, both equal to -128). Used to
    /// bound the dimension before that kernel's int32 accumulator overflows; beyond it we use the scalar path.
    static constexpr UInt64 max_int8_simd_term = 128 * 128;
#endif

    template <typename T>
    static void distance(const T * __restrict x, const T * __restrict y, std::size_t array_size, Float64 * result)
    {
        if constexpr (std::is_same_v<T, Int8>)
            distanceScalar<Int8, Float64>(x, y, array_size, result);
        else if constexpr (std::is_same_v<T, BFloat16>)
            distanceScalar<BFloat16, Float32>(x, y, array_size, result);
        else if constexpr (std::is_same_v<T, Float32>)
            distanceScalar<Float32, Float32>(x, y, array_size, result);
        else if constexpr (std::is_same_v<T, Float64>)
            distanceScalar<Float64, Float64>(x, y, array_size, result);
    }

    template <typename InputType, typename AccumulatorType>
    static void distanceScalar(const InputType * __restrict x, const InputType * __restrict y, std::size_t array_size, Float64 * result)
    {
        /// This could be vectorized, but we consider this a fallback code path, so no need to optimize it heavily
        AccumulatorType ab = 0;
        for (size_t i = 0; i != array_size; ++i)
        {
            AccumulatorType xi = static_cast<AccumulatorType>(*(x + i));
            AccumulatorType yi = static_cast<AccumulatorType>(*(y + i));
            ab += xi * yi;
        }
        *result = static_cast<Float64>(ab);
    }
};

/** Each [L2/cosine/...]DistanceTransposed has two calling conventions:
  * 1. User-facing (documented): DistanceTransposed(qbit, ref_vec, precision)
  * 2. Internal (undocumented): DistanceTransposed(vec.1, ..., vec.precision, qbit_size, ref_vec)
  *
  * The second form is generated by ...DistanceTransposedPartialReadsPass for partial column reads.
  * It is not exposed in documentation and users should not call it directly.
  *
  * IMPORTANT: In the second form, ref_vec type must match the original QBit element type
  * (Int8/BFloat16/Float32/Float64). This is the only way to determine the QBit type since we
  * only receive individual bit planes. Type mismatches will produce incorrect results.
  */

/// When `Quantized` is true the function operates on a `QBit(Int8)` whose codes were produced by the `quantizeBFloat16ToInt8`
/// Lloyd-Max codec. Because that quantizer is non-linear, the distance cannot be computed on the `Int8` codes directly: each
/// reconstructed code is dequantized to its Lloyd-Max reconstruction level (as `Float32`) on the fly and the distance is
/// computed against the reference (query) vector. The reference vector is polymorphic: a `Float` reference is the query, compared
/// directly (asymmetric distance) and cast to `Float32` -- the reconstruction precision of the dequantized codes -- exactly as the
/// non-quantized transposed functions cast the reference to the QBit element type (so a `BFloat16` query widens to `Float32`
/// losslessly and a `Float64` query is narrowed to the `Float32` compute precision, since the stored side reconstructs only to
/// `Float32`). An `Array(Int8)` reference is instead treated as `quantizeBFloat16ToInt8` codes and dequantized to its exact Lloyd-Max
/// levels (equivalent to `dequantizeInt8ToBFloat16`). Note that `p` truncates only the stored `QBit` codes; the `Array(Int8)`
/// reference is a complete query and is always reconstructed at full 8-bit precision (row 8 of the LUT), so the comparison is a
/// symmetric quantized-vs-quantized distance only at `p = 8` -- for `p < 8` only the stored side is read at coarser precision. The
/// function is registered under the `...Quantized` name (e.g. `cosineDistanceTransposedQuantized`).
template <typename Kernel, bool Quantized = false>
class FunctionArrayDistance : public IFunction
{
public:
    String getName() const override
    {
        if constexpr (Quantized)
            return String(Kernel::name) + "Quantized";
        else
            return Kernel::name;
    }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayDistance>(); }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception(
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Number of arguments for function {} can't be {}, should be at least 3",
                getName(),
                arguments.size());

        /// Check if we are in the optimised internal calling convention generated by DistanceTransposedPartialReadsPass:
        ///   non-strided: DistanceTransposed(vec.1, ..., vec.p, qbit_size, ref_vec)
        ///   strided:     DistanceTransposed(plane..., stride, used_dims, ref_vec)
        /// If something goes wrong, we fallback to the original DistanceTransposed(qbit, ref_vec, precision[, used_dims]) handling. The
        /// arguments in optimised case are generated by us and are almost certainly correct. It is extremely unlikely that a user will
        /// write the optimised case manually. Thus, any error in arguments is treated as user error from the original case.
        if (parseInternalArguments(arguments).has_value())
            return std::make_shared<DataTypeFloat64>();

        if (arguments.size() > 4)
            throw Exception(
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "Number of arguments for function {} is {}. Expected 3 or 4",
                getName(),
                arguments.size());

        /// Check the first two arguments
        const auto * zeroth_arg_type = checkAndGetDataType<DataTypeQBit>(arguments[0].type.get());
        const auto * first_arg_type = checkAndGetDataType<DataTypeArray>(arguments[1].type.get());

        if (!zeroth_arg_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be a QBit", getName());

        if constexpr (Quantized)
        {
            if (!WhichDataType(zeroth_arg_type->getElementType()).isInt8())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument of function {} must be a QBit(Int8) holding quantizeBFloat16ToInt8 codes, got QBit({})",
                    getName(),
                    zeroth_arg_type->getElementType()->getName());
        }

        if (!first_arg_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be an Array", getName());

        /// Check that precision (third argument) is valid
        const auto & precision_col = arguments[2];
        WhichDataType which(precision_col.type);

        if (!which.isUInt8())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The third argument of function {} must be a UInt8 constant, got {}",
                getName(),
                precision_col.type->getName());

        if (!(precision_col.column && precision_col.column->isConst()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The third argument of function {} must be a UInt8 constant, got {} (not constant)",
                getName(),
                precision_col.type->getName());

        const auto precision = precision_col.column->getUInt(0);
        const auto element_size = zeroth_arg_type->getElementSize();

        if (precision == 0 || precision > element_size)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The third argument (precision) of function {} must be in range [1, {}] for {} QBit, got {}",
                getName(),
                element_size,
                zeroth_arg_type->getElementType()->getName(),
                precision);

        /// Optional fourth argument: the number of dimensions to read (for Matryoshka-style partial-dimension search).
        if (arguments.size() == 4)
        {
            const auto & used_dims_col = arguments[3];
            if (!WhichDataType(used_dims_col.type).isUInt() || !(used_dims_col.column && used_dims_col.column->isConst()))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The fourth argument (used_dims) of function {} must be a UInt constant, got {}",
                    getName(),
                    used_dims_col.type->getName());

            const auto used_dims = used_dims_col.column->getUInt(0);
            const auto dimension = zeroth_arg_type->getDimension();
            const auto stride = zeroth_arg_type->getStride();

            if (used_dims == 0 || used_dims > dimension || used_dims % stride != 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The fourth argument (used_dims) of function {} must be a positive multiple of the QBit stride {} not exceeding the "
                    "dimension {}, got {}",
                    getName(),
                    stride,
                    dimension,
                    used_dims);
        }

        return std::make_shared<DataTypeFloat64>();
    }

    /// Parameters parsed from the internal (optimised) calling convention.
    struct InternalArguments
    {
        size_t num_planes; /// Number of bit-plane FixedString columns at the front of the argument list.
        size_t precision; /// Number of bit planes per stride group.
        size_t stride; /// Number of dimensions per stride group.
        size_t used_dims; /// Total number of reconstructed dimensions (num_groups * stride).
    };

    /// Detects and validates the internal forms:
    ///   non-strided: (plane_0, ..., plane_{p-1}, qbit_size, ref_vec)              -- one trailing UInt size
    ///   strided:     (plane_0, ..., plane_{g*p-1}, stride, used_dims, ref_vec)         -- two trailing UInt sizes, planes group-major
    /// Returns std::nullopt if `arguments` is not a (valid) internal form (e.g. it is the user-facing form).
    ///
    /// `require_const_sizes` must be true during type inference (the sizes are constants generated by the optimiser pass), but false
    /// during execution: `useDefaultImplementationForConstants` may have unwrapped the all-constant case into non-const single-row
    /// columns by the time `executeImpl` runs, while the values read via `getUInt(0)` are still correct.
    std::optional<InternalArguments> parseInternalArguments(const ColumnsWithTypeAndName & arguments, bool require_const_sizes = true) const
    {
        constexpr size_t max_precision = 64;

        if (arguments.size() < 3)
            return {};

        const auto * ref_vec_type = checkAndGetDataType<DataTypeArray>(arguments.back().type.get());
        if (!ref_vec_type)
            return {};

        if (!DataTypeQBit::isSupportedElementType(ref_vec_type->getNestedType()))
            return {};

        auto is_uint_size = [require_const_sizes](const ColumnWithTypeAndName & arg)
        { return arg.column && (!require_const_sizes || arg.column->isConst()) && WhichDataType(arg.type).isUInt(); };

        /// The element right before the reference vector is always a UInt size (qbit_size for non-strided, used_dims for strided).
        if (!is_uint_size(arguments[arguments.size() - 2]))
            return {};

        InternalArguments res{};

        /// Strided form has a second UInt size (the stride) right before the used_dims size.
        const bool strided = arguments.size() >= 4 && is_uint_size(arguments[arguments.size() - 3]);
        if (strided)
        {
            res.stride = arguments[arguments.size() - 3].column->getUInt(0);
            res.used_dims = arguments[arguments.size() - 2].column->getUInt(0);
            res.num_planes = arguments.size() - 3;

            if (res.stride == 0 || res.used_dims == 0 || res.used_dims % res.stride != 0)
                return {};
            const size_t num_groups = res.used_dims / res.stride;
            if (res.num_planes == 0 || res.num_planes % num_groups != 0)
                return {};
            res.precision = res.num_planes / num_groups;
        }
        else
        {
            res.used_dims = arguments[arguments.size() - 2].column->getUInt(0);
            res.stride = res.used_dims;
            res.num_planes = arguments.size() - 2;
            res.precision = res.num_planes;
        }

        if (res.precision == 0 || res.precision > max_precision)
            return {};

        /// All bit-plane arguments must be FixedString of the per-group size.
        const auto plane_bytes = DataTypeQBit::bitsToBytes(res.stride);
        for (size_t i = 0; i < res.num_planes; ++i)
        {
            const auto * arg_type = checkAndGetDataType<DataTypeFixedString>(arguments[i].type.get());
            if (!arg_type || arg_type->getN() != plane_bytes)
                return {};
        }

        /// For the quantized form the QBit is always Int8 (8 bit planes) and the reference vector is either the full-precision
        /// Float32 query or a quantized Array(Int8) query, so the reference type no longer encodes the QBit element type; cap the
        /// precision at 8 directly.
        if constexpr (Quantized)
        {
            /// The quantized internal form is only ever generated with an Array(Float32) reference (DistanceTransposedPartialReadsPass
            /// casts a Float query to Array(Float32)) or an Array(Int8) reference (a quantized query, passed through unchanged), and
            /// executeQuantizedDistanceCalculation reads it as ColumnVector<Float32> or dequantizes it from ColumnVector<Int8>
            /// accordingly. Reject any other element type so a hand-written internal call with e.g. an Array(Float64) or Array(BFloat16)
            /// reference falls through to the user-facing path and gets a clean ILLEGAL_TYPE_OF_ARGUMENT instead of reinterpreting the
            /// reference vector's memory as Float32.
            const WhichDataType ref_which(ref_vec_type->getNestedType());
            if (!ref_which.isFloat32() && !ref_which.isInt8())
                return {};

            if (res.precision > 8)
                return {};
        }
        else
        {
            /// The type of reference vector dictates what type QBit had before we sliced it into bit planes.
            /// Check that the precision doesn't exceed the maximum for the reference vector type.
            const size_t max_precision_for_type = ref_vec_type->getNestedType()->getSizeOfValueInMemory() * 8;
            if (res.precision > max_precision_for_type)
                return {};
        }

        return res;
    }


    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        const auto & last_arg = arguments.back();

        /// The user-facing form DistanceTransposed(qbit, ref_vec, precision[, used_dims]) has a QBit as the first argument.
        if (checkAndGetDataType<DataTypeQBit>(arguments[0].type.get()))
            return executeWithQBitColumnConverted(arguments, input_rows_count);

        /// Otherwise we are in the internal form generated by the optimiser pass (or written manually):
        ///   non-strided: (vec.1, ..., vec.p, qbit_size, ref_vec)
        ///   strided:     (plane..., stride, used_dims, ref_vec)   -- planes group-major
        /// The sizes may have been unwrapped from constants by useDefaultImplementationForConstants, so don't require them to be const.
        const auto parsed = parseInternalArguments(arguments, /*require_const_sizes=*/false);
        chassert(parsed.has_value());
        const size_t precision = parsed->precision;
        const size_t stride = parsed->stride;
        const size_t used_dims = parsed->used_dims;
        const size_t num_planes = parsed->num_planes;

        /// First, check that the reference vector sizes match the reconstructed dimension
        const ColumnArray & reference_vector = *assert_cast<const ColumnArray *>(extractFromConst(arguments.back().column).get());
        const auto & offsets = reference_vector.getOffsets();

        /// In dry run, the offsets can be empty with non-constant reference vector
        if (!offsets.empty())
        {
            for (size_t i = 0; i < reference_vector.size(); ++i)
            {
                if (offsets[i] - offsets[i - 1] != used_dims)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "The reference vector in the last argument of function {} has wrong size. Got: {}, expected: {}",
                        getName(),
                        offsets[i] - offsets[i - 1],
                        used_dims);
            }
        }

        /// Continue with execution
        auto type_y = checkAndGetDataType<DataTypeArray>(last_arg.type.get())->getNestedType()->getTypeId();

        /// Expand constant bit plane columns to input_rows_count rows so executeDistanceCalculation doesn't need to handle constants
        ColumnsWithTypeAndName expanded_planes;
        expanded_planes.reserve(num_planes);
        for (size_t i = 0; i < num_planes; ++i)
        {
            const auto & arg = arguments[i];
            if (arg.column->isConst())
                expanded_planes.emplace_back(arg.column->convertToFullColumnIfConst(), arg.type, arg.name);
            else
                expanded_planes.emplace_back(arg);
        }

        if constexpr (Quantized)
        {
            /// The QBit is Int8 (Lloyd-Max codes) and the reference vector is Float32. Reconstruct each code, dequantize it to
            /// its Float32 Lloyd-Max level, and compute the distance against the reference. See executeQuantizedDistanceCalculation.
            const bool ref_is_const = arguments.back().column->isConst();
            return ref_is_const
                ? executeQuantizedDistanceCalculation<true>(reference_vector, expanded_planes, precision, stride, used_dims, input_rows_count)
                : executeQuantizedDistanceCalculation<false>(reference_vector, expanded_planes, precision, stride, used_dims, input_rows_count);
        }

        /// We need to find two types: the type of the reference vector and the type of the calculation.
        /// The type of calculation is determined by the value of `precision. For example, if col_x is Float32 and p = 16, we will only have
        /// 16 meaningful bits to calculate the distance. So we can downcast the reference vector to BFloat16 and do calculations faster.
        auto dispatch_by_accum_type = [&]<typename RefT>(auto func)
        {
            /// Int8 has only 8 bits and cannot be downcasted to a narrower type, so calculations are always done in Int8.
            if constexpr (std::is_same_v<RefT, Int8>)
            {
                return func.template operator()<Int8, Int8>();
            }
            else
            {
                auto calc_type
                    = (precision <= 16 ? TypeToTypeIndex<BFloat16>
                                       : (precision <= 32 ? TypeToTypeIndex<Float32> : TypeToTypeIndex<Float64>));

                /// Float64 cannot be downcasted to Float32 or BFloat16 in an easy way by reordering bits. That is why with it we always do
                /// calculations in full width. Alternatively, we could static_cast each element when calculating, but it is slower.
                if (std::is_same_v<RefT, Float64>)
                    return func.template operator()<RefT, Float64>();
                else if (calc_type == TypeToTypeIndex<Float32>)
                    return func.template operator()<RefT, Float32>();
                else if (calc_type == TypeToTypeIndex<BFloat16>)
                    return func.template operator()<RefT, BFloat16>();
                else
                    UNREACHABLE();
            }
        };

        const bool ref_is_const = arguments.back().column->isConst();

        auto execute_with_type = [&]<typename T>() -> ColumnPtr
        {
            return dispatch_by_accum_type.template operator()<T>(
                [&]<typename RefT, typename CalcT>()
                {
                    return ref_is_const
                        ? executeDistanceCalculation<RefT, CalcT, true>(
                              reference_vector, expanded_planes, precision, stride, used_dims, input_rows_count)
                        : executeDistanceCalculation<RefT, CalcT, false>(
                              reference_vector, expanded_planes, precision, stride, used_dims, input_rows_count);
                });
        };

        /// Dispatch to type-specific implementation based on reference vector type
        switch (type_y)
        {
            case TypeIndex::Int8:
                return execute_with_type.template operator()<Int8>();
            case TypeIndex::BFloat16:
                return execute_with_type.template operator()<BFloat16>();
            case TypeIndex::Float32:
                return execute_with_type.template operator()<Float32>();
            case TypeIndex::Float64:
                return execute_with_type.template operator()<Float64>();
            default:
                UNREACHABLE();
        }
    }


private:
    static ColumnPtr extractFromConst(const ColumnPtr & column)
    {
        return column->isConst() ? assert_cast<const ColumnConst *>(column.get())->getDataColumnPtr() : column;
    }

    /// DistanceTransposed(qbit, ref_vec, precision[, used_dims]) case. Convert arguments to the internal bit-plane form before executing.
    /// For non-strided QBit this produces (qbit.1, ..., qbit.p, dimension, ref_vec); for strided QBit it produces the group-major
    /// (plane..., stride, used_dims, ref_vec) form covering only the first `used_dims / stride` stride groups.
    ColumnPtr executeWithQBitColumnConverted(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        ColumnsWithTypeAndName converted_arguments;

        const auto * qbit_type = assert_cast<const DataTypeQBit *>(arguments[0].type.get());
        const auto * qbit_ptr = assert_cast<const ColumnQBit *>(extractFromConst(arguments[0].column).get());
        const auto & qbit_tuple = assert_cast<const ColumnTuple &>(qbit_ptr->getTupleColumn());
        const auto precision = arguments[2].column->getUInt(0);
        const auto qbit_is_const = arguments[0].column->isConst();
        const auto element_size = qbit_type->getElementSize();
        const auto stride = qbit_type->getStride();
        const auto dimension = qbit_type->getDimension();
        const auto bit_plane_type = qbit_type->getNestedTupleElementType();
        const bool is_strided = qbit_type->getNumStrides() > 1;

        /// The number of dimensions to read. Defaults to the full dimension when the optional 4th argument is absent.
        const size_t used_dims = (arguments.size() == 4) ? arguments[3].column->getUInt(0) : dimension;

        auto add_plane = [&](size_t tuple_idx)
        {
            auto bit_plane_col = qbit_tuple.getColumn(tuple_idx).getPtr();
            /// If QBit was constant, wrap the bit plane in ColumnConst to preserve const-ness for downstream handling
            if (qbit_is_const)
                bit_plane_col = ColumnConst::create(std::move(bit_plane_col), input_rows_count);
            converted_arguments.emplace_back(std::move(bit_plane_col), bit_plane_type, toString(tuple_idx + 1));
        };

        if (is_strided)
        {
            const size_t num_groups = used_dims / stride;
            /// Group-major order: for each stride group, its first `precision` bit planes (tuple index = group * element_size + bit).
            for (size_t group = 0; group < num_groups; ++group)
                for (size_t bit = 0; bit < precision; ++bit)
                    add_plane(group * element_size + bit);

            converted_arguments.emplace_back(
                DataTypeUInt64().createColumnConst(1, stride), std::make_shared<DataTypeUInt64>(), "stride");
            converted_arguments.emplace_back(DataTypeUInt64().createColumnConst(1, used_dims), std::make_shared<DataTypeUInt64>(), "used_dims");
        }
        else
        {
            for (size_t bit = 0; bit < precision; ++bit)
                add_plane(bit);

            converted_arguments.emplace_back(
                DataTypeUInt64().createColumnConst(1, dimension), std::make_shared<DataTypeUInt64>(), "dimension");
        }

        /// Cast reference vector to match QBit element type to ensure correct dispatch. For the quantized form the QBit codes are
        /// dequantized to Float32 levels on the fly, so a Float reference (query) is cast to Array(Float32); a quantized Array(Int8)
        /// reference is passed through unchanged and dequantized on the fly exactly like the QBit codes.
        auto ref_vec_type = arguments[1].type;
        auto expected_ref_vec_type = [&]() -> std::shared_ptr<DataTypeArray>
        {
            if constexpr (Quantized)
            {
                if (const auto * ref_array = checkAndGetDataType<DataTypeArray>(ref_vec_type.get());
                    ref_array && WhichDataType(ref_array->getNestedType()).isInt8())
                    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>());
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
            }
            else
                return std::make_shared<DataTypeArray>(qbit_type->getElementType());
        }();

        if (ref_vec_type->equals(*expected_ref_vec_type))
        {
            converted_arguments.emplace_back(arguments[1]);
        }
        else
        {
            auto casted_column = castColumn(arguments[1], expected_ref_vec_type);
            converted_arguments.emplace_back(casted_column, expected_ref_vec_type, arguments[1].name);
        }

        /// We go back to the function that called us, but now with converted arguments
        return executeImpl(converted_arguments, nullptr, input_rows_count);
    }

#if USE_SIMSIMD
    /// Resolve the SimSIMD kernel for this distance metric and calculation type, or nullptr if none exists.
    template <typename CalcT>
    static simsimd_metric_dense_punned_t resolveSimdKernel()
    {
        const simsimd_datatype_t datatype = std::is_same_v<CalcT, Int8>
            ? simsimd_datatype_i8_k
            : (std::is_same_v<CalcT, BFloat16> ? simsimd_datatype_bf16_k
                                               : (std::is_same_v<CalcT, Float32> ? simsimd_datatype_f32_k : simsimd_datatype_f64_k));
        simsimd_kernel_punned_t simd_kernel = nullptr;
        simsimd_capability_t unused = simsimd_cap_any_k;
        simsimd_find_kernel_punned(Kernel::metric_kind, datatype, simsimd_capabilities(), simsimd_cap_any_k, &simd_kernel, &unused);
        return std::bit_cast<simsimd_metric_dense_punned_t>(simd_kernel); /// NOLINT(bugprone-bitwise-pointer-cast)
    }
#endif

    /// RefT is the type of the reference vector, CalcT is the type used for calculation.
    /// `planes` holds `num_groups * precision` FixedString bit-plane columns in group-major order (plane[g * precision + b]).
    /// Each stride group is untransposed into its own contiguous slice of the reconstructed `used_dims`-element vector.
    template <typename RefT, typename CalcT, bool ref_is_const>
    ColumnPtr executeDistanceCalculation(
        const ColumnArray & col_y,
        const ColumnsWithTypeAndName & planes,
        const size_t precision,
        const size_t stride,
        const size_t used_dims,
        size_t input_rows_count) const
    {
        const size_t num_groups = used_dims / stride;
        const size_t bytes_per_group = DataTypeQBit::bitsToBytes(stride);
        /// Number of floats one group untransposes into. When strided, stride % 8 == 0, so this equals `stride` and groups pack
        /// contiguously into a `used_dims`-element buffer. For the non-strided single group it may be padded above `used_dims`.
        const size_t padded_group = bytes_per_group * 8;
        const size_t padded_array_size = num_groups * padded_group;

        /// For the sake of speed, downcast the reference vector to CalcT if `precision` is low enough
        const auto & array_data = static_cast<const ColumnVector<RefT> &>(col_y.getData()).getData();
        const PaddedPODArray<CalcT> * data_ptr = nullptr;
        PaddedPODArray<CalcT> array_data_downcasted;
        if constexpr (!std::is_same_v<RefT, CalcT>)
        {
            array_data_downcasted.resize(array_data.size());
            for (size_t i = 0; i < array_data.size(); ++i)
                array_data_downcasted[i] = static_cast<CalcT>(array_data[i]);
            data_ptr = &array_data_downcasted;
        }
        else
        {
            data_ptr = &array_data;
        }

        /// Only needed for non-const reference vectors
        [[maybe_unused]] const auto & ref_offsets = col_y.getOffsets();

        auto col_res = ColumnVector<Float64>::create(input_rows_count);
        auto & result_data = col_res->getData();

        /// Note: the 8-bit word is `uint8_t` (not ClickHouse's `UInt8`, which is `char8_t` and does not satisfy `std::countr_zero`).
        using Word = std::conditional_t<
            sizeof(CalcT) == 1,
            uint8_t,
            std::conditional_t<sizeof(CalcT) == 2, UInt16, std::conditional_t<sizeof(CalcT) == 4, UInt32, UInt64>>>;

        /// We process 32 rows per iteration. It's a magic number, but gives a good trade-off between memory usage and performance
        constexpr size_t block_size = 32;
        VectorWithMemoryTracking<CalcT> block(block_size * padded_array_size);
        auto block_row = [&](size_t r) -> CalcT * { return block.data() + r * padded_array_size; };

#if USE_SIMSIMD
        simsimd_metric_dense_punned_t simd_kernel = resolveSimdKernel<CalcT>();
        /// SimSIMD's i8 kernels accumulate in int32, which overflows for large dimensions (the scalar
        /// fallback accumulates in Float64). Use the scalar path beyond the overflow-safe dimension.
        if constexpr (std::is_same_v<CalcT, Int8>)
            if (used_dims > static_cast<size_t>(std::numeric_limits<Int32>::max()) / Kernel::max_int8_simd_term)
                simd_kernel = nullptr;
#endif

#if USE_MULTITARGET_CODE
        const auto untranspose_kernel = SerializationQBit::resolveUntransposeBitPlane<Word>();
#else
        constexpr auto untranspose_kernel = TargetSpecific::Default::untransposeBitPlaneImpl<Word>;
#endif

        for (size_t base_row = 0; base_row < input_rows_count; base_row += block_size)
        {
            const size_t rows_in_block = std::min(block_size, input_rows_count - base_row);

            memset(block.data(), 0, rows_in_block * padded_array_size * sizeof(CalcT));

            /// Untranspose, for each stride group, its `precision` bit planes into that group's slice of every row of the block
            for (size_t group = 0; group < num_groups; ++group)
            {
                for (size_t bit = 0; bit < precision; ++bit)
                {
                    const auto & col = assert_cast<const ColumnFixedString &>(*planes[group * precision + bit].column);
                    Word bit_mask = static_cast<Word>(Word(1) << (sizeof(Word) * 8 - 1 - bit));

                    for (size_t r = 0; r < rows_in_block; ++r)
                    {
                        const UInt8 * src = reinterpret_cast<const UInt8 *>(col.getChars().data()) + (base_row + r) * bytes_per_group;
                        untranspose_kernel(
                            src, reinterpret_cast<Word *>(block_row(r) + group * padded_group), padded_group, bit_mask);
                    }
                }
            }

            /// Calculate distance
            for (size_t r = 0; r < rows_in_block; ++r)
            {
                /// The branching in `else` is fine performance-wise since multiple reference vectors per QBit is rare
                const CalcT * ref_data = [&]()
                {
                    if constexpr (ref_is_const)
                        return data_ptr->data();
                    else
                        return data_ptr->data() + (base_row + r == 0 ? 0 : ref_offsets[base_row + r - 1]);
                }();

                auto * dst = block_row(r);
                Float64 * res = &result_data[base_row + r];

#if USE_SIMSIMD
                /// Branch is scary, but clang hoists it out of the loop.
                if (simd_kernel)
                    simd_kernel(dst, ref_data, used_dims, res);
                else
#endif
                    Kernel::distance(dst, ref_data, used_dims, res);
            }
        }

        return col_res;
    }

    /// Quantized variant: the QBit holds Int8 Lloyd-Max codes and the reference vector is either the full-precision Float32 query or
    /// a quantized Array(Int8) query.
    /// `planes` holds `num_groups * precision` FixedString bit-plane columns in group-major order (plane[g * precision + b]).
    /// Each stride group is untransposed into a contiguous slice of a `used_dims`-element buffer of raw code bytes, which is then
    /// dequantized to Float32 Lloyd-Max reconstruction levels (rounding a truncated code to its coarse cell's centre) before the
    /// Float32 distance kernel runs. The distance therefore equals the distance computed on `dequantizeInt8ToBFloat16` of the codes.
    /// An Array(Int8) reference is a complete (not partially read) quantized query, so it is dequantized at full precision (row 8 of
    /// the LUT, i.e. `dequantizeInt8ToBFloat16`) before the kernel runs; a Float32 reference is used verbatim.
    template <bool ref_is_const>
    ColumnPtr executeQuantizedDistanceCalculation(
        const ColumnArray & col_y,
        const ColumnsWithTypeAndName & planes,
        const size_t precision,
        const size_t stride,
        const size_t used_dims,
        size_t input_rows_count) const
    {
        const size_t num_groups = used_dims / stride;
        const size_t bytes_per_group = DataTypeQBit::bitsToBytes(stride);
        /// Number of code bytes one group untransposes into. When strided, stride % 8 == 0, so this equals `stride` and groups pack
        /// contiguously into a `used_dims`-element buffer. For the non-strided single group it may be padded above `used_dims`.
        const size_t padded_group = bytes_per_group * 8;
        const size_t padded_array_size = num_groups * padded_group;

        /// The reference (query) vector is either the full-precision Float32 query (an Array(Float32), used verbatim) or a quantized
        /// Array(Int8) query, whose codes are dequantized at full precision (row 8 of the LUT, i.e. `dequantizeInt8ToBFloat16`) into a
        /// Float32 buffer once, up front. `parseInternalArguments` guarantees the reference element type is exactly Float32 or Int8.
        const IColumn & ref_data_column = col_y.getData();
        PaddedPODArray<Float32> ref_dequantized;
        const PaddedPODArray<Float32> * ref_data_ptr = nullptr;
        if (const auto * ref_f32 = checkAndGetColumn<ColumnVector<Float32>>(&ref_data_column))
        {
            ref_data_ptr = &ref_f32->getData();
        }
        else
        {
            const auto & ref_codes = assert_cast<const ColumnVector<Int8> &>(ref_data_column).getData();
            const std::array<Float32, 256> & dequant_ref = LloydMax::transposedDequantLUT()[8];
            ref_dequantized.resize(ref_codes.size());
            for (size_t i = 0; i < ref_codes.size(); ++i)
                ref_dequantized[i] = dequant_ref[static_cast<uint8_t>(ref_codes[i])];
            ref_data_ptr = &ref_dequantized;
        }
        const auto & ref_array_data = *ref_data_ptr;
        [[maybe_unused]] const auto & ref_offsets = col_y.getOffsets();

        auto col_res = ColumnVector<Float64>::create(input_rows_count);
        auto & result_data = col_res->getData();

        /// Reconstruction table for this precision: maps a raw code byte to its Float32 Lloyd-Max level.
        const std::array<Float32, 256> & dequant = LloydMax::transposedDequantLUT()[precision];

        /// We process 32 rows per iteration, mirroring executeDistanceCalculation.
        constexpr size_t block_size = 32;
        /// Reconstructed raw code bytes for the block; the Int8 QBit untransposes into 8-bit words.
        VectorWithMemoryTracking<uint8_t> codes(block_size * padded_array_size);
        /// The dequantized Float32 vectors for the block.
        VectorWithMemoryTracking<Float32> block(block_size * padded_array_size);
        auto codes_row = [&](size_t r) -> uint8_t * { return codes.data() + r * padded_array_size; };
        auto block_row = [&](size_t r) -> Float32 * { return block.data() + r * padded_array_size; };

#if USE_SIMSIMD
        simsimd_metric_dense_punned_t simd_kernel = resolveSimdKernel<Float32>();
#endif

#if USE_MULTITARGET_CODE
        const auto untranspose_kernel = SerializationQBit::resolveUntransposeBitPlane<uint8_t>();
#else
        constexpr auto untranspose_kernel = TargetSpecific::Default::untransposeBitPlaneImpl<uint8_t>;
#endif

        for (size_t base_row = 0; base_row < input_rows_count; base_row += block_size)
        {
            const size_t rows_in_block = std::min(block_size, input_rows_count - base_row);

            memset(codes.data(), 0, rows_in_block * padded_array_size * sizeof(uint8_t));

            /// Untranspose, for each stride group, its `precision` bit planes (MSB first) into that group's code-byte slice.
            for (size_t group = 0; group < num_groups; ++group)
            {
                for (size_t bit = 0; bit < precision; ++bit)
                {
                    const auto & col = assert_cast<const ColumnFixedString &>(*planes[group * precision + bit].column);
                    const uint8_t bit_mask = static_cast<uint8_t>(1u << (8 - 1 - bit));

                    for (size_t r = 0; r < rows_in_block; ++r)
                    {
                        const UInt8 * src = reinterpret_cast<const UInt8 *>(col.getChars().data()) + (base_row + r) * bytes_per_group;
                        untranspose_kernel(src, codes_row(r) + group * padded_group, padded_group, bit_mask);
                    }
                }
            }

            /// Dequantize the reconstructed codes to Float32 Lloyd-Max levels. Only the first `used_dims` entries are meaningful:
            /// strided groups pack contiguously, and for the single non-strided group the trailing entries are padding.
            for (size_t r = 0; r < rows_in_block; ++r)
            {
                const uint8_t * src_codes = codes_row(r);
                Float32 * dst = block_row(r);
                for (size_t i = 0; i < used_dims; ++i)
                    dst[i] = dequant[src_codes[i]];
            }

            /// Calculate distance
            for (size_t r = 0; r < rows_in_block; ++r)
            {
                /// The branching in `else` is fine performance-wise since multiple reference vectors per QBit is rare
                const Float32 * ref_data = [&]()
                {
                    if constexpr (ref_is_const)
                        return ref_array_data.data();
                    else
                        return ref_array_data.data() + (base_row + r == 0 ? 0 : ref_offsets[base_row + r - 1]);
                }();

                Float32 * dst = block_row(r);
                Float64 * res = &result_data[base_row + r];

#if USE_SIMSIMD
                /// Branch is scary, but clang hoists it out of the loop.
                if (simd_kernel)
                    simd_kernel(dst, ref_data, used_dims, res);
                else
#endif
                    Kernel::distance(dst, ref_data, used_dims, res);
            }
        }

        return col_res;
    }
};

/// Used by TupleOrArrayFunction
FunctionPtr createFunctionArrayL2DistanceTransposed(ContextPtr context_);
FunctionPtr createFunctionArrayCosineDistanceTransposed(ContextPtr context_);
FunctionPtr createFunctionArrayDotProductTransposed(ContextPtr context_);
FunctionPtr createFunctionArrayL2DistanceTransposedQuantized(ContextPtr context_);
FunctionPtr createFunctionArrayCosineDistanceTransposedQuantized(ContextPtr context_);
FunctionPtr createFunctionArrayDotProductTransposedQuantized(ContextPtr context_);

FunctionPtr createFunctionArrayL2DistanceTransposed(ContextPtr context_)
{
    return FunctionArrayDistance<L2DistanceTransposed>::create(context_);
}

FunctionPtr createFunctionArrayCosineDistanceTransposed(ContextPtr context_)
{
    return FunctionArrayDistance<CosineDistanceTransposed>::create(context_);
}

FunctionPtr createFunctionArrayDotProductTransposed(ContextPtr context_)
{
    return FunctionArrayDistance<DotProductTransposed>::create(context_);
}

FunctionPtr createFunctionArrayL2DistanceTransposedQuantized(ContextPtr context_)
{
    return FunctionArrayDistance<L2DistanceTransposed, true>::create(context_);
}

FunctionPtr createFunctionArrayCosineDistanceTransposedQuantized(ContextPtr context_)
{
    return FunctionArrayDistance<CosineDistanceTransposed, true>::create(context_);
}

FunctionPtr createFunctionArrayDotProductTransposedQuantized(ContextPtr context_)
{
    return FunctionArrayDistance<DotProductTransposed, true>::create(context_);
}
}
