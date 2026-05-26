#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <base/BFloat16.h>
#include <base/bit_cast.h>
#include <Common/TargetSpecific.h>

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

template <typename Float>
struct FloatTraits;
template <>
struct FloatTraits<Float64>
{
    using UInt = UInt64;
    static constexpr UInt abs_mask = 0x7FFFFFFFFFFFFFFFULL;
    static constexpr UInt inf_bits = 0x7FF0000000000000ULL;
};
template <>
struct FloatTraits<Float32>
{
    using UInt = UInt32;
    static constexpr UInt abs_mask = 0x7FFFFFFFu;
    static constexpr UInt inf_bits = 0x7F800000u;
};
template <>
struct FloatTraits<BFloat16>
{
    using UInt = UInt16;
    static constexpr UInt abs_mask = 0x7FFF;
    static constexpr UInt inf_bits = 0x7F80;
};

template <typename Float, typename UInt = typename FloatTraits<Float>::UInt>
inline Float processOne(Float v, UInt mask)
{
    using Traits = FloatTraits<Float>;
    const UInt bits = bit_cast<UInt>(v);
    const UInt is_nan = ((bits & Traits::abs_mask) > Traits::inf_bits) ? UInt(~UInt{0}) : UInt{0};
    return bit_cast<Float>(bits & (mask | is_nan));
}

template <typename Float, typename UInt>
static void processRangeScalar(const Float * __restrict src, Float * __restrict dst, UInt mask, size_t n)
{
    for (size_t i = 0; i < n; ++i)
        dst[i] = processOne(src[i], mask);
}

#if USE_MULTITARGET_CODE

X86_64_V3_FUNCTION_SPECIFIC_ATTRIBUTE
static void processRangeF64_v3(const Float64 * __restrict src, Float64 * __restrict dst, UInt64 mask, size_t n)
{
    constexpr size_t W = 4;
    const __m256i mask_v = _mm256_set1_epi64x(static_cast<int64_t>(mask));
    size_t i = 0;
    for (; i + W <= n; i += W)
    {
        __m256d v = _mm256_loadu_pd(src + i);
        __m256d nan = _mm256_cmp_pd(v, v, _CMP_UNORD_Q);
        __m256i emsk = _mm256_or_si256(_mm256_castpd_si256(nan), mask_v);
        __m256i ri = _mm256_and_si256(_mm256_castpd_si256(v), emsk);
        _mm256_storeu_pd(dst + i, _mm256_castsi256_pd(ri));
    }
    for (; i < n; ++i)
        dst[i] = processOne(src[i], mask);
}

X86_64_V3_FUNCTION_SPECIFIC_ATTRIBUTE
static void processRangeF32_v3(const Float32 * __restrict src, Float32 * __restrict dst, UInt32 mask, size_t n)
{
    constexpr size_t W = 8;
    const __m256i mask_v = _mm256_set1_epi32(static_cast<int32_t>(mask));
    size_t i = 0;
    for (; i + W <= n; i += W)
    {
        __m256 v = _mm256_loadu_ps(src + i);
        __m256 nan = _mm256_cmp_ps(v, v, _CMP_UNORD_Q);
        __m256i emsk = _mm256_or_si256(_mm256_castps_si256(nan), mask_v);
        __m256i ri = _mm256_and_si256(_mm256_castps_si256(v), emsk);
        _mm256_storeu_ps(dst + i, _mm256_castsi256_ps(ri));
    }
    for (; i < n; ++i)
        dst[i] = processOne(src[i], mask);
}

/// BFloat16 NaN test on raw bit pattern: (x & 0x7FFF) > 0x7F80 (Inf).
X86_64_V3_FUNCTION_SPECIFIC_ATTRIBUTE
static void processRangeBF16_v3(const BFloat16 * __restrict src, BFloat16 * __restrict dst, UInt16 mask, size_t n)
{
    using Traits = FloatTraits<BFloat16>;
    constexpr size_t W = 16;
    const __m256i mask_v = _mm256_set1_epi16(static_cast<int16_t>(mask));
    const __m256i abs_m = _mm256_set1_epi16(static_cast<int16_t>(Traits::abs_mask));
    const __m256i inf_v = _mm256_set1_epi16(static_cast<int16_t>(Traits::inf_bits));
    size_t i = 0;
    for (; i + W <= n; i += W)
    {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(src + i));
        __m256i abs_b = _mm256_and_si256(v, abs_m);
        __m256i is_nan = _mm256_cmpgt_epi16(abs_b, inf_v);
        __m256i emsk = _mm256_or_si256(mask_v, is_nan);
        __m256i ri = _mm256_and_si256(v, emsk);
        _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst + i), ri);
    }
    for (; i < n; ++i)
        dst[i] = processOne(src[i], mask);
}

X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE
static void processRangeF64_v4(const Float64 * __restrict src, Float64 * __restrict dst, UInt64 mask, size_t n)
{
    constexpr size_t W = 8;
    const __m512i mask_v = _mm512_set1_epi64(static_cast<int64_t>(mask));
    size_t i = 0;
    for (; i + W <= n; i += W)
    {
        __m512d v = _mm512_loadu_pd(src + i);
        __m512i vi = _mm512_castpd_si512(v);
        __mmask8 not_nan = _mm512_cmp_pd_mask(v, v, _CMP_ORD_Q);
        __m512i ri = _mm512_mask_and_epi64(vi, not_nan, vi, mask_v);
        _mm512_storeu_pd(dst + i, _mm512_castsi512_pd(ri));
    }
    for (; i < n; ++i)
        dst[i] = processOne(src[i], mask);
}

X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE
static void processRangeF32_v4(const Float32 * __restrict src, Float32 * __restrict dst, UInt32 mask, size_t n)
{
    constexpr size_t W = 16;
    const __m512i mask_v = _mm512_set1_epi32(static_cast<int32_t>(mask));
    size_t i = 0;
    for (; i + W <= n; i += W)
    {
        __m512 v = _mm512_loadu_ps(src + i);
        __m512i vi = _mm512_castps_si512(v);
        __mmask16 not_nan = _mm512_cmp_ps_mask(v, v, _CMP_ORD_Q);
        __m512i ri = _mm512_mask_and_epi32(vi, not_nan, vi, mask_v);
        _mm512_storeu_ps(dst + i, _mm512_castsi512_ps(ri));
    }
    for (; i < n; ++i)
        dst[i] = processOne(src[i], mask);
}

X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE
static void processRangeBF16_v4(const BFloat16 * __restrict src, BFloat16 * __restrict dst, UInt16 mask, size_t n)
{
    using Traits = FloatTraits<BFloat16>;
    constexpr size_t W = 32;
    const __m512i mask_v = _mm512_set1_epi16(static_cast<int16_t>(mask));
    const __m512i abs_m = _mm512_set1_epi16(static_cast<int16_t>(Traits::abs_mask));
    const __m512i inf_v = _mm512_set1_epi16(static_cast<int16_t>(Traits::inf_bits));
    size_t i = 0;
    for (; i + W <= n; i += W)
    {
        __m512i v = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(src + i));
        __m512i abs_b = _mm512_and_si512(v, abs_m);
        __mmask32 is_nan = _mm512_cmpgt_epi16_mask(abs_b, inf_v);
        __m512i ri = _mm512_and_si512(v, mask_v);
        ri = _mm512_mask_mov_epi16(ri, is_nan, v);
        _mm512_storeu_si512(reinterpret_cast<__m512i *>(dst + i), ri);
    }
    for (; i < n; ++i)
        dst[i] = processOne(src[i], mask);
}

#endif

template <typename Float, typename UInt>
static void processRange(const Float * src, Float * dst, UInt mask, size_t n)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        if constexpr (std::is_same_v<Float, Float64>)
        {
            processRangeF64_v4(src, dst, mask, n);
            return;
        }
        else if constexpr (std::is_same_v<Float, Float32>)
        {
            processRangeF32_v4(src, dst, mask, n);
            return;
        }
        else if constexpr (std::is_same_v<Float, BFloat16>)
        {
            processRangeBF16_v4(src, dst, mask, n);
            return;
        }
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        if constexpr (std::is_same_v<Float, Float64>)
        {
            processRangeF64_v3(src, dst, mask, n);
            return;
        }
        else if constexpr (std::is_same_v<Float, Float32>)
        {
            processRangeF32_v3(src, dst, mask, n);
            return;
        }
        else if constexpr (std::is_same_v<Float, BFloat16>)
        {
            processRangeBF16_v3(src, dst, mask, n);
            return;
        }
    }
#endif
    processRangeScalar(src, dst, mask, n);
}

class FunctionFloatBitTrim : public IFunction
{
public:
    static constexpr auto name = "floatBitTrim";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFloatBitTrim>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isFloat(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of {} must be BFloat16, Float32 or Float64, got {}",
                getName(),
                arguments[0]->getName());
        if (!isNativeInteger(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of {} must be a native integer, got {}",
                getName(),
                arguments[1]->getName());
        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        WhichDataType which(arguments[0].type);
        if (which.isFloat32())
            return executeForType<Float32, UInt32, 23>(arguments, input_rows_count);
        if (which.isFloat64())
            return executeForType<Float64, UInt64, 52>(arguments, input_rows_count);
        if (which.isBFloat16())
            return executeForType<BFloat16, UInt16, 7>(arguments, input_rows_count);
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected type for {}", getName());
    }

private:
    template <typename MaskType, size_t MantissaBits, typename BitsToTrimType>
    static MaskType getMask(BitsToTrimType n_raw)
    {
        if constexpr (std::is_signed_v<BitsToTrimType>)
        {
            if (n_raw < 0) [[unlikely]]
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Number of bits to trim in {} must be non-negative", name);
        }
        /// Clamp at MantissaBits
        const UInt64 n = std::min<UInt64>(static_cast<UInt64>(n_raw), MantissaBits);
        /// n is clamped to MantissaBits (52|23|7)), so shift is defined
        return static_cast<MaskType>(~((static_cast<MaskType>(1) << n) - 1));
    }

    template <typename Float, typename MaskType, size_t MantissaBits>
    static ColumnPtr executeForType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
    {
        auto values_col = arguments[0].column->convertToFullColumnIfConst();
        const auto & bits_col_ptr = arguments[1].column;

        const auto & values = assert_cast<const ColumnVector<Float> &>(*values_col).getData();

        auto result = ColumnVector<Float>::create(input_rows_count);
        auto & result_data = result->getData();

        if (isColumnConst(*bits_col_ptr))
        {
            const auto mask = getMask<MaskType, MantissaBits>(bits_col_ptr->getInt(0));
            processRange(values.data(), result_data.data(), mask, input_rows_count);
            return result;
        }

        WhichDataType bits_which(arguments[1].type);
        if (bits_which.isUInt8())
            runVariable<Float, MaskType, MantissaBits, UInt8>(values, *bits_col_ptr, result_data, input_rows_count);
        else if (bits_which.isUInt16())
            runVariable<Float, MaskType, MantissaBits, UInt16>(values, *bits_col_ptr, result_data, input_rows_count);
        else if (bits_which.isUInt32())
            runVariable<Float, MaskType, MantissaBits, UInt32>(values, *bits_col_ptr, result_data, input_rows_count);
        else if (bits_which.isUInt64())
            runVariable<Float, MaskType, MantissaBits, UInt64>(values, *bits_col_ptr, result_data, input_rows_count);
        else if (bits_which.isInt8())
            runVariable<Float, MaskType, MantissaBits, Int8>(values, *bits_col_ptr, result_data, input_rows_count);
        else if (bits_which.isInt16())
            runVariable<Float, MaskType, MantissaBits, Int16>(values, *bits_col_ptr, result_data, input_rows_count);
        else if (bits_which.isInt32())
            runVariable<Float, MaskType, MantissaBits, Int32>(values, *bits_col_ptr, result_data, input_rows_count);
        else if (bits_which.isInt64())
            runVariable<Float, MaskType, MantissaBits, Int64>(values, *bits_col_ptr, result_data, input_rows_count);
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected integer type for second argument of {}", name);

        return result;
    }

    template <typename Float, typename MaskType, size_t MantissaBits, typename BitsType>
    static void runVariable(
        const PaddedPODArray<Float> & values, const IColumn & bits_col, PaddedPODArray<Float> & result_data, size_t input_rows_count)
    {
        const auto & bits_data = assert_cast<const ColumnVector<BitsType> &>(bits_col).getData();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto mask = getMask<MaskType, MantissaBits>(bits_data[i]);
            result_data[i] = processOne(values[i], mask);
        }
    }
};

}

REGISTER_FUNCTION(FloatBitTrim)
{
    FunctionDocumentation::Description description = R"(
Zeroes the lowest `n` bits of the IEEE 754 mantissa of a floating-point value.
This is a lossy precision reduction useful for improving compression of float columns.
The exponent and sign are preserved; `n` is clamped to the mantissa width
(7 for `BFloat16`, 23 for `Float32`, 52 for `Float64`).
)";
    FunctionDocumentation::Syntax syntax = "floatBitTrim(value, n)";
    FunctionDocumentation::Arguments arguments
        = {{"value", "Floating-point value to trim.", {"BFloat16", "Float32", "Float64"}},
           {"n", "Number of low mantissa bits to zero.", {"(U)Int*"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns `value` with the lowest `n` mantissa bits zeroed, of the same type as `value`.", {"BFloat16", "Float32", "Float64"}};
    FunctionDocumentation::Examples examples = {{"Trim 20 mantissa bits", "SELECT floatBitTrim(1.234::Float64, 20)", "1.2339999999385327"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFloatBitTrim>(documentation);
}

}
