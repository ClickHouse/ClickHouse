#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <base/BFloat16.h>
#include <base/bit_cast.h>
#include <base/defines.h>
#include <Common/TargetSpecific.h>

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

MULTITARGET_FUNCTION_X86_V4(
    MULTITARGET_FUNCTION_HEADER(template <typename Float, typename UInt> void NO_INLINE),
    processRangeImpl,
    MULTITARGET_FUNCTION_BODY((const Float * __restrict src, Float * __restrict dst, UInt mask, size_t n) {
        for (size_t i = 0; i < n; ++i)
            dst[i] = processOne(src[i], mask);
    }))

template <typename Float, typename UInt>
void processRange(const Float * src, Float * dst, UInt mask, size_t n)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
        return processRangeImpl_x86_64_v4<Float, UInt>(src, dst, mask, n);
#endif
    processRangeImpl<Float, UInt>(src, dst, mask, n);
}

class FunctionFloatBitTrim : public IFunction
{
public:
    static constexpr auto name = "floatBitTrim";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFloatBitTrim>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return !arguments[1].is_const;
    }

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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto result_type = getReturnTypeImpl(DataTypes{arguments[0].type, arguments[1].type});

        const auto & bits_column = arguments[1].column;
        if (bits_column && isColumnConst(*bits_column) && !WhichDataType(arguments[1].type).isNativeUInt() && bits_column->getInt(0) < 0)
            throw Exception(
                ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Number of bits to trim in {} must be non-negative, got {}",
                getName(),
                bits_column->getInt(0));

        return result_type;
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
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Number of bits to trim in {} must be non-negative, got {}",
                    name,
                    static_cast<Int64>(n_raw));
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

        WhichDataType bits_which(arguments[1].type);

        if (isColumnConst(*bits_col_ptr))
        {
            /// `getInt` wraps a `UInt64` above 2^63 into a negative value, which would be rejected as negative.
            const auto mask = bits_which.isNativeUInt() ? getMask<MaskType, MantissaBits>(bits_col_ptr->getUInt(0))
                                                        : getMask<MaskType, MantissaBits>(bits_col_ptr->getInt(0));
            processRange(values.data(), result_data.data(), mask, input_rows_count);
            return result;
        }

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
The value is truncated, not rounded.

Special values:
- `NaN` is returned unchanged, including its sign and its full payload. It never collapses into infinity, and a signaling `NaN` stays signaling.
- Infinity is returned unchanged.
- Subnormal values may become zero, because their significant bits live in the low mantissa bits that are zeroed.

A negative `n` throws `ARGUMENT_OUT_OF_BOUND`.
)";
    FunctionDocumentation::Syntax syntax = "floatBitTrim(value, n)";
    FunctionDocumentation::Arguments arguments
        = {{"value", "Floating-point value to trim.", {"BFloat16", "Float32", "Float64"}},
           {"n", "Number of low mantissa bits to zero.", {"UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns `value` with the lowest `n` mantissa bits zeroed, of the same type as `value`.", {"BFloat16", "Float32", "Float64"}};
    FunctionDocumentation::Examples examples = {{"Trim 20 mantissa bits", "SELECT floatBitTrim(1.234::Float64, 20)", "1.2339999999385327"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Bit;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFloatBitTrim>(documentation);
}

}
