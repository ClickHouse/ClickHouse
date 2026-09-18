#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <base/BFloat16.h>
#include <base/DecomposedFloat.h>
#include <base/bit_cast.h>
#include <base/defines.h>
#include <Common/TargetSpecific.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int LOGICAL_ERROR;
}

namespace
{

template <typename Float>
struct TrimTraits
{
    using Traits = FloatTraits<Float>;
    using UInt = typename Traits::UInt;

    static constexpr size_t mantissa_bits = Traits::mantissa_bits;
    /// Everything but the sign bit.
    static constexpr UInt abs_mask = static_cast<UInt>((UInt{1} << (Traits::bits - 1)) - 1);
    /// Exponent all ones and mantissa zero. With the sign dropped, anything greater is a `NaN`.
    static constexpr UInt inf_bits
        = static_cast<UInt>(((UInt{1} << Traits::exponent_bits) - 1) << Traits::mantissa_bits);
};

template <typename Float>
inline Float processOne(Float v, typename TrimTraits<Float>::UInt mask)
{
    using Traits = TrimTraits<Float>;
    using UInt = typename Traits::UInt;
    const UInt bits = bit_cast<UInt>(v);
    const UInt is_nan = ((bits & Traits::abs_mask) > Traits::inf_bits) ? UInt(~UInt{0}) : UInt{0};
    return bit_cast<Float>(bits & (mask | is_nan));
}

MULTITARGET_FUNCTION_X86_V4(
    MULTITARGET_FUNCTION_HEADER(template <typename Float> void NO_INLINE),
    processRangeImpl,
    MULTITARGET_FUNCTION_BODY((const Float * __restrict src, Float * __restrict dst, typename TrimTraits<Float>::UInt mask, size_t n) {
        for (size_t i = 0; i < n; ++i)
            dst[i] = processOne(src[i], mask);
    }))

template <typename Float>
void processRange(const Float * src, Float * dst, typename TrimTraits<Float>::UInt mask, size_t n)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
        return processRangeImpl_x86_64_v4<Float>(src, dst, mask, n);
#endif
    processRangeImpl<Float>(src, dst, mask, n);
}

class FunctionFloatBitTrim : public IFunction
{
public:
    static constexpr auto name = "floatBitTrim";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFloatBitTrim>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// Clearing the low mantissa bits is `floor(|x| / 2^n) * 2^n` with the sign kept, which is
    /// non-decreasing across the whole sign-magnitude float line, for every `n` including 0.
    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        if (!type.isValueRepresentedByNumber())
            return {};
        /// `NaN` is returned unchanged and is unordered, so a range touching it carries no
        /// information. This mirrors `PositiveMonotonicity`, which `roundToExp2` uses.
        if (isNaNField(left) || isNaNField(right))
            return {};
        /// Not strict: many inputs collapse onto the same output.
        return {.is_monotonic = true};
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
        if (!arguments[1].column || !isColumnConst(*arguments[1].column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of {} must be constant", getName());

        auto result_type = getReturnTypeImpl(DataTypes{arguments[0].type, arguments[1].type});

        if (isNativeInt(arguments[1].type))
        {
            const Int64 n = arguments[1].column->getInt(0);
            if (n < 0)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Second argument of {} must be non-negative, got {}", getName(), n);
        }

        return result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        WhichDataType which(arguments[0].type);
        if (which.isFloat32())
            return executeForType<Float32>(arguments, input_rows_count);
        if (which.isFloat64())
            return executeForType<Float64>(arguments, input_rows_count);
        if (which.isBFloat16())
            return executeForType<BFloat16>(arguments, input_rows_count);
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Function {} got argument of type {}, which its return type check should have rejected",
            getName(),
            arguments[0].type->getName());
    }

private:
    template <typename Float>
    static typename TrimTraits<Float>::UInt getMask(UInt64 n_raw)
    {
        using MaskType = typename TrimTraits<Float>::UInt;
        /// Clamp at the mantissa width (52|23|7)
        const UInt64 n = std::min<UInt64>(n_raw, TrimTraits<Float>::mantissa_bits);
        return static_cast<MaskType>(~((static_cast<MaskType>(1) << n) - 1));
    }

    template <typename Float>
    static ColumnPtr executeForType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
    {
        using MaskType = typename TrimTraits<Float>::UInt;

        const auto & values = assert_cast<const ColumnVector<Float> &>(*arguments[0].column).getData();
        const auto mask = getMask<Float>(arguments[1].column->getUInt(0));

        /// An `n` of 0 keeps every bit, so the argument column is already the result
        if (mask == static_cast<MaskType>(~MaskType{0}))
            return arguments[0].column;

        auto result = ColumnVector<Float>::create(input_rows_count);
        processRange(values.data(), result->getData().data(), mask, input_rows_count);
        return result;
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

`n` must be a constant non-negative integer; a non-constant `n` throws `ILLEGAL_COLUMN` and a
negative `n` throws `ARGUMENT_OUT_OF_BOUND`.
)";
    FunctionDocumentation::Syntax syntax = "floatBitTrim(value, n)";
    FunctionDocumentation::Arguments arguments
        = {{"value", "Floating-point value to trim.", {"BFloat16", "Float32", "Float64"}},
           {"n",
            "Number of low mantissa bits to zero. Must be a non-negative constant.",
            {"UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns `value` with the lowest `n` mantissa bits zeroed, of the same type as `value`.", {"BFloat16", "Float32", "Float64"}};
    FunctionDocumentation::Examples examples = {{"Trim 20 mantissa bits", "SELECT floatBitTrim(1.234::Float64, 20)", "1.2339999999385327"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Rounding;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFloatBitTrim>(documentation);
}

}
