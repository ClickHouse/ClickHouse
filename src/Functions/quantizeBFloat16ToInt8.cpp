#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Common/FunctionDocumentation.h>
#include <Common/LloydMaxQuantizer.h>

#include <array>
#include <bit>

/** quantizeBFloat16ToInt8 / dequantizeInt8ToBFloat16
  *
  * A scalar codec pair for compressing embedding components with a 256-level Gaussian
  * Lloyd-Max quantizer (the MSE-optimal scalar quantizer for a standard-normal source). The
  * quantizer tables and the scalar quantize/dequantize live in Common/LloydMaxQuantizer.h, shared
  * with the `int8` vector quantization method.
  *
  * It is intended for values that are approximately N(0, 1). A random orthogonal (randomized
  * Hadamard) rotation preserves the norm, so for a d-dimensional unit-norm embedding each rotated
  * coordinate has variance 1/d; multiply the rotated vector by sqrt(d) to reach unit variance
  * before quantizing. Apply over a vector with arrayMap, e.g.
  *     arrayMap(x -> quantizeBFloat16ToInt8(x), rotated_embedding)  -- rotated_embedding ~ N(0, 1)
  *
  * quantizeBFloat16ToInt8 maps a value to the index (0..255) of the Lloyd-Max cell it falls
  * into and stores it as Int8 (index - 128), so the sign bit equals the sign of the value (+0 and
  * -0 map to the positive and negative central cells respectively) and the top b bits form a valid
  * 2^b-level (embedded) quantizer -- truncating bits yields coarser Int4/Int2/binary codes.
  * dequantizeInt8ToBFloat16 maps the code back to the cell's reconstruction level.
  */
namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Direct lookup table keyed by the raw BFloat16 bit pattern. BFloat16 has only 65536 possible
/// values, so the whole quantizer is precomputed once: at runtime quantization is a single load
/// indexed by the bits, with no Float32 conversion or boundary search. Built lazily on first use.
const std::array<Int8, 65536> & quantizeCodeLUT()
{
    static const std::array<Int8, 65536> lut = []
    {
        std::array<Int8, 65536> table{};
        for (UInt32 bits = 0; bits <= 0xFFFFu; ++bits)
            table[bits] = LloydMax::quantize(static_cast<Float32>(BFloat16::fromBits(static_cast<UInt16>(bits))));
        return table;
    }();
    return lut;
}

/// Precomputed BFloat16 reconstruction levels (the bf16 representations of the 256 Lloyd-Max levels).
/// Dequantization is then a direct BFloat16 (i.e. UInt16) copy from this table, with no Float32
/// conversion at runtime. Built lazily on first use.
const std::array<BFloat16, 256> & dequantizeLevels()
{
    static const std::array<BFloat16, 256> levels = []
    {
        std::array<BFloat16, 256> table{};
        for (size_t i = 0; i < 256; ++i)
            table[i] = BFloat16(LloydMax::LEVELS[i]);
        return table;
    }();
    return levels;
}

class FunctionQuantizeBFloat16ToInt8 : public IFunction
{
public:
    static constexpr auto name = "quantizeBFloat16ToInt8";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionQuantizeBFloat16ToInt8>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[0]).isBFloat16())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be BFloat16, got {}", getName(), arguments[0]->getName());
        return std::make_shared<DataTypeInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col = checkAndGetColumn<ColumnBFloat16>(arguments[0].column.get());
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be BFloat16", getName());

        const std::array<Int8, 65536> & lut = quantizeCodeLUT();
        auto col_res = ColumnInt8::create(input_rows_count);
        const auto & src = col->getData();
        auto & dst = col_res->getData();
        for (size_t i = 0; i < input_rows_count; ++i)
            dst[i] = lut[std::bit_cast<UInt16>(src[i])];
        return col_res;
    }
};

class FunctionDequantizeInt8ToBFloat16 : public IFunction
{
public:
    static constexpr auto name = "dequantizeInt8ToBFloat16";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDequantizeInt8ToBFloat16>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[0]).isInt8())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be Int8, got {}", getName(), arguments[0]->getName());
        return std::make_shared<DataTypeBFloat16>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col = checkAndGetColumn<ColumnInt8>(arguments[0].column.get());
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be Int8", getName());

        const std::array<BFloat16, 256> & levels = dequantizeLevels();
        auto col_res = ColumnBFloat16::create(input_rows_count);
        const auto & src = col->getData();
        auto & dst = col_res->getData();
        /// Direct copy of the precomputed BFloat16 level -- no Float32 conversion at runtime.
        for (size_t i = 0; i < input_rows_count; ++i)
            dst[i] = levels[static_cast<size_t>(static_cast<Int16>(src[i]) + 128)];
        return col_res;
    }
};

}

REGISTER_FUNCTION(QuantizeLloydMax)
{
    {
        FunctionDocumentation::Description description = R"(
Quantizes a `BFloat16` value to `Int8` using a 256-level Gaussian Lloyd-Max quantizer
(the MSE-optimal scalar quantizer for a standard-normal source).

Intended for values that are approximately distributed as `N(0, 1)`. A random orthogonal
(randomized Hadamard) rotation preserves the norm, so for a `d`-dimensional unit-norm embedding
each rotated coordinate has variance `1/d`; scale the rotated vector by `sqrt(d)` to reach unit
variance before quantizing. Apply it over a vector with
[`arrayMap`](/sql-reference/functions/array-functions#arrayMap).

The result is the index `0..255` of the Lloyd-Max cell, stored as `Int8` as `index - 128`, so
the sign bit equals the sign of the value (`+0`/`-0` map to the positive/negative central cells)
and the top `b` bits form a valid embedded `2^b`-level quantizer: bit-truncation of the code
yields coarser Int4/Int2/binary codes.

Use [`dequantizeInt8ToBFloat16`](#dequantizeInt8ToBFloat16) to reconstruct the value.
)";
        FunctionDocumentation::Syntax syntax = "quantizeBFloat16ToInt8(x)";
        FunctionDocumentation::Arguments arguments = {{"x", "Value to quantize (expected to be ~N(0,1)).", {"BFloat16"}}};
        FunctionDocumentation::ReturnedValue returned_value = {"The Lloyd-Max cell index minus 128.", {"Int8"}};
        FunctionDocumentation::Examples examples = {
            {"Quantize a vector", "SELECT arrayMap(x -> quantizeBFloat16ToInt8(x), [0.1, -0.5, 2.0]::Array(BFloat16))", "[10,-49,116]"}};
        FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::QBit;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionQuantizeBFloat16ToInt8>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Reconstructs a `BFloat16` value from an `Int8` code produced by
[`quantizeBFloat16ToInt8`](#quantizeBFloat16ToInt8), by mapping the code back to the
reconstruction level of its Gaussian Lloyd-Max cell.
)";
        FunctionDocumentation::Syntax syntax = "dequantizeInt8ToBFloat16(x)";
        FunctionDocumentation::Arguments arguments = {{"x", "Code produced by quantizeBFloat16ToInt8.", {"Int8"}}};
        FunctionDocumentation::ReturnedValue returned_value = {"The reconstruction level of the cell.", {"BFloat16"}};
        FunctionDocumentation::Examples examples = {
            {"Round trip", "SELECT round(toFloat32(dequantizeInt8ToBFloat16(quantizeBFloat16ToInt8(0.5::BFloat16))), 4)", "0.4961"}};
        FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::QBit;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionDequantizeInt8ToBFloat16>(documentation);
    }
}

}
