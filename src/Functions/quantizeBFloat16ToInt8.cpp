#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/LloydMaxQuantization.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationQBit.h>
#include <Common/FunctionDocumentation.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>

#include <array>
#include <bit>
#include <cstring>

/** quantizeBFloat16ToInt8 / dequantizeInt8ToBFloat16
  *
  * A scalar codec pair for compressing embedding components with a 256-level Gaussian
  * Lloyd-Max quantizer (the MSE-optimal scalar quantizer for a standard-normal source).
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

/// Apply an elementwise codec to every element of an Array column, preserving the offsets.
template <typename SrcColumn, typename DstColumn, typename MapFunc>
ColumnPtr executeOverArray(const ColumnArray & col_array, MapFunc && map)
{
    const auto & src_data = assert_cast<const SrcColumn &>(col_array.getData()).getData();
    const size_t elements_count = src_data.size();

    auto res_data_column = DstColumn::create(elements_count);
    auto & res_data = res_data_column->getData();
    for (size_t i = 0; i < elements_count; ++i)
        res_data[i] = map(src_data[i]);

    return ColumnArray::create(std::move(res_data_column), col_array.getOffsetsPtr());
}

/// Reconstruct each vector from its bit-transposed QBit planes, apply an elementwise codec to every
/// element, then re-transpose the result into a QBit of the destination element type. Used for both
/// quantize (BFloat16 -> Int8) and dequantize (Int8 -> BFloat16); the two directions differ only in the
/// codec and the element types (and hence the number of bit planes: 16 for BFloat16, 8 for Int8). This
/// mirrors the Array <-> QBit conversion path in FunctionsConversion.
template <typename SrcElem, typename DstElem, typename MapFunc>
ColumnPtr executeOverQBit(const ColumnQBit & col_qbit, size_t dimension, size_t stride, MapFunc && map)
{
    /// The transpose/untranspose kernels operate on an unsigned word of the element's width. The 8-bit word
    /// must be `uint8_t`, not ClickHouse's `UInt8` (`char8_t`), to satisfy the kernels' bit intrinsics.
    using SrcWord = std::conditional_t<sizeof(SrcElem) == 1, uint8_t, UInt16>;
    using DstWord = std::conditional_t<sizeof(DstElem) == 1, uint8_t, UInt16>;

    constexpr size_t src_bits = sizeof(SrcElem) * 8;
    constexpr size_t dst_bits = sizeof(DstElem) * 8;

    const size_t rows = col_qbit.size();
    const ColumnTuple & src_tuple = col_qbit.getNestedData();

    const size_t num_strides = dimension / stride;
    const size_t bytes_per_fixedstring = DataTypeQBit::bitsToBytes(stride);
    const size_t padded_stride = bytes_per_fixedstring * 8;

    const auto untranspose = SerializationQBit::resolveUntransposeBitPlane<SrcWord>();

    /// The destination QBit stores `dst_bits` bit planes per stride group, grouped as [group][plane].
    const size_t num_dst_columns = dst_bits * num_strides;
    MutableColumns dst_columns(num_dst_columns);
    for (size_t i = 0; i < num_dst_columns; ++i)
    {
        auto column = ColumnFixedString::create(bytes_per_fixedstring);
        column->reserve(rows);
        dst_columns[i] = std::move(column);
    }

    /// Reusable scratch buffer for one stride group's reconstructed elements. The untranspose kernel ORs
    /// bits in, so it must be zeroed before each group.
    VectorWithMemoryTracking<SrcElem> reconstructed(padded_stride);

    for (size_t row = 0; row < rows; ++row)
    {
        /// Append a default (zero) FixedString for this row to every destination plane and remember where
        /// to transpose into. Padding elements (only present when stride is not a multiple of 8, i.e. the
        /// non-strided case) stay zero, matching the Array -> QBit conversion path.
        VectorWithMemoryTracking<char *> row_ptrs(num_dst_columns);
        for (size_t j = 0; j < num_dst_columns; ++j)
        {
            auto & fixed_string_column = assert_cast<ColumnFixedString &>(*dst_columns[j]);
            fixed_string_column.insertDefault();
            auto & chars = fixed_string_column.getChars();
            row_ptrs[j] = reinterpret_cast<char *>(&chars[chars.size() - bytes_per_fixedstring]);
        }

        for (size_t group = 0; group < num_strides; ++group)
        {
            /// The value 0 has an all-zero bit pattern for BFloat16 and Int8, matching the zero word the kernel ORs into.
            std::memset(reconstructed.data(), 0, reconstructed.size() * sizeof(SrcElem));

            /// Untranspose stride group `group`'s bit planes (tuple columns [group * src_bits, group * src_bits + src_bits)).
            for (size_t bit = 0; bit < src_bits; ++bit)
            {
                const auto & fixed_string_column = assert_cast<const ColumnFixedString &>(src_tuple.getColumn(group * src_bits + bit));
                const UInt8 * src = reinterpret_cast<const UInt8 *>(fixed_string_column.getChars().data()) + row * bytes_per_fixedstring;
                const SrcWord mask = static_cast<SrcWord>(SrcWord(1) << (src_bits - 1 - bit));
                untranspose(src, reinterpret_cast<SrcWord *>(reconstructed.data()), padded_stride, mask);
            }

            /// Apply the codec to each real dimension and transpose it into this group's destination planes.
            for (size_t i = 0; i < stride; ++i)
            {
                DstElem mapped = map(reconstructed[i]);
                DstWord w = 0;
                std::memcpy(&w, &mapped, sizeof(DstElem));
                SerializationQBit::transposeBits<DstWord>(w, i, padded_stride, row_ptrs.data() + group * dst_bits);
            }
        }
    }

    ColumnPtr dst_tuple = ColumnTuple::create(std::move(dst_columns));
    return ColumnQBit::create(dst_tuple, dimension, stride);
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
        const DataTypePtr & arg = arguments[0];

        if (WhichDataType(arg).isBFloat16())
            return std::make_shared<DataTypeInt8>();

        if (const auto * array_type = checkAndGetDataType<DataTypeArray>(arg.get()))
        {
            if (WhichDataType(array_type->getNestedType()).isBFloat16())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt8>());
        }
        else if (const auto * qbit_type = checkAndGetDataType<DataTypeQBit>(arg.get()))
        {
            if (WhichDataType(qbit_type->getElementType()).isBFloat16())
                return std::make_shared<DataTypeQBit>(
                    std::make_shared<DataTypeInt8>(), qbit_type->getDimension(), qbit_type->getStride());
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} must be BFloat16, Array(BFloat16) or QBit(BFloat16, ...), got {}", getName(), arg->getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * col = arguments[0].column.get();
        const std::array<Int8, 65536> & lut = LloydMax::quantizeCodeLUT();
        auto quantize = [&lut](BFloat16 v) -> Int8 { return lut[std::bit_cast<UInt16>(v)]; };

        if (const auto * col_bfloat16 = checkAndGetColumn<ColumnBFloat16>(col))
        {
            auto col_res = ColumnInt8::create(input_rows_count);
            const auto & src = col_bfloat16->getData();
            auto & dst = col_res->getData();
            for (size_t i = 0; i < input_rows_count; ++i)
                dst[i] = quantize(src[i]);
            return col_res;
        }

        if (const auto * col_array = checkAndGetColumn<ColumnArray>(col))
            return executeOverArray<ColumnBFloat16, ColumnInt8>(*col_array, quantize);

        if (const auto * col_qbit = checkAndGetColumn<ColumnQBit>(col))
            return executeOverQBit<BFloat16, Int8>(*col_qbit, col_qbit->getDimension(), col_qbit->getStride(), quantize);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} has unexpected type {}", getName(), arguments[0].type->getName());
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
        const DataTypePtr & arg = arguments[0];

        if (WhichDataType(arg).isInt8())
            return std::make_shared<DataTypeBFloat16>();

        if (const auto * array_type = checkAndGetDataType<DataTypeArray>(arg.get()))
        {
            if (WhichDataType(array_type->getNestedType()).isInt8())
                return std::make_shared<DataTypeArray>(std::make_shared<DataTypeBFloat16>());
        }
        else if (const auto * qbit_type = checkAndGetDataType<DataTypeQBit>(arg.get()))
        {
            if (WhichDataType(qbit_type->getElementType()).isInt8())
                return std::make_shared<DataTypeQBit>(
                    std::make_shared<DataTypeBFloat16>(), qbit_type->getDimension(), qbit_type->getStride());
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} must be Int8, Array(Int8) or QBit(Int8, ...), got {}", getName(), arg->getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * col = arguments[0].column.get();
        const std::array<BFloat16, 256> & levels = LloydMax::dequantizeLevels();
        /// Direct copy of the precomputed BFloat16 level -- no Float32 conversion at runtime.
        auto dequantize = [&levels](Int8 code) -> BFloat16 { return levels[static_cast<size_t>(static_cast<Int16>(code) + 128)]; };

        if (const auto * col_int8 = checkAndGetColumn<ColumnInt8>(col))
        {
            auto col_res = ColumnBFloat16::create(input_rows_count);
            const auto & src = col_int8->getData();
            auto & dst = col_res->getData();
            for (size_t i = 0; i < input_rows_count; ++i)
                dst[i] = dequantize(src[i]);
            return col_res;
        }

        if (const auto * col_array = checkAndGetColumn<ColumnArray>(col))
            return executeOverArray<ColumnInt8, ColumnBFloat16>(*col_array, dequantize);

        if (const auto * col_qbit = checkAndGetColumn<ColumnQBit>(col))
            return executeOverQBit<Int8, BFloat16>(*col_qbit, col_qbit->getDimension(), col_qbit->getStride(), dequantize);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of function {} has unexpected type {}", getName(), arguments[0].type->getName());
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
variance before quantizing.

The argument may be a single `BFloat16`, an `Array(BFloat16)`, or a `QBit(BFloat16, ...)`; the
function is applied to every element and returns `Int8`, `Array(Int8)`, or a `QBit(Int8, ...)` of
the same dimension and stride respectively. Passing a whole vector (`Array` or `QBit`) is preferred
over `arrayMap(x -> quantizeBFloat16ToInt8(x), ...)`.

The result is the index `0..255` of the Lloyd-Max cell, stored as `Int8` as `index - 128`, so
the sign bit equals the sign of the value (`+0`/`-0` map to the positive/negative central cells)
and the top `b` bits form a valid embedded `2^b`-level quantizer: bit-truncation of the code
yields coarser Int4/Int2/binary codes.

Use [`dequantizeInt8ToBFloat16`](#dequantizeInt8ToBFloat16) to reconstruct the value.
)";
        FunctionDocumentation::Syntax syntax = "quantizeBFloat16ToInt8(x)";
        FunctionDocumentation::Arguments arguments = {{"x", "Value(s) to quantize (expected to be ~N(0,1)).", {"BFloat16", "Array(BFloat16)", "QBit(BFloat16)"}}};
        FunctionDocumentation::ReturnedValue returned_value = {"The Lloyd-Max cell index minus 128.", {"Int8", "Array(Int8)", "QBit(Int8)"}};
        FunctionDocumentation::Examples examples = {
            {"Quantize a scalar", "SELECT quantizeBFloat16ToInt8(2.0::BFloat16)", "116"},
            {"Quantize an array", "SELECT quantizeBFloat16ToInt8([0.1, -0.5, 2.0]::Array(BFloat16))", "[10,-49,116]"}};
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

The argument may be a single `Int8`, an `Array(Int8)`, or a `QBit(Int8, ...)`; the function is
applied to every element and returns `BFloat16`, `Array(BFloat16)`, or a `QBit(BFloat16, ...)` of
the same dimension and stride respectively.
)";
        FunctionDocumentation::Syntax syntax = "dequantizeInt8ToBFloat16(x)";
        FunctionDocumentation::Arguments arguments = {{"x", "Code(s) produced by quantizeBFloat16ToInt8.", {"Int8", "Array(Int8)", "QBit(Int8)"}}};
        FunctionDocumentation::ReturnedValue returned_value = {"The reconstruction level of the cell.", {"BFloat16", "Array(BFloat16)", "QBit(BFloat16)"}};
        FunctionDocumentation::Examples examples = {
            {"Round trip", "SELECT round(toFloat32(dequantizeInt8ToBFloat16(quantizeBFloat16ToInt8(0.5::BFloat16))), 4)", "0.4961"}};
        FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::QBit;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionDequantizeInt8ToBFloat16>(documentation);
    }
}

}
