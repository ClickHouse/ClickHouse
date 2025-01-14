#include "FunctionQuantize.h"

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

UInt32 FunctionQuantize::quantizeToBits(Float64 value, UInt8 bit_width)
{
    constexpr Float64 min_value = -1.0;
    constexpr Float64 max_value = 1.0;

    value = std::max(value, min_value);
    value = std::min(value, max_value);

    Float64 normalized = (value - min_value) / (max_value - min_value);
    UInt32 max_int = (1U << bit_width) - 1;
    return static_cast<UInt32>(std::round(normalized * max_int));
}

UInt16 FunctionQuantize::float32ToUInt16(Float32 value)
{
    union
    {
        Float32 f;
        UInt32 u;
    } val;
    val.f = value;

    UInt32 f = val.u;
    UInt16 h = 0;

    UInt32 sign = (f >> 31) & 0x1;
    UInt32 exponent = (f >> 23) & 0xFF;
    UInt32 mantissa = f & 0x7FFFFF;

    if (exponent == 0xFF)
    {
        h = (sign << 15) | (0x1F << 10);
        if (mantissa)
            h |= (mantissa & 0x3FF);
    }
    else if (exponent > 0x70)
    {
        exponent -= 0x70;
        h = (sign << 15) | (exponent << 10) | (mantissa >> 13);
    }
    else
    {
        h = (sign << 15);
    }

    return h;
}

template <typename T>
void FunctionQuantize::processArray(
    const PaddedPODArray<T> & src_data,
    size_t array_size,
    UInt8 bit_width,
    PaddedPODArray<UInt8> & result_chars,
    size_t row,
    size_t fixed_string_length)
{
    size_t offset_in_result = row * fixed_string_length;
    size_t start_idx = row * array_size;

    if (offset_in_result + fixed_string_length > result_chars.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Buffer overflow in quantize function");

    size_t bits_written = 0;
    UInt8 current_byte = 0;

    for (size_t i = 0; i < array_size; ++i)
    {
        UInt32 quantized_value;
        if (bit_width == 16)
            quantized_value = float32ToUInt16(static_cast<Float32>(src_data[start_idx + i]));
        else
            quantized_value = quantizeToBits(static_cast<Float64>(src_data[start_idx + i]), bit_width);

        for (size_t bit = 0; bit < bit_width; ++bit)
        {
            if (quantized_value & (1U << (bit_width - 1 - bit)))
                current_byte |= (1U << (7 - (bits_written % 8)));

            bits_written++;
            if (bits_written % 8 == 0)
            {
                result_chars[offset_in_result++] = current_byte;
                current_byte = 0;
            }
        }
    }

    if (bits_written % 8 != 0)
        result_chars[offset_in_result] = current_byte;
}

DataTypePtr FunctionQuantize::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 2.",
            getName(),
            arguments.size());

    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be an array", getName());

    const auto * nested_type = array_type->getNestedType().get();
    if (!isFloat(nested_type->getTypeId()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Array elements must be Float32 or Float64, got {}", nested_type->getName());
    if (!WhichDataType(arguments[1]).isUInt8())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be UInt8", getName());

    return std::make_shared<DataTypeFixedString>(1);
}

ColumnPtr FunctionQuantize::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    const ColumnArray * col_array = nullptr;
    if (const auto * col_const = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get()))
        col_array = checkAndGetColumn<ColumnArray>(col_const->getDataColumnPtr().get());
    else
        col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be an array", getName());

    UInt8 bit_width = 0;
    if (const auto * const_col = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get()))
        bit_width = const_col->getValue<UInt8>();
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be a constant UInt8", getName());

    if (!(bit_width == 16 || bit_width == 8 || bit_width == 4 || bit_width == 2 || bit_width == 1))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bit width must be one of 16, 8, 4, 2, or 1 in function {}", getName());

    const auto & offsets = col_array->getOffsets();
    if (offsets.empty())
        return ColumnFixedString::create(0);

    size_t array_size = offsets[0];
    for (size_t i = 1; i < offsets.size(); ++i)
    {
        if (offsets[i] - offsets[i - 1] != array_size)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "All arrays must have the same size in function {}", getName());
    }

    size_t total_bits = array_size * bit_width;
    size_t fixed_string_length = (total_bits + 7) / 8;
    if (fixed_string_length == 0)
        fixed_string_length = 1;

    auto result_column = ColumnFixedString::create(fixed_string_length);
    auto & result_chars = result_column->getChars();
    result_chars.resize_fill(input_rows_count * fixed_string_length);

    const auto & array_data = col_array->getData();
    if (const auto * col_float32 = checkAndGetColumn<ColumnFloat32>(&array_data))
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            processArray(col_float32->getData(), array_size, bit_width, result_chars, i, fixed_string_length);
    }
    else if (const auto * col_float64 = checkAndGetColumn<ColumnFloat64>(&array_data))
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            processArray(col_float64->getData(), array_size, bit_width, result_chars, i, fixed_string_length);
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected array element type for function {}", getName());

    return result_column;
}

template void
FunctionQuantize::processArray<Float32>(const PaddedPODArray<Float32> &, size_t, UInt8, PaddedPODArray<UInt8> &, size_t, size_t);
template void
FunctionQuantize::processArray<Float64>(const PaddedPODArray<Float64> &, size_t, UInt8, PaddedPODArray<UInt8> &, size_t, size_t);
}

namespace DB
{
REGISTER_FUNCTION(Quantize)
{
    factory.registerFunction<FunctionQuantize>();
}
}
