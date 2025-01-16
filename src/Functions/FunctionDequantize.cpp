#include "FunctionDequantize.h"
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include "Columns/ColumnsNumber.h"

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
}

Float32 dequantizeFromBits(UInt32 quantized_value, UInt8 bit_width)
{
    UInt32 max_int = (1 << bit_width) - 1;
    Float32 normalized = static_cast<Float32>(quantized_value) / static_cast<Float32>(max_int);
    Float32 value = normalized * 2.0f - 1.0f; 
    return value;
}

Float32 uint16ToFloat32(UInt16 h)
{
    UInt32 sign = (h >> 15) & 0x1;
    UInt32 exponent = (h >> 10) & 0x1F;
    UInt32 mantissa = h & 0x3FF;

    UInt32 f_sign = sign << 31;
    UInt32 f_exponent;
    UInt32 f_mantissa;

    if (exponent == 0)
    {
        if (mantissa == 0)
        {
            f_exponent = 0;
            f_mantissa = 0;
        }
        else
        {
            exponent = 1;
            while ((mantissa & 0x400) == 0)
            {
                mantissa <<= 1;
                exponent -= 1;
            }
            mantissa &= 0x3FF;
            exponent += 127 - 15;
            f_exponent = exponent << 23;
            f_mantissa = mantissa << 13;
        }
    }
    else if (exponent == 0x1F)
    {
        f_exponent = 0xFF << 23;
        f_mantissa = mantissa << 13;
    }
    else
    {
        exponent += 127 - 15;
        f_exponent = exponent << 23;
        f_mantissa = mantissa << 13;
    }

    UInt32 f = f_sign | f_exponent | f_mantissa;

    union
    {
        UInt32 u;
        Float32 f;
    } val = {f};

    return val.f;
}

DataTypePtr FunctionDequantize::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (!checkDataTypes<DataTypeFixedString>(arguments[0].get()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be FixedString", getName());

    if (!WhichDataType(arguments[1]).isUInt8())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be UInt8", getName());

    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
}

ColumnPtr FunctionDequantize::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    const auto & fixed_string_col = arguments[0].column;
    const auto * col_fixed_string = checkAndGetColumn<ColumnFixedString>(fixed_string_col.get());
    if (!col_fixed_string)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be FixedString", getName());

    const auto & data = col_fixed_string->getChars();
    size_t fixed_string_length = col_fixed_string->getN();

    UInt8 bit_width = 0;
    if (const auto * const_col = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get()))
        bit_width = const_col->getValue<UInt8>();
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be a constant UInt8", getName());

    if (!(bit_width == 16 || bit_width == 8 || bit_width == 4 || bit_width == 2 || bit_width == 1))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bit width must be one of 16, 8, 4, 2, or 1");

    size_t total_bits_per_row = fixed_string_length * 8;
    if (total_bits_per_row % bit_width != 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Total bits per row is not divisible by bit width in function {}", getName());
    }
    size_t num_elements = total_bits_per_row / bit_width;

    auto col_float = ColumnFloat32::create();
    auto & float_data = col_float->getData();
    float_data.reserve(input_rows_count * num_elements);

    auto col_array = ColumnArray::create(std::move(col_float));
    auto & offsets = col_array->getOffsets();
    offsets.resize(input_rows_count);

    size_t offset = 0;

    for (size_t row = 0; row < input_rows_count; ++row)
    {
        size_t row_offset = row * fixed_string_length;

        size_t bits_read = 0;

        for (size_t idx = 0; idx < num_elements; ++idx)
        {
            UInt32 quantized_value = 0;
            for (size_t bit = 0; bit < bit_width; ++bit)
            {
                size_t bit_pos = bits_read++;
                size_t byte_pos = bit_pos / 8;
                size_t bit_in_byte = bit_pos % 8;

                UInt8 current_byte = data[row_offset + byte_pos];

                if (current_byte & (1 << (7 - bit_in_byte)))
                    quantized_value |= (1 << (bit_width - 1 - bit));
            }

            Float32 value = 0.0f;
            if (bit_width == 16)
            {
                value = uint16ToFloat32(static_cast<UInt16>(quantized_value));
            }
            else
            {
                value = dequantizeFromBits(quantized_value, bit_width);
            }

            float_data.emplace_back(value);
        }

        offset += num_elements;
        offsets[row] = offset;
    }

    return col_array;
}

}
