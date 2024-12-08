#include "FunctionQuantize.h"
#include <cmath>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include "Columns/ColumnsNumber.h"
#include "Functions/FunctionApproximateL2Distance.h"
#include "Functions/FunctionDequantize.h"

namespace DB
{

REGISTER_FUNCTION(Quantize)
{
    factory.registerFunction<FunctionQuantize>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionDequantize>({}, FunctionFactory::Case::Sensitive);
    factory.registerFunction<FunctionApproximateL2Distance>({}, FunctionFactory::Case::Insensitive);
}

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
}

UInt32 quantizeToBits(Float64 value, UInt8 bit_width)
{
    constexpr Float64 min_value = -1.0;
    constexpr Float64 max_value = 1.0;

    value = std::max(value, min_value);
    value = std::min(value, max_value);

    Float64 normalized = (value - min_value) / (max_value - min_value);

    UInt32 max_int = (1U << bit_width) - 1;
    UInt32 quantized = static_cast<UInt32>(std::round(normalized * max_int));

    return quantized;
}

UInt16 float32ToUInt16(Float32 value)
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

DataTypePtr FunctionQuantize::getReturnTypeImpl(const DataTypes & arguments) const
{
    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument must be an array in function {}", getName());

    const IDataType * nested_type = array_type->getNestedType().get();
    if (!WhichDataType(nested_type).isFloat())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Array elements must be Float32 or Float64 in function {}", getName());

    if (!WhichDataType(arguments[1]).isUInt8())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be UInt8", getName());

    return std::make_shared<DataTypeFixedString>(0);
}


ColumnPtr FunctionQuantize::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    const auto & array_col_with_type = arguments[0];
    const auto & array_col = array_col_with_type.column;

    const auto * col_array = checkAndGetColumn<ColumnArray>(array_col.get());
    if (!col_array)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be an array", getName());

    const auto & data_col = col_array->getData();
    const auto & offsets = col_array->getOffsets();

    const PaddedPODArray<Float32> * float32_data = nullptr;
    const PaddedPODArray<Float64> * float64_data = nullptr;
    bool is_float32 = false;

    if (const auto * col_float32 = checkAndGetColumn<ColumnFloat32>(&data_col))
    {
        float32_data = &col_float32->getData();
        is_float32 = true;
    }
    else if (const auto * col_float64 = checkAndGetColumn<ColumnFloat64>(&data_col))
    {
        float64_data = &col_float64->getData();
        is_float32 = false;
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Array elements in function {} must be Float32 or Float64", getName());
    }

    UInt8 bit_width = 0;
    if (const auto * const_col = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get()))
        bit_width = const_col->getValue<UInt8>();
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of function {} must be a constant UInt8", getName());

    if (!(bit_width == 16 || bit_width == 8 || bit_width == 4 || bit_width == 2 || bit_width == 1))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bit width must be one of 16, 8, 4, 2, or 1 in function {}", getName());

    size_t num_elements_per_row = 0;
    if (input_rows_count > 0)
    {
        num_elements_per_row = offsets[0];
    }

    for (size_t row = 1; row < input_rows_count; ++row)
    {
        size_t num_elements = offsets[row] - offsets[row - 1];
        if (num_elements != num_elements_per_row)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All input arrays must have the same number of elements in function {}", getName());
        }
    }

    size_t total_bits = num_elements_per_row * bit_width;
    size_t fixed_string_length = (total_bits + 7) / 8;

    auto result_column = ColumnFixedString::create(fixed_string_length);
    auto & result_chars = result_column->getChars();
    result_chars.resize(input_rows_count * fixed_string_length);

    size_t prev_offset = 0;

    for (size_t row = 0; row < input_rows_count; ++row)
    {
        size_t current_offset = offsets[row];
        size_t offset_in_result = row * fixed_string_length;

        size_t bits_written = 0;
        UInt8 current_byte = 0;

        for (size_t idx = prev_offset; idx < current_offset; ++idx)
        {
            UInt32 quantized_value = 0;
            if (bit_width == 16)
            {
                Float32 value;
                if (is_float32)
                {
                    value = (*float32_data)[idx];
                }
                else
                {
                    value = static_cast<Float32>((*float64_data)[idx]);
                }
                quantized_value = float32ToUInt16(value);
            }
            else
            {
                Float64 value;
                if (is_float32)
                {
                    value = static_cast<Float64>((*float32_data)[idx]);
                }
                else
                {
                    value = (*float64_data)[idx];
                }
                quantized_value = quantizeToBits(value, bit_width);
            }

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
        {
            result_chars[offset_in_result++] = current_byte;
        }

        prev_offset = current_offset;
    }

    return result_column;
}


}
