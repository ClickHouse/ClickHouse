#include "FunctionApproximateL2Distance.h"
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include "Columns/ColumnsNumber.h"
#include "Functions/FunctionDequantize.h"

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
}

DataTypePtr FunctionApproximateL2Distance::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (!checkDataTypes<DataTypeFixedString>(arguments[0].get()) || !checkDataTypes<DataTypeFixedString>(arguments[1].get()))
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of function {} must be FixedString", getName());
    }

    if (!WhichDataType(arguments[2]).isUInt8())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Third argument of function {} must be UInt8", getName());

    return std::make_shared<DataTypeFloat64>();
}

ColumnPtr
FunctionApproximateL2Distance::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    const auto * col_left = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get());
    const auto * col_right = checkAndGetColumn<ColumnFixedString>(arguments[1].column.get());

    if (!col_left || !col_right)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of function {} must be FixedString", getName());

    if (col_left->getN() != col_right->getN())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "FixedStrings in function {} must have the same length", getName());

    const auto & data_left = col_left->getChars();
    const auto & data_right = col_right->getChars();
    size_t fixed_string_length = col_left->getN();

    UInt8 bit_width = 0;
    if (const auto * const_col = checkAndGetColumnConst<ColumnUInt8>(arguments[2].column.get()))
        bit_width = const_col->getValue<UInt8>();
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Third argument of function {} must be a constant UInt8", getName());

    if (!(bit_width == 16 || bit_width == 8 || bit_width == 4 || bit_width == 2 || bit_width == 1))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bit width must be one of 16, 8, 4, 2, or 1");

    auto result_column = ColumnFloat64::create();
    auto & result_data = result_column->getData();
    result_data.resize(input_rows_count);

    size_t total_bits_per_row = fixed_string_length * 8;

    for (size_t row = 0; row < input_rows_count; ++row)
    {
        size_t row_offset = row * fixed_string_length;

        size_t num_elements = total_bits_per_row / bit_width;

        size_t bits_read = 0;

        double sum = 0.0;

        for (size_t idx = 0; idx < num_elements; ++idx)
        {
            UInt32 value_left = 0;
            UInt32 value_right = 0;
            for (size_t bit = 0; bit < bit_width; ++bit)
            {
                size_t bit_pos = bits_read++;
                size_t byte_pos = bit_pos / 8;
                size_t bit_in_byte = bit_pos % 8;

                UInt8 byte_left = data_left[row_offset + byte_pos];
                UInt8 byte_right = data_right[row_offset + byte_pos];

                if (byte_left & (1 << (7 - bit_in_byte)))
                    value_left |= (1 << (bit_width - 1 - bit));

                if (byte_right & (1 << (7 - bit_in_byte)))
                    value_right |= (1 << (bit_width - 1 - bit));
            }

            Float32 left_value = 0.0f;
            Float32 right_value = 0.0f;

            if (bit_width == 16)
            {
                left_value = uint16ToFloat32(static_cast<UInt16>(value_left));
                right_value = uint16ToFloat32(static_cast<UInt16>(value_right));
            }
            else
            {
                left_value = dequantizeFromBits(value_left, bit_width);
                right_value = dequantizeFromBits(value_right, bit_width);
            }

            double diff = static_cast<double>(left_value) - static_cast<double>(right_value);
            sum += diff * diff;
        }

        double distance = std::sqrt(sum);
        result_data[row] = distance;
    }

    return result_column;
}

}
