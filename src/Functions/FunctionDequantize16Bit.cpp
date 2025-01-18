#include "FunctionDequantize16Bit.h"
#include <immintrin.h>
#include "Functions/FunctionHelpers.h"

DECLARE_AVX512BF16_SPECIFIC_CODE(

    void dequantize16BitSIMD(const UInt8 * input, float * output, size_t size) {
        size_t i = 0;

        for (; i + 8 <= size; i += 8)
        {
            __m128i fp16 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(input + i * 2));
            __m256 output_vec = _mm256_cvtph_ps(fp16);
            _mm256_storeu_ps(output + i, output_vec);
        }

        for (; i < size; ++i)
        {
            uint16_t half = static_cast<uint16_t>(input[i * 2]) | (static_cast<uint16_t>(input[i * 2 + 1]) << 8);
            __m128i fp16 = _mm_set1_epi16(half);
            __m128 single = _mm_cvtph_ps(fp16);
            _mm_store_ss(output + i, single);
        }
    }

)

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void FunctionDequantize16Bit::dequantize16Bit(const UInt8 * input, float * output, size_t size)
{
    ::TargetSpecific::AVX512BF16::dequantize16BitSIMD(input, output, size);
}

void FunctionDequantize16Bit::processArray(
    const PaddedPODArray<UInt8> & src_data,
    size_t array_size,
    PaddedPODArray<Float32> & result_data,
    size_t row,
    size_t fixed_string_length)
{
    size_t offset_in_result = row * fixed_string_length;
    ::TargetSpecific::AVX512BF16::dequantize16BitSIMD(src_data.data() + offset_in_result, result_data.data(), array_size);
}

DataTypePtr FunctionDequantize16Bit::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 1.",
            getName(),
            arguments.size());

    const DataTypeFixedString * fixed_string_type = typeid_cast<const DataTypeFixedString *>(arguments[0].get());
    if (!fixed_string_type || fixed_string_type->getN() % 2 != 0)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be a FixedString with even length", getName());

    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
}


ColumnPtr FunctionDequantize16Bit::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    const ColumnFixedString * col_fixed_string = nullptr;

    if (const auto * col_const = checkAndGetColumnConst<ColumnFixedString>(arguments[0].column.get()))
    {
        col_fixed_string = checkAndGetColumn<ColumnFixedString>(col_const->getDataColumnPtr().get());
    }
    else
    {
        col_fixed_string = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get());
    }

    if (!col_fixed_string)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a FixedString", getName());

    size_t fixed_string_length = col_fixed_string->getN();
    size_t array_size = fixed_string_length / 2;

    auto result_column = ColumnArray::create(ColumnFloat32::create());
    auto & result_data = typeid_cast<ColumnFloat32 &>(result_column->getData()).getData();
    auto & result_offsets = result_column->getOffsets();

    result_data.resize(input_rows_count * array_size);
    result_offsets.resize(input_rows_count);

    const auto & fixed_string_data = col_fixed_string->getChars();

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        processArray(fixed_string_data, array_size, result_data, i, fixed_string_length);
        result_offsets[i] = (i + 1) * array_size;
    }

    if (isColumnConst(*arguments[0].column))
    {
        return ColumnConst::create(std::move(result_column), input_rows_count);
    }

    return result_column;
}

}

namespace DB
{
REGISTER_FUNCTION(Dequantize16Bit)
{
    factory.registerFunction<FunctionDequantize16Bit>();
}
}
