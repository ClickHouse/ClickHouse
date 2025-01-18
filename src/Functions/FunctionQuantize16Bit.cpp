#include "FunctionQuantize16Bit.h"
#include <immintrin.h>
#include "Functions/FunctionHelpers.h"

DECLARE_AVX512BF16_SPECIFIC_CODE(

    void quantize16BitSIMD(const float * input, UInt8 * output, size_t size) {
        size_t i = 0;

        for (; i + 8 <= size; i += 8)
        {
            __m256 input_vec = _mm256_loadu_ps(input + i);
            __m128i fp16 = _mm256_cvtps_ph(input_vec, 0);
            _mm_storeu_si128(reinterpret_cast<__m128i *>(output + i * 2), fp16);
        }

        for (; i < size; ++i)
        {
            __m128 xmm = _mm_load_ss(input + i);
            __m128i fp16 = _mm_cvtps_ph(xmm, 0);
            uint16_t half = static_cast<uint16_t>(_mm_extract_epi16(fp16, 0));

            output[i * 2] = half & 0xFF;
            output[i * 2 + 1] = (half >> 8) & 0xFF;
        }
    }

    void quantize16BitSIMD(const double * input, UInt8 * output, size_t size) {
        size_t i = 0;

        for (; i + 4 <= size; i += 4)
        {
            __m256d input_vec = _mm256_loadu_pd(input + i);
            __m128 singles = _mm256_cvtpd_ps(input_vec);
            __m128i fp16 = _mm_cvtps_ph(singles, 0);
            _mm_storeu_si128(reinterpret_cast<__m128i *>(output + i * 2), fp16);
        }

        for (; i < size; ++i)
        {
            __m128 single = _mm_cvtsd_ss(_mm_setzero_ps(), _mm_load_sd(input + i));
            __m128i fp16 = _mm_cvtps_ph(single, 0);
            uint16_t half = static_cast<uint16_t>(_mm_extract_epi16(fp16, 0));

            output[i * 2] = half & 0xFF;
            output[i * 2 + 1] = (half >> 8) & 0xFF;
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


template <typename T>
void FunctionQuantize16Bit::processArray(
    const PaddedPODArray<T> & src_data, size_t array_size, PaddedPODArray<UInt8> & result_chars, size_t row, size_t fixed_string_length)
{
    size_t offset_in_result = row * fixed_string_length;
    ::TargetSpecific::AVX512BF16::quantize16BitSIMD(src_data.data(), result_chars.data() + offset_in_result, array_size);
}

DataTypePtr FunctionQuantize16Bit::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 1.",
            getName(),
            arguments.size());

    const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
    if (!array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must be an array", getName());

    const auto * nested_type = array_type->getNestedType().get();
    if (!isFloat(nested_type->getTypeId()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Array elements must be Float32 or Float64");

    return std::make_shared<DataTypeFixedString>(2);
}

ColumnPtr FunctionQuantize16Bit::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    const ColumnArray * col_array = nullptr;
    if (const auto * col_const = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get()))
        col_array = checkAndGetColumn<ColumnArray>(col_const->getDataColumnPtr().get());
    else
        col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be an array", getName());

    const auto & offsets = col_array->getOffsets();
    if (offsets.empty())
        return ColumnFixedString::create(0);

    size_t array_size = offsets[0];
    for (size_t i = 1; i < offsets.size(); ++i)
    {
        if (offsets[i] - offsets[i - 1] != array_size)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "All arrays must have the same size");
    }

    size_t fixed_string_length = array_size * 2;
    auto result_column = ColumnFixedString::create(fixed_string_length);
    auto & result_chars = result_column->getChars();
    result_chars.resize_fill(input_rows_count * fixed_string_length);

    const auto & array_data = col_array->getData();
    if (const auto * col_float32 = checkAndGetColumn<ColumnFloat32>(&array_data))
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            processArray(col_float32->getData(), array_size, result_chars, i, fixed_string_length);
    }
    else if (const auto * col_float64 = checkAndGetColumn<ColumnFloat64>(&array_data))
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            processArray(col_float64->getData(), array_size, result_chars, i, fixed_string_length);
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected array element type");

    return result_column;
}

template void
FunctionQuantize16Bit::processArray<Float32>(const PaddedPODArray<Float32> &, size_t, PaddedPODArray<UInt8> &, size_t, size_t);
template void
FunctionQuantize16Bit::processArray<Float64>(const PaddedPODArray<Float64> &, size_t, PaddedPODArray<UInt8> &, size_t, size_t);

}

namespace DB
{
REGISTER_FUNCTION(Quantize16Bit)
{
    factory.registerFunction<FunctionQuantize16Bit>();
}
}
