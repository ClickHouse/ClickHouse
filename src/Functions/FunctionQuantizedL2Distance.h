#pragma once

#include <vector>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include "Common/TargetSpecific.h"
#include "FunctionDequantize16Bit.h"
#ifdef ENABLE_MULTITARGET_CODE
#    include <immintrin.h>
#    include <x86intrin.h>
#endif
#include <Common/CPUID.h>

namespace DB
{

DECLARE_MULTITARGET_CODE(

    struct Quantized8BitL2DistanceImpl {
        template <typename Dequantizer>
        static void execute(const UInt8 * data_a, const UInt8 * data_b, size_t length, float & result);
    };

)

DECLARE_DEFAULT_CODE(

    template <typename Dequantizer>
    void Quantized8BitL2DistanceImpl::execute(const UInt8 * data_a, const UInt8 * data_b, size_t length, float & result) {
        float sum = 0.0f;
        for (size_t i = 0; i < length; ++i)
        {
            float a_val = Dequantizer::dequantize(data_a[i]);
            float b_val = Dequantizer::dequantize(data_b[i]);
            float diff = a_val - b_val;
            sum += diff * diff;
        }
        result = std::sqrt(sum);
    }

)

DECLARE_AVX2_SPECIFIC_CODE(

    template <typename Dequantizer>
    void Quantized8BitL2DistanceImpl::execute(const UInt8 * data_a, const UInt8 * data_b, size_t length, float & result) {
        size_t i = 0;
        __m256 sum_vec = _mm256_setzero_ps();
        for (; i + 8 <= length; i += 8)
        {
            __m128i quant_a = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(data_a + i)); 
            __m128i quant_b = _mm_loadl_epi64(reinterpret_cast<const __m128i *>(data_b + i));

            __m256i qa = _mm256_cvtepu8_epi32(quant_a);
            __m256i qb = _mm256_cvtepu8_epi32(quant_b);

            const __m256i mask7F = _mm256_set1_epi32(0x7F);
            __m256i masked_a = _mm256_and_si256(qa, mask7F);
            __m256i masked_b = _mm256_and_si256(qb, mask7F);

            __m256i zero_mask_a = _mm256_cmpeq_epi32(masked_a, _mm256_setzero_si256());
            __m256i zero_mask_b = _mm256_cmpeq_epi32(masked_b, _mm256_setzero_si256());

            __m256i sign32_a = _mm256_slli_epi32(_mm256_and_si256(qa, _mm256_set1_epi32(0x80)), 24);
            __m256i sign32_b = _mm256_slli_epi32(_mm256_and_si256(qb, _mm256_set1_epi32(0x80)), 24);

            __m256i large_e_a = _mm256_srli_epi32(masked_a, 6);
            __m256i large_e_b = _mm256_srli_epi32(masked_b, 6);

            __m256i m_mask_a = _mm256_or_si256(_mm256_set1_epi32(3), _mm256_slli_epi32(large_e_a, 2));
            __m256i m_mask_b = _mm256_or_si256(_mm256_set1_epi32(3), _mm256_slli_epi32(large_e_b, 2));

            __m256i m_a = _mm256_and_si256(masked_a, m_mask_a);
            __m256i m_b = _mm256_and_si256(masked_b, m_mask_b);

            __m256i m_bits_a = _mm256_add_epi32(_mm256_set1_epi32(2), large_e_a);
            __m256i m_bits_b = _mm256_add_epi32(_mm256_set1_epi32(2), large_e_b);

            __m256i e_a = _mm256_srlv_epi32(masked_a, m_bits_a);
            __m256i e_b = _mm256_srlv_epi32(masked_b, m_bits_b);

            __m256i exp_term_a = _mm256_add_epi32(_mm256_set1_epi32(104), e_a);
            exp_term_a = _mm256_add_epi32(exp_term_a, _mm256_slli_epi32(large_e_a, 3));
            __m256i exp_a = _mm256_slli_epi32(exp_term_a, 23);

            __m256i exp_term_b = _mm256_add_epi32(_mm256_set1_epi32(104), e_b);
            exp_term_b = _mm256_add_epi32(exp_term_b, _mm256_slli_epi32(large_e_b, 3));
            __m256i exp_b = _mm256_slli_epi32(exp_term_b, 23);

            __m256i m_shift_a = _mm256_sub_epi32(_mm256_set1_epi32(21), large_e_a);
            __m256i m_shift_b = _mm256_sub_epi32(_mm256_set1_epi32(21), large_e_b);

            __m256i mnt_a = _mm256_sllv_epi32(m_a, m_shift_a);
            __m256i mnt_b = _mm256_sllv_epi32(m_b, m_shift_b);

            __m256i bin_a = _mm256_or_si256(sign32_a, exp_a);
            bin_a = _mm256_or_si256(bin_a, mnt_a);
            bin_a = _mm256_blendv_epi8(bin_a, _mm256_setzero_si256(), zero_mask_a);

            __m256i bin_b = _mm256_or_si256(sign32_b, exp_b);
            bin_b = _mm256_or_si256(bin_b, mnt_b);
            bin_b = _mm256_blendv_epi8(bin_b, _mm256_setzero_si256(), zero_mask_b);

            __m256 fa = _mm256_castsi256_ps(bin_a);
            __m256 fb = _mm256_castsi256_ps(bin_b);

            __m256 diff = _mm256_sub_ps(fa, fb);
            __m256 sq = _mm256_mul_ps(diff, diff);
            sum_vec = _mm256_add_ps(sum_vec, sq);
        }

        __m128 low = _mm256_castps256_ps128(sum_vec);
        __m128 high = _mm256_extractf128_ps(sum_vec, 1);
        __m128 sum128 = _mm_add_ps(low, high);
        sum128 = _mm_hadd_ps(sum128, sum128);
        sum128 = _mm_hadd_ps(sum128, sum128);
        float sum = _mm_cvtss_f32(sum128);

        for (; i < length; ++i)
        {
            float a_val = Dequantizer::dequantize(data_a[i]);
            float b_val = Dequantizer::dequantize(data_b[i]);
            float diff = a_val - b_val;
            sum += diff * diff;
        }
        result = std::sqrt(sum);
    }

)

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


template <typename QuantizedL2DistanceImpl, typename Dequantizer, const char * function_name>
class FunctionQuantizedL2DistanceImpl : public IFunction
{
public:
    static constexpr auto name = function_name;

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return std::make_shared<DataTypeFloat32>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnFixedString * col_a = nullptr;
        const ColumnFixedString * col_b = nullptr;

        if (const auto * col_const = checkAndGetColumnConst<ColumnFixedString>(arguments[0].column.get()))
            col_a = checkAndGetColumn<ColumnFixedString>(col_const->getDataColumnPtr().get());
        else
            col_a = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get());

        if (const auto * col_const = checkAndGetColumnConst<ColumnFixedString>(arguments[1].column.get()))
            col_b = checkAndGetColumn<ColumnFixedString>(col_const->getDataColumnPtr().get());
        else
            col_b = checkAndGetColumn<ColumnFixedString>(arguments[1].column.get());

        if (!col_a || !col_b)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Both arguments for function {} must be FixedString", getName());

        size_t fixed_string_length = col_a->getN();
        if (fixed_string_length != col_b->getN())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "FixedStrings for function {} must have the same size", getName());

        const auto & data_a = col_a->getChars();
        const auto & data_b = col_b->getChars();

        auto result_column = ColumnVector<Float32>::create(input_rows_count);
        auto & result_data = result_column->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt8 * ptr_a = reinterpret_cast<const UInt8 *>(data_a.data() + row * fixed_string_length);
            const UInt8 * ptr_b = reinterpret_cast<const UInt8 *>(data_b.data() + row * fixed_string_length);
            float dist = 0.0f;
            QuantizedL2DistanceImpl::template execute<Dequantizer>(ptr_a, ptr_b, fixed_string_length, dist);
            result_data[row] = dist;
        }
        return result_column;
    }
};

template <typename Dequantizer, const char * function_name>
class FunctionQuantizedL2Distance
    : public FunctionQuantizedL2DistanceImpl<TargetSpecific::Default::Quantized8BitL2DistanceImpl, Dequantizer, function_name>
{
public:
    explicit FunctionQuantizedL2Distance(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<
            TargetArch::Default,
            FunctionQuantizedL2DistanceImpl<TargetSpecific::Default::Quantized8BitL2DistanceImpl, Dequantizer, function_name>>();
#if USE_MULTITARGET_CODE
        selector.registerImplementation<
            TargetArch::AVX2,
            FunctionQuantizedL2DistanceImpl<TargetSpecific::AVX2::Quantized8BitL2DistanceImpl, Dequantizer, function_name>>();
#endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionQuantizedL2Distance>(context); }

private:
    ImplementationSelector<IFunction> selector;
};


}
