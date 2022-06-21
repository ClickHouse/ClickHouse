#include <algorithm>
#include <cassert>
#include <chrono>
#include <random>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <x86intrin.h>
#include <Common/TargetSpecific.h>
#include <Common/logger_useful.h>

#if defined(__SSE2__)
#    include <emmintrin.h>
#endif
#if defined(__AVX512F__) || defined(__AVX512BW__) || defined(__AVX__) || defined(__AVX2__)
#    include <immintrin.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
}


#if defined(__AVX512F__) || defined(__AVX512BW__) || defined(__AVX__) || defined(__AVX2__) || defined(__SSE2__)
// reads 0 <= d < 4 floats as __m128
static inline __m128 masked_read(int d, const float * x)
{
    assert(0 <= d && d < 4);
    __attribute__((__aligned__(16))) float buf[4] = {0, 0, 0, 0};
    switch (d)
    {
        case 3:
            buf[2] = x[2];
            break;
        case 2:
            buf[1] = x[1];
            break;
        case 1:
            buf[0] = x[0];
            break;
    }
    return _mm_load_ps(buf);
    // cannot use AVX2 _mm_mask_set1_epi32
}
#endif

#if defined(__AVX512F__) || defined(__AVX512BW__)
// reads 0 <= d < 8 floats as __m256
static inline __m256 masked_read_8(int d, const float * x)
{
    assert(0 <= d && d < 8);
    if (d < 4)
    {
        __m256 res = _mm256_setzero_ps();
        res = _mm256_insertf128_ps(res, masked_read(d, x), 0);
        return res;
    }
    else
    {
        __m256 res = _mm256_setzero_ps();
        res = _mm256_insertf128_ps(res, _mm_loadu_ps(x), 0);
        res = _mm256_insertf128_ps(res, masked_read(d - 4, x + 4), 1);
        return res;
    }
}

// reads 0 <= d < 16 floats as __m512
static inline __m512 masked_read_16(int d, const float * x)
{
    assert(0 <= d && d < 16);
    if (d < 8)
    {
        __m512 res = _mm512_setzero_ps();
        res = _mm512_insertf32x8(res, masked_read_8(d, x), 0);
        return res;
    }
    else
    {
        __m512 res = _mm512_setzero_ps();
        res = _mm512_insertf32x8(res, _mm256_loadu_ps(x), 0);
        res = _mm512_insertf32x8(res, masked_read_8(d - 8, x + 8), 1);
        return res;
    }
}


float fvec_L2sqr(const float * x, const float * y, size_t d)
{
    __m512 msum1 = _mm512_setzero_ps();

    while (d >= 16)
    {
        __m512 mx = _mm512_loadu_ps(x);
        x += 16;
        __m512 my = _mm512_loadu_ps(y);
        y += 16;
        const __m512 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
        d -= 16;
    }

    __m256 msum2 = _mm512_extractf32x8_ps(msum1, 1);
    msum2 += _mm512_extractf32x8_ps(msum1, 0);

    while (d >= 8)
    {
        __m256 mx = _mm256_loadu_ps(x);
        x += 8;
        __m256 my = _mm256_loadu_ps(y);
        y += 8;
        const __m256 a_m_b1 = mx - my;
        msum2 += a_m_b1 * a_m_b1;
        d -= 8;
    }

    __m128 msum3 = _mm256_extractf128_ps(msum2, 1);
    msum3 += _mm256_extractf128_ps(msum2, 0);

    if (d >= 4)
    {
        __m128 mx = _mm_loadu_ps(x);
        x += 4;
        __m128 my = _mm_loadu_ps(y);
        y += 4;
        const __m128 a_m_b1 = mx - my;
        msum3 += a_m_b1 * a_m_b1;
        d -= 4;
    }

    if (d > 0)
    {
        __m128 mx = masked_read(d, x);
        __m128 my = masked_read(d, y);
        __m128 a_m_b1 = mx - my;
        msum3 += a_m_b1 * a_m_b1;
    }

    msum3 = _mm_hadd_ps(msum3, msum3);
    msum3 = _mm_hadd_ps(msum3, msum3);
    return _mm_cvtss_f32(msum3);
}
#elif defined(__AVX__) || defined(__AVX2__)
float fvec_L2sqr(const float * x, const float * y, size_t d)
{
    __m256 msum1 = _mm256_setzero_ps();
    __m256 msum11 = _mm256_setzero_ps();
    __m256 msum12 = _mm256_setzero_ps();
    __m256 msum13 = _mm256_setzero_ps();

    while (d >= 4 * 8)
    {
        __m256 mx = _mm256_loadu_ps(x);
        __m256 my = _mm256_loadu_ps(y);
        const __m256 a_m_b1 = _mm256_sub_ps(mx, my);
        msum1 = _mm256_add_ps(msum1, _mm256_mul_ps(a_m_b1, a_m_b1));

        __m256 mx1 = _mm256_loadu_ps(x + 8);
        __m256 my1 = _mm256_loadu_ps(y + 8);
        const __m256 a_m_b11 = _mm256_sub_ps(mx1, my1);
        msum11 = _mm256_add_ps(msum11, _mm256_mul_ps(a_m_b11, a_m_b11));

        __m256 mx2 = _mm256_loadu_ps(x + 16);
        __m256 my2 = _mm256_loadu_ps(y + 16);
        const __m256 a_m_b12 = _mm256_sub_ps(mx2, my2);
        msum12 = _mm256_add_ps(msum12, _mm256_mul_ps(a_m_b12, a_m_b12));

        __m256 mx3 = _mm256_loadu_ps(x + 24);
        __m256 my3 = _mm256_loadu_ps(y + 24);
        const __m256 a_m_b13 = _mm256_sub_ps(mx3, my3);
        msum13 = _mm256_add_ps(msum13, _mm256_mul_ps(a_m_b13, a_m_b13));

        x += 4 * 8;
        y += 4 * 8;
        d -= 4 * 8;
    }

    msum1 = _mm256_add_ps(msum11, _mm256_add_ps(msum12, msum13));

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 = _mm_add_ps(msum2, _mm256_extractf128_ps(msum1, 0));

    if (d >= 4)
    {
        __m128 mx = _mm_loadu_ps(x);
        x += 4;
        __m128 my = _mm_loadu_ps(y);
        y += 4;
        const __m128 a_m_b1 = _mm_sub_ps(mx, my);
        msum2 = _mm_add_ps(msum2, _mm_mul_ps(a_m_b1, a_m_b1));
        d -= 4;
    }

    if (d > 0)
    {
        __m128 mx = masked_read(d, x);
        __m128 my = masked_read(d, y);
        const __m128 a_m_b1 = _mm_sub_ps(mx, my);
        msum2 = _mm_add_ps(msum2, _mm_mul_ps(a_m_b1, a_m_b1));
    }

    msum2 = _mm_hadd_ps(msum2, msum2);
    msum2 = _mm_hadd_ps(msum2, msum2);
    return _mm_cvtss_f32(msum2);
}
#elif defined(__SSE2__)
float fvec_L2sqr(const float * x, const float * y, size_t d)
{
    __m128 msum1 = _mm_setzero_ps();

    while (d >= 4)
    {
        __m128 mx = _mm_loadu_ps(x);
        x += 4;
        __m128 my = _mm_loadu_ps(y);
        y += 4;
        const __m128 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
        d -= 4;
    }

    if (d > 0)
    {
        // add the last 1, 2 or 3 values
        __m128 mx = masked_read(d, x);
        __m128 my = masked_read(d, y);
        __m128 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
    }

    msum1 = _mm_hadd_ps(msum1, msum1);
    msum1 = _mm_hadd_ps(msum1, msum1);
    return _mm_cvtss_f32(msum1);
}
#else
float fvec_L2sqr(const float * x, const float * y, size_t d)
{
    float msum = 0.0f;
    for (size_t i = 0; i < d; ++i)
    {
        float a_m_b = x[i] - y[i];
        msum += a_m_b * a_m_b;
    }
    return msum;
}
#endif

class FunctionArrayL2 : public IFunction
{
public:
    //    using ResultType = Float32;

    static constexpr auto name = "arrayL2";
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayL2>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypes types;
        for (size_t i = 0; i < 2; ++i)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument {} of function {} must be array.", i, getName());

            types.push_back(array_type->getNestedType());
        }
        const auto & common_type = getLeastSupertype(types);
        switch (common_type->getTypeId())
        {
            case TypeIndex::Float32:
                return std::make_shared<DataTypeFloat32>();
            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Support: Float32.",
                    getName(),
                    common_type->getName());
        }
    }

    //    template <typename T, typename U>
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnPtr col_x = arguments[0].column;
        ColumnPtr col_y = arguments[1].column;

        if (!(result_type->getTypeId() == TypeIndex::Float32))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type {}", result_type->getName());

        if (typeid_cast<const ColumnConst *>(col_x.get()))
        {
            LOG_INFO(&Poco::Logger::get("ARRAY L2"), "CONST COL X BUT IS FLOAT64");
            //                    return executeWithTypesFirstArgConst<ResultType, FirstArgType, SecondArgType>(col_x, col_y, input_rows_count, arguments);
        }
        else if (typeid_cast<const ColumnConst *>(col_y.get()))
        {
            LOG_INFO(&Poco::Logger::get("ARRAY L2"), "CONST COL Y BUT IS FLOAT64");
            //                    return executeWithTypesFirstArgConst<ResultType, SecondArgType, FirstArgType>(col_y, col_x, input_rows_count, arguments);
        }

        col_x = col_x->convertToFullColumnIfConst();
        col_y = col_y->convertToFullColumnIfConst();

        const auto & array_x = *assert_cast<const ColumnArray *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnArray *>(col_y.get());

        const auto & data_x = typeid_cast<const ColumnVector<Float32> &>(array_x.getData()).getData();
        const auto & data_y = typeid_cast<const ColumnVector<Float32> &>(array_y.getData()).getData();

        const auto & offsets_x = array_x.getOffsets();
        const auto & offsets_y = array_y.getOffsets();

        /// Check that arrays in both columns are the sames size
        for (size_t row = 0; row < offsets_x.size(); ++row)
        {
            if (unlikely(offsets_x[row] != offsets_y[row]))
            {
                ColumnArray::Offset prev_offset = row > 0 ? offsets_x[row] : 0;
                throw Exception(
                    ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH,
                    "Arguments of function {} have different array sizes: {} and {}",
                    getName(),
                    offsets_x[row] - prev_offset,
                    offsets_y[row] - prev_offset);
            }
        }

        auto result = ColumnVector<Float32>::create(input_rows_count);
        auto & result_data = result->getData();

        size_t row = 0;
        size_t prev = 0;
        for (auto off : offsets_x)
        {
            result_data[row] = fvec_L2sqr(&data_x[prev], &data_y[prev], off - prev);
            row++;
            prev = off;
        }
        return result;
    }
};

void registerFunctionArrayL2(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayL2>();
}
}
