#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/TargetSpecific.h>

#if defined(__x86_64__)
    #define LIBDIVIDE_SSE2 1
    #define LIBDIVIDE_AVX2 1

    #if defined(__clang__)
        #pragma clang attribute push(__attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2"))), apply_to=function)
    #else
        #pragma GCC push_options
        #pragma GCC target("sse,sse2,sse3,ssse3,sse4,popcnt,avx,avx2,tune=native")
    #endif
#endif

#include <libdivide.h>

#if defined(__x86_64__)
    #if defined(__clang__)
        #pragma clang attribute pop
    #else
        #pragma GCC pop_options
    #endif
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
}

namespace
{

/// Optimizations for integer division by a constant.

#if defined(__x86_64__)

DECLARE_DEFAULT_CODE (
    template <typename A, typename B, typename ResultType>
    void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
    {
        libdivide::divider<A> divider(b);
        const A * a_end = a_pos + size;

        static constexpr size_t values_per_simd_register = 16 / sizeof(A);
        const A * a_end_simd = a_pos + size / values_per_simd_register * values_per_simd_register;

        while (a_pos < a_end_simd)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(c_pos),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(a_pos)) / divider);

            a_pos += values_per_simd_register;
            c_pos += values_per_simd_register;
        }

        while (a_pos < a_end)
        {
            *c_pos = *a_pos / divider;
            ++a_pos;
            ++c_pos;
        }
    }
)

DECLARE_AVX2_SPECIFIC_CODE (
    template <typename A, typename B, typename ResultType>
    void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
    {
        libdivide::divider<A> divider(b);
        const A * a_end = a_pos + size;

        static constexpr size_t values_per_simd_register = 32 / sizeof(A);
        const A * a_end_simd = a_pos + size / values_per_simd_register * values_per_simd_register;

        while (a_pos < a_end_simd)
        {
            _mm256_storeu_si256(reinterpret_cast<__m256i *>(c_pos),
                _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a_pos)) / divider);

            a_pos += values_per_simd_register;
            c_pos += values_per_simd_register;
        }

        while (a_pos < a_end)
        {
            *c_pos = *a_pos / divider;
            ++a_pos;
            ++c_pos;
        }
    }
)

#else

template <typename A, typename B, typename ResultType>
void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
{
    libdivide::divider<A> divider(b);
    const A * a_end = a_pos + size;

    while (a_pos < a_end)
    {
        *c_pos = *a_pos / divider;
        ++a_pos;
        ++c_pos;
    }
}

#endif


template <typename A, typename B>
struct DivideIntegralByConstantImpl
    : BinaryOperation<A, B, DivideIntegralImpl<A, B>>
{
    using Op = DivideIntegralImpl<A, B>;
    using ResultType = typename Op::ResultType;
    static const constexpr bool allow_fixed_string = false;

    template <OpCase op_case>
    static void NO_INLINE process(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t size)
    {
        if constexpr (op_case == OpCase::Vector)
            for (size_t i = 0; i < size; ++i)
                c[i] = Op::template apply<ResultType>(a[i], b[i]);
        else if constexpr (op_case == OpCase::LeftConstant)
            for (size_t i = 0; i < size; ++i)
                c[i] = Op::template apply<ResultType>(*a, b[i]);
        else
            vectorConstant(a, *b, c, size);
    }

    static ResultType process(A a, B b) { return Op::template apply<ResultType>(a, b); }

    static NO_INLINE void vectorConstant(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

        /// Division by -1. By the way, we avoid FPE by division of the largest negative number by -1.
        if (unlikely(is_signed_v<B> && b == -1))
        {
            for (size_t i = 0; i < size; ++i)
                c_pos[i] = -make_unsigned_t<A>(a_pos[i]);   /// Avoid UBSan report in signed integer overflow.
            return;
        }

        /// Division with too large divisor.
        if (unlikely(b > std::numeric_limits<A>::max()
            || (std::is_signed_v<A> && std::is_signed_v<B> && b < std::numeric_limits<A>::lowest())))
        {
            for (size_t i = 0; i < size; ++i)
                c_pos[i] = 0;
            return;
        }

#pragma GCC diagnostic pop

        if (unlikely(static_cast<A>(b) == 0))
            throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX2))
        {
            TargetSpecific::AVX2::divideImpl(a_pos, b, c_pos, size);
        }
        else
#endif
        {
#if __x86_64__
            TargetSpecific::Default::divideImpl(a_pos, b, c_pos, size);
#else
            divideImpl(a_pos, b, c_pos, size);
#endif
        }
    }
};

/** Specializations are specified for dividing numbers of the type UInt64 and UInt32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

}

namespace impl_
{
template <> struct BinaryOperationImpl<UInt64, UInt8, DivideIntegralImpl<UInt64, UInt8>> : DivideIntegralByConstantImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16, DivideIntegralImpl<UInt64, UInt16>> : DivideIntegralByConstantImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, DivideIntegralImpl<UInt64, UInt32>> : DivideIntegralByConstantImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, DivideIntegralImpl<UInt64, UInt64>> : DivideIntegralByConstantImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, DivideIntegralImpl<UInt32, UInt8>> : DivideIntegralByConstantImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, DivideIntegralImpl<UInt32, UInt16>> : DivideIntegralByConstantImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, DivideIntegralImpl<UInt32, UInt32>> : DivideIntegralByConstantImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, DivideIntegralImpl<UInt32, UInt64>> : DivideIntegralByConstantImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, DivideIntegralImpl<Int64, Int8>> : DivideIntegralByConstantImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, DivideIntegralImpl<Int64, Int16>> : DivideIntegralByConstantImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, DivideIntegralImpl<Int64, Int32>> : DivideIntegralByConstantImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, DivideIntegralImpl<Int64, Int64>> : DivideIntegralByConstantImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, DivideIntegralImpl<Int32, Int8>> : DivideIntegralByConstantImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, DivideIntegralImpl<Int32, Int16>> : DivideIntegralByConstantImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, DivideIntegralImpl<Int32, Int32>> : DivideIntegralByConstantImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, DivideIntegralImpl<Int32, Int64>> : DivideIntegralByConstantImpl<Int32, Int64> {};
}

struct NameIntDiv { static constexpr auto name = "intDiv"; };
using FunctionIntDiv = BinaryArithmeticOverloadResolver<DivideIntegralImpl, NameIntDiv, false>;

void registerFunctionIntDiv(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntDiv>();
}

}
