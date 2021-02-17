#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>

#if defined(__SSE2__)
#    define LIBDIVIDE_SSE2 1
#endif

#include <libdivide.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
}

/// Optimizations for integer division by a constant.

template <typename A, typename B>
struct DivideIntegralByConstantImpl
    : BinaryOperationImplBase<A, B, DivideIntegralImpl<A, B>>
{
    using ResultType = typename DivideIntegralImpl<A, B>::ResultType;
    static const constexpr bool allow_fixed_string = false;

    static NO_INLINE void vectorConstant(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

        /// Division by -1. By the way, we avoid FPE by division of the largest negative number by -1.
        /// And signed integer overflow is well defined in C++20.
        if (unlikely(is_signed_v<B> && b == -1))
        {
            for (size_t i = 0; i < size; ++i)
                c_pos[i] = -a_pos[i];
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

        libdivide::divider<A> divider(b);

        const A * a_end = a_pos + size;

#if defined(__SSE2__)
        static constexpr size_t values_per_sse_register = 16 / sizeof(A);
        const A * a_end_sse = a_pos + size / values_per_sse_register * values_per_sse_register;

        while (a_pos < a_end_sse)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(c_pos),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(a_pos)) / divider);

            a_pos += values_per_sse_register;
            c_pos += values_per_sse_register;
        }
#endif

        while (a_pos < a_end)
        {
            *c_pos = *a_pos / divider;
            ++a_pos;
            ++c_pos;
        }
    }
};

/** Specializations are specified for dividing numbers of the type UInt64 and UInt32 by the numbers of the same sign.
  * Can be expanded to all possible combinations, but more code is needed.
  */

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


struct NameIntDiv { static constexpr auto name = "intDiv"; };
using FunctionIntDiv = FunctionBinaryArithmetic<DivideIntegralImpl, NameIntDiv, false>;

void registerFunctionIntDiv(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntDiv>();
}

}
