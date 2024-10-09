#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Common/Exception.h>
#include <libdivide.h>

namespace DB
{

template <typename A, typename B>
struct ModuloOrNullImpl
    : BinaryOperation<A, B, ModuloImpl<A, B>>
{
    using Op = ModuloImpl<A, B>;
    using ResultType = typename Op::ResultType;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <OpCase op_case>
    static void NO_INLINE process(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t size, const NullMap * right_nullmap, NullMap * res_nullmap)
    {
        chassert(res_nullmap);
        if constexpr (op_case == OpCase::RightConstant)
        {
            if (right_nullmap && (*right_nullmap)[0])
                return;
            vectorConstant(a, *b, c, size, res_nullmap);
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
                if ((*res_nullmap)[i])
                    c[i] = ResultType();
                else
                    apply<op_case>(a, b, c, i, &((*res_nullmap)[i]));
        }
    }

    static void NO_INLINE NO_SANITIZE_UNDEFINED vectorConstant(const A * __restrict src, B b, ResultType * __restrict dst, size_t size, NullMap * res_nullmap)
    {
        /// Modulo with too small divisor.
        if (unlikely((std::is_signed_v<B> && b == -1) || b == 1))
        {
            for (size_t i = 0; i < size; ++i)
                dst[i] = 0;
            return;
        }

        /// Modulo with too large divisor.
        if (unlikely(b > std::numeric_limits<A>::max()
            || (std::is_signed_v<A> && std::is_signed_v<B> && b < std::numeric_limits<A>::lowest())))
        {
            for (size_t i = 0; i < size; ++i)
                dst[i] = static_cast<ResultType>(src[i]);
            return;
        }

        /// Set result to NULL if divide by zero or too large divisor.
        if (unlikely(static_cast<A>(b) == 0 || std::is_signed_v<B> && b == std::numeric_limits<B>::lowest()))
        {
            for (size_t i = 0; i < size; ++i)
                (*res_nullmap)[i] = 1;
            return;
        }

        /// Modulo of division by negative number is the same as the positive number.
        if (b < 0)
            b = -b;

        /// Here we failed to make the SSE variant from libdivide give an advantage.

        if (b & (b - 1))
        {
            libdivide::divider<A> divider(static_cast<A>(b));
            for (size_t i = 0; i < size; ++i)
            {
                /// NOTE: perhaps, the division semantics with the remainder of negative numbers is not preserved.
                dst[i] = static_cast<ResultType>(src[i] - (src[i] / divider) * b);
            }
        }
        else
        {
            // gcc libdivide doesn't work well for pow2 division
            auto mask = b - 1;
            for (size_t i = 0; i < size; ++i)
                dst[i] = static_cast<ResultType>(src[i] & mask);
        }
    }

    static ResultType process(A a, B b, NullMap::value_type * m)
    {
        ResultType res;
        try
        {
            res = Op::template apply<ResultType>(a, b);
        }
        catch (const std::exception&)
        {
            if (m)
                *m = 1;
        }
        return res;
    }

private:
    template <OpCase op_case>
    static void apply(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t i, NullMap::value_type * m)
    {
        try
        {
            if constexpr (op_case == OpCase::Vector)
                c[i] = Op::template apply<ResultType>(a[i], b[i]);
            else
                c[i] = Op::template apply<ResultType>(*a, b[i]);
        }
        catch (const std::exception&)
        {
            *m = 1;
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO implement the checks
#endif
};

namespace impl_
{
template <> struct BinaryOperationImpl<UInt64, UInt8, ModuloImpl<UInt64, UInt8>> : ModuloOrNullImpl<UInt64, UInt8> {};
template <> struct BinaryOperationImpl<UInt64, UInt16, ModuloImpl<UInt64, UInt16>> : ModuloOrNullImpl<UInt64, UInt16> {};
template <> struct BinaryOperationImpl<UInt64, UInt32, ModuloImpl<UInt64, UInt32>> : ModuloOrNullImpl<UInt64, UInt32> {};
template <> struct BinaryOperationImpl<UInt64, UInt64, ModuloImpl<UInt64, UInt64>> : ModuloOrNullImpl<UInt64, UInt64> {};

template <> struct BinaryOperationImpl<UInt32, UInt8, ModuloImpl<UInt32, UInt8>> : ModuloOrNullImpl<UInt32, UInt8> {};
template <> struct BinaryOperationImpl<UInt32, UInt16, ModuloImpl<UInt32, UInt16>> : ModuloOrNullImpl<UInt32, UInt16> {};
template <> struct BinaryOperationImpl<UInt32, UInt32, ModuloImpl<UInt32, UInt32>> : ModuloOrNullImpl<UInt32, UInt32> {};
template <> struct BinaryOperationImpl<UInt32, UInt64, ModuloImpl<UInt32, UInt64>> : ModuloOrNullImpl<UInt32, UInt64> {};

template <> struct BinaryOperationImpl<Int64, Int8, ModuloImpl<Int64, Int8>> : ModuloOrNullImpl<Int64, Int8> {};
template <> struct BinaryOperationImpl<Int64, Int16, ModuloImpl<Int64, Int16>> : ModuloOrNullImpl<Int64, Int16> {};
template <> struct BinaryOperationImpl<Int64, Int32, ModuloImpl<Int64, Int32>> : ModuloOrNullImpl<Int64, Int32> {};
template <> struct BinaryOperationImpl<Int64, Int64, ModuloImpl<Int64, Int64>> : ModuloOrNullImpl<Int64, Int64> {};

template <> struct BinaryOperationImpl<Int32, Int8, ModuloImpl<Int32, Int8>> : ModuloOrNullImpl<Int32, Int8> {};
template <> struct BinaryOperationImpl<Int32, Int16, ModuloImpl<Int32, Int16>> : ModuloOrNullImpl<Int32, Int16> {};
template <> struct BinaryOperationImpl<Int32, Int32, ModuloImpl<Int32, Int32>> : ModuloOrNullImpl<Int32, Int32> {};
template <> struct BinaryOperationImpl<Int32, Int64, ModuloImpl<Int32, Int64>> : ModuloOrNullImpl<Int32, Int64> {};
template <typename A, typename B> struct BinaryOperationImpl<A, B, ModuloImpl<A, B>> : ModuloOrNullImpl<A, B> {};
}

struct NameModuloOrNull { static constexpr auto name = "moduloOrNull"; };
using FunctionModuloOrNull = BinaryArithmeticOverloadResolver<ModuloOrNullImpl, NameModuloOrNull>;


REGISTER_FUNCTION(ModuloOrNull)
{
    factory.registerFunction<FunctionModuloOrNull>();
}

}
