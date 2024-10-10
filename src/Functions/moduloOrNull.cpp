#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
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

    /// NOTE: res_nullmap already initialized with right_nullmap if it is not nullptr
    template <OpCase op_case>
    static void NO_INLINE process(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t size, const NullMap * right_nullmap [[maybe_unused]], NullMap * res_nullmap)
    {
        chassert(res_nullmap);
        /// NOTE: to dismiss the compiler error with libdivide
        if constexpr (op_case == OpCase::RightConstant && !std::is_same_v<ResultType, Float64> && !is_big_int_v<A> && !is_big_int_v<B> && !std::is_same_v<A, Int8> && !std::is_same_v<B, Int8> && !std::is_same_v<A, UInt8> && !std::is_same_v<B, UInt8>)
        {
            if ((*res_nullmap)[0])
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

    static ResultType process(A a, B b, NullMap::value_type * m [[maybe_unused]] = nullptr)
    {
        ResultType res{};
        try
        {
            res = Op::template apply<ResultType>(a, b);
            if constexpr (std::is_floating_point_v<ResultType>)
            {
                if (unlikely(!std::isfinite(res)) && m)
                    *m = 1;
            }
        }
        catch (const std::exception&)
        {
            if (m)
                *m = 1;
            else
                throw;
        }
        return res;
    }

    static void NO_INLINE NO_SANITIZE_UNDEFINED vectorConstant(const A * __restrict src, B b, ResultType * __restrict dst, size_t size, NullMap * res_nullmap)
    {
        /// Modulo with too small divisor.
        if constexpr (std::is_signed_v<B>)
        {
            if (unlikely((b == -1)))
            {
                for (size_t i = 0; i < size; ++i)
                    dst[i] = 0;
                return;
            }
        }
        if (b == 1)
        {
            for (size_t i = 0; i < size; ++i)
                dst[i] = 0;
            return;
        }

        /// Modulo with too large divisor.
        if constexpr ((std::is_signed_v<B> && std::is_signed_v<A>) || (std::is_unsigned_v<B> && std::is_unsigned_v<A>))
        {
            if (unlikely(b > std::numeric_limits<A>::max()
                || (std::is_signed_v<A> && std::is_signed_v<B> && b < std::numeric_limits<A>::lowest())))
            {
                for (size_t i = 0; i < size; ++i)
                    dst[i] = static_cast<ResultType>(src[i]);
                return;
            }
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

    template <typename Result = ResultType>
    static Result apply(A a, B b, NullMap::value_type * m [[maybe_unused]] = nullptr)
    {
        Result res{};
        try
        {
            res = Op::template apply<Result>(a, b);
            if constexpr (std::is_floating_point_v<Result>)
                if (unlikely(!std::isfinite(res)) && m)
                    *m = 1;
        }
        catch (const std::exception&)
        {
            if (m)
                *m = 1;
            else
                throw;
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

            if constexpr (std::is_floating_point_v<ResultType>)
                if (unlikely(!std::isfinite(c[i])) && m)
                    * m = 1;
        }
        catch (const std::exception&)
        {
            *m = 1;
        }
    }

public:
#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO implement the checks
#endif
};

namespace impl_
{
template <typename A, typename B> struct BinaryOperationImpl<A, B, ModuloOrNullImpl<A, B>> : ModuloOrNullImpl<A, B> {};
}

struct NameModuloOrNull { static constexpr auto name = "moduloOrNull"; };
using FunctionModuloOrNull = BinaryArithmeticOverloadResolver<ModuloOrNullImpl, NameModuloOrNull>;


REGISTER_FUNCTION(ModuloOrNull)
{
    factory.registerFunction<FunctionModuloOrNull>();
}

}
