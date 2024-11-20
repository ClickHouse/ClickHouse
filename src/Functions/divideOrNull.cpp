#include <cmath>
#include <limits>
#include <type_traits>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/DivisionUtils.h>

namespace DB
{

template <typename A, typename B>
struct DivideOrNullImpl
    : BinaryOperation<A, B, DivideFloatingImpl<A, B>>
{
    using Op = DivideFloatingImpl<A, B>;
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <OpCase op_case>
    static void NO_INLINE process(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t size, const NullMap * right_nullmap [[maybe_unused]], NullMap * res_nullmap)
    {
        chassert(res_nullmap);
        if constexpr (op_case == OpCase::RightConstant)
        {
            if (right_nullmap && (*right_nullmap)[0])
                return;

            if (unlikely(*b == 0))
            {
                for (size_t i = 0; i < size; ++i)
                {
                    c[i] = ResultType();
                    (*res_nullmap)[i] = 1;
                }
                return;
            }

            if constexpr (std::is_signed_v<B>)
            {
                if (*b == -1)
                {
                    for (size_t i = 0; i < size; ++i)
                    {
                        if (unlikely(a[i] == std::numeric_limits<A>::min()))
                        {
                            (*res_nullmap)[i] = 1;
                            c[i] = ResultType();
                        }
                        else
                            c[i] = static_cast<ResultType>(-a[i]);
                    }
                    return;
                }
            }
            for (size_t i = 0; i < size; ++i)
                c[i] = Op::template apply<ResultType>(a[i], *b);
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

    static ResultType process(A a, B b, NullMap::value_type * m)
    {
        chassert(m);
        ResultType res{};
        try
        {
            if (b == 0)
            {
                *m = 1;
                return res;
            }
            if constexpr (std::is_signed_v<B>)
            {
                if (b == -1)
                {
                    if (unlikely(a == std::numeric_limits<A>::min()))
                    {
                        *m = 1;
                        return res;
                    }
                    return static_cast<ResultType>(-a);
                }
            }
            return static_cast<ResultType>(a) / b;
        }
        catch (const std::exception&)
        {
            *m = 1;
        }
        return res;
    }

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a, B b)
    {
        return static_cast<Result>(a) / b;
    }

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a, B b, NullMap::value_type * m)
    {
        chassert(m);
        auto res = static_cast<Result>(a) / b;
        if constexpr (std::is_floating_point_v<ResultType>)
            if (unlikely(!std::isfinite(res)))
                *m = 1;

        return res;
    }

private:
    template <OpCase op_case>
    static void apply(const A * __restrict a, const B * __restrict b, ResultType * __restrict c, size_t i, NullMap::value_type * m)
    {
        chassert(m);
        try
        {
            if constexpr (op_case == OpCase::Vector)
                c[i] = Op::template apply<ResultType>(a[i], b[i]);
            else
                c[i] = Op::template apply<ResultType>(*a, b[i]);

            if constexpr (std::is_floating_point_v<ResultType>)
            {
                if (unlikely(!std::isfinite(c[i])))
                    *m = 1;
            }
        }
        catch (const std::exception&)
        {
            *m = 1;
        }
    }

public:
#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};
namespace impl_
{
template <typename A, typename B> struct BinaryOperationImpl<A, B, DivideOrNullImpl<A, B>> : DivideOrNullImpl<A, B> {};
}

struct NameDivideOrNull { static constexpr auto name = "divideOrNull"; };
using FunctionDivideOrNull = BinaryArithmeticOverloadResolver<DivideOrNullImpl, NameDivideOrNull>;

REGISTER_FUNCTION(DivideOrNull)
{
    factory.registerFunction<FunctionDivideOrNull>();
}

}
