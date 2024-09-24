#include <limits>
#include <type_traits>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include "base/defines.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename A, typename B>
struct DivideOrNullImpl
{
    using ResultType = typename NumberTraits::ResultOfFloatingPointDivision<A, B>::Type;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        return static_cast<Result>(a) / b;
    }

    template <typename Result = ResultType>
    static NO_SANITIZE_UNDEFINED Result apply(A a [[maybe_unused]], B b [[maybe_unused]], NullMap::value_type * m)
    {
        assert(m);
        auto res = static_cast<Result>(a) / b;
        if constexpr (std::is_same_v<ResultType, Float32>)
            if likely(res != std::numeric_limits<Float32>::infinity())
                return res;

        if constexpr (std::is_same_v<ResultType, Float64>)
            if likely(res != std::numeric_limits<Float64>::infinity())
                return res;

        *m = 1;

        return res;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false;
#endif
};

struct NameDivideOrNull { static constexpr auto name = "divideOrNull"; };
using FunctionDivideOrNull = BinaryArithmeticOverloadResolver<DivideOrNullImpl, NameDivideOrNull>;

REGISTER_FUNCTION(DivideOrNull)
{
    factory.registerFunction<FunctionDivideOrNull>();
}

}
