#pragma once

#include <type_traits>
#include <common/likely.h>
#include <Common/Exception.h>
#include <Common/config.h>
#include <DataTypes/NumberTraits.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_DIVISION;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

template <typename A, typename B>
inline void throwIfDivisionLeadsToFPE(A a, B b)
{
    /// Is it better to use siglongjmp instead of checks?

    if (unlikely(b == 0))
        throw Exception("Division by zero", ErrorCodes::ILLEGAL_DIVISION);

    /// http://avva.livejournal.com/2548306.html
    if (unlikely(std::is_signed_v<A> && std::is_signed_v<B> && a == std::numeric_limits<A>::min() && b == -1))
        throw Exception("Division of minimal signed number by minus one", ErrorCodes::ILLEGAL_DIVISION);
}

template <typename A, typename B>
inline bool divisionLeadsToFPE(A a, B b)
{
    if (unlikely(b == 0))
        return true;

    if (unlikely(std::is_signed_v<A> && std::is_signed_v<B> && a == std::numeric_limits<A>::min() && b == -1))
        return true;

    return false;
}


#pragma GCC diagnostic pop

template <typename A, typename B>
struct DivideIntegralImpl
{
    using ResultType = typename NumberTraits::ResultOfIntegerDivision<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b)
    {
        throwIfDivisionLeadsToFPE(a, b);
        return a / b;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// don't know how to throw from LLVM IR
#endif
};

}
