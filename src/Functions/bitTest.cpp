#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Core/Defines.h>
#include "Columns/ColumnNullable.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int PARAMETER_OUT_OF_BOUND;
}

namespace
{

template <typename A, typename B>
struct BitTestImpl
{
    using ResultType = UInt8;
    static const constexpr bool allow_fixed_string = false;
    static const constexpr bool allow_string_integer = false;

    template <typename Result = ResultType>
    static Result apply(A a [[maybe_unused]], B b [[maybe_unused]], NullMap::value_type * m [[maybe_unused]] = nullptr)
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "bitTest is not implemented for big integers as second argument");
        else
        {
            typename NumberTraits::ToInteger<A>::Type a_int = a;
            typename NumberTraits::ToInteger<B>::Type b_int = b;
            const auto max_position = static_cast<decltype(b)>((8 * sizeof(a)) - 1);
            if (b_int > max_position || b_int < 0)
            {
                if (!m)
                    throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
                                "The bit position argument needs to a positive value and less or equal to {} for integer {}",
                                std::to_string(max_position), std::to_string(a_int));
                else
                {
                    *m = 1;
                    return Result();
                }
            }
            return (a_int >> b_int) & 1;
        }
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO
#endif
};

struct NameBitTest { static constexpr auto name = "bitTest"; };
using FunctionBitTest = BinaryArithmeticOverloadResolver<BitTestImpl, NameBitTest, true, false>;

}

REGISTER_FUNCTION(BitTest)
{
    factory.registerFunction<FunctionBitTest>();
}

}
