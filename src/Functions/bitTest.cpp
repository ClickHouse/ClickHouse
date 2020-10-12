#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Core/Defines.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

template <typename A, typename B>
struct BitTestImpl
{
    using ResultType = UInt8;
    static const constexpr bool allow_fixed_string = false;

    template <typename Result = ResultType>
    NO_SANITIZE_UNDEFINED static inline Result apply(A a [[maybe_unused]], B b [[maybe_unused]])
    {
        if constexpr (is_big_int_v<A> || is_big_int_v<B>)
            throw Exception("bitTest is not implemented for big integers as second argument", ErrorCodes::NOT_IMPLEMENTED);
        else
            return (typename NumberTraits::ToInteger<A>::Type(a) >> typename NumberTraits::ToInteger<B>::Type(b)) & 1;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO
#endif
};

struct NameBitTest { static constexpr auto name = "bitTest"; };
using FunctionBitTest = BinaryArithmeticOverloadResolver<BitTestImpl, NameBitTest>;

}

void registerFunctionBitTest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTest>();
}

}
