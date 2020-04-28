#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Core/Defines.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename A, typename B>
inline UInt8 applySpecial(A /*a*/, B /*b*/)
{
    throw Exception("Bit test is not implemented for big integers", ErrorCodes::LOGICAL_ERROR);
    // if constexpr (std::is_same_v<A, UInt8>)
    //     return (UInt16(a) >> b) & 1;
    // else
    //     return (a >> UInt16(b)) & 1;
}

template <typename A, typename B>
struct BitTestImpl
{
    using ResultType = UInt8;
    static const constexpr bool allow_fixed_string = false;
    static constexpr bool is_special = is_big_int_v<A> || is_big_int_v<B>;

    template <typename Result = ResultType>
    NO_SANITIZE_UNDEFINED static inline Result apply(A a, B b)
    {
        if constexpr (is_special)
            return applySpecial(a, b);
        else
            return (typename NumberTraits::ToInteger<A>::Type(a) >> typename NumberTraits::ToInteger<B>::Type(b)) & 1;
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// TODO
#endif
};

struct NameBitTest { static constexpr auto name = "bitTest"; };
using FunctionBitTest = FunctionBinaryArithmetic<BitTestImpl, NameBitTest>;

void registerFunctionBitTest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTest>();
}

}
