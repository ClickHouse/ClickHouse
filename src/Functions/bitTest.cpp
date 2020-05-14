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
inline UInt8 applySpecial([[maybe_unused]] A a, [[maybe_unused]] B b)
{
    if constexpr (!std::is_same_v<B, UInt32>)
        throw Exception("Bit test for big integers is implemented only with UInt32 as second argument", ErrorCodes::LOGICAL_ERROR);
    else
        return bit_test(a, b);
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
