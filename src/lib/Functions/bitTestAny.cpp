#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBitTestMany.h>

namespace DB
{

struct BitTestAnyImpl
{
    template <typename A, typename B>
    static inline UInt8 apply(A a, B b) { return (a & b) != 0; }
};

struct NameBitTestAny { static constexpr auto name = "bitTestAny"; };
using FunctionBitTestAny = FunctionBitTestMany<BitTestAnyImpl, NameBitTestAny>;

void registerFunctionBitTestAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAny>();
}

}
