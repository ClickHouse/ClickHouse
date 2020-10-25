#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBitTestMany.h>

namespace DB
{

struct BitTestAllImpl
{
    template <typename A, typename B>
    static inline UInt8 apply(A a, B b) { return (a & b) == b; }
};

struct NameBitTestAll { static constexpr auto name = "bitTestAll"; };
using FunctionBitTestAll = FunctionBitTestMany<BitTestAllImpl, NameBitTestAll>;

void registerFunctionBitTestAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAll>();
}

}
