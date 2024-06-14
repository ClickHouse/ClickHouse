#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBitTestMany.h>

namespace DB
{
namespace
{

struct BitTestAllImpl
{
    template <typename A, typename B>
    static UInt8 apply(A a, B b) { return (a & b) == b; }
};

struct NameBitTestAll { static constexpr auto name = "bitTestAll"; };
using FunctionBitTestAll = FunctionBitTestMany<BitTestAllImpl, NameBitTestAll>;

}

REGISTER_FUNCTION(BitTestAll)
{
    factory.registerFunction<FunctionBitTestAll>();
}

}
