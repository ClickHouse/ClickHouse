#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct Log2Name { static constexpr auto name = "log2"; };
using FunctionLog2 = FunctionMathUnary<UnaryFunctionVectorized<Log2Name, log2>>;

}

REGISTER_FUNCTION(Log2)
{
    factory.registerFunction<FunctionLog2>({}, FunctionFactory::Case::Insensitive);
}

}
