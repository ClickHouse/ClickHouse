#include <Functions/FunctionFactory.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/DivisionUtils.h>

namespace DB
{

struct NameDivide { static constexpr auto name = "divide"; };
using FunctionDivide = BinaryArithmeticOverloadResolver<DivideFloatingImpl, NameDivide>;

REGISTER_FUNCTION(Divide)
{
    factory.registerFunction<FunctionDivide>();
}

}
