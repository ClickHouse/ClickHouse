#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionFormatDecimal.h>

namespace DB
{

REGISTER_FUNCTION(FormatDecimal)
{
    factory.registerFunction<FunctionFormatDecimal>(
        {
            R"(
Returns string representation of a number. First argument is the number of any numeric type,
second argument is the desired number of digits in fractional part. Returns String.

        )",
            Documentation::Examples{{"formatDecimal", "SELECT formatDecimal(2.1456,2)"}},
            Documentation::Categories{"String"}
        }, FunctionFactory::CaseInsensitive);
}

}


