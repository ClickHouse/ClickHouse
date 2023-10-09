#include <Functions/FunctionFactory.h>
#include <Functions/FunctionToDecimalString.h>
#include <Functions/IFunction.h>

namespace DB
{

REGISTER_FUNCTION(ToDecimalString)
{
    factory.registerFunction<FunctionToDecimalString>(
        {
            R"(
Returns string representation of a number. First argument is the number of any numeric type,
second argument is the desired number of digits in fractional part. Returns String.

        )",
            Documentation::Examples{{"toDecimalString", "SELECT toDecimalString(2.1456,2)"}},
            Documentation::Categories{"String"}
        }, FunctionFactory::CaseInsensitive);
}

}
