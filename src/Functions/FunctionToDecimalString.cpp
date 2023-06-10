#include <Functions/FunctionFactory.h>
#include <Functions/FunctionToDecimalString.h>
#include <Functions/IFunction.h>

namespace DB
{

REGISTER_FUNCTION(ToDecimalString)
{
    factory.registerFunction<FunctionToDecimalString>(
        FunctionDocumentation{
            .description=R"(
Returns string representation of a number. First argument is the number of any numeric type,
second argument is the desired number of digits in fractional part. Returns String.

        )",
            .examples{{"toDecimalString", "SELECT toDecimalString(2.1456,2)", ""}},
            .categories{"String"}
        }, FunctionFactory::CaseInsensitive);
}

}
