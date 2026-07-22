#include <Functions/FunctionFactory.h>
#include <Functions/toFixedString.h>


namespace DB
{

REGISTER_FUNCTION(FixedString)
{
    factory.registerFunction<FunctionToFixedString>();
}

}
