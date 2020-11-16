#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddQuarters = FunctionDateOrDateTimeAddInterval<AddQuartersImpl>;

void registerFunctionAddQuarters(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAddQuarters>();
}

}


