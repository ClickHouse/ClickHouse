#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfQuarter = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfQuarterImpl>;

REGISTER_FUNCTION(ToStartOfQuarter)
{
    factory.registerFunction<FunctionToStartOfQuarter>();
}

}


