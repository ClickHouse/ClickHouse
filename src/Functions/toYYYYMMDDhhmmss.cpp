#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMMDDhhmmss = FunctionDateOrDateTimeToSomething<DataTypeUInt64, ToYYYYMMDDhhmmssImpl>;

REGISTER_FUNCTION(ToYYYYMMDDhhmmss)
{
    factory.registerFunction<FunctionToYYYYMMDDhhmmss>();
}

}


