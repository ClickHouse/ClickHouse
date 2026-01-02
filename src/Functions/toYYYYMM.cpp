#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMM = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMImpl>;

REGISTER_FUNCTION(ToYYYYMM)
{
    factory.registerFunction<FunctionToYYYYMM>();
}

}


