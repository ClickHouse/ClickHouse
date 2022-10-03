#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeSecondNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeSecondNumImpl>;

REGISTER_FUNCTION(ToRelativeSecondNum)
{
    factory.registerFunction<FunctionToRelativeSecondNum>();
}

}


