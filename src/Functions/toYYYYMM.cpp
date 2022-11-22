#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMM = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMImpl>;

void registerFunctionToYYYYMM(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToYYYYMM>();
}

}


