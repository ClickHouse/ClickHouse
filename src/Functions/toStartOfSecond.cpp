#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToStartOfSecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfSecondImpl>;

void registerFunctionToStartOfSecond(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfSecond>();
}

}
