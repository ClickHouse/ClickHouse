#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CustomDateTransforms.h>
#include <Functions/FunctionCustomDateToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToCustomWeek = FunctionCustomDateToSomething<DataTypeUInt8, ToCustomWeekImpl>;

void registerFunctionToCustomWeek(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToCustomWeek>();
}

}


