#include <DataTypes/DataTypesNumber.h>
#include <Functions/CustomDateTransforms.h>
#include <Functions/FunctionCustomDateToSomething.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>


namespace DB
{
using FunctionToCustomWeek = FunctionCustomDateToSomething<DataTypeUInt8, ToCustomWeekImpl>;

void registerFunctionToCustomWeek(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToCustomWeek>();
}

}
