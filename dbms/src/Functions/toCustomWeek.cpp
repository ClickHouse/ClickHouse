#include <DataTypes/DataTypesNumber.h>
#include <Functions/CustomWeekTransforms.h>
#include <Functions/FunctionCustomWeekToSomething.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>


namespace DB
{
using FunctionWeek = FunctionCustomWeekToSomething<DataTypeUInt8, WeekImpl>;
using FunctionYearWeek = FunctionCustomWeekToSomething<DataTypeUInt32, YearWeekImpl>;

void registerFunctionToCustomWeek(FunctionFactory & factory)
{
    factory.registerFunction<FunctionWeek>();
    factory.registerFunction<FunctionYearWeek>();
}

}
