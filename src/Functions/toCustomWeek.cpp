#include <DataTypes/DataTypesNumber.h>
#include <Functions/CustomWeekTransforms.h>
#include <Functions/FunctionCustomWeekToSomething.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>


namespace DB
{
using FunctionToWeek = FunctionCustomWeekToSomething<DataTypeUInt8, ToWeekImpl>;
using FunctionToYearWeek = FunctionCustomWeekToSomething<DataTypeUInt32, ToYearWeekImpl>;
using FunctionToStartOfWeek = FunctionCustomWeekToSomething<DataTypeDate, ToStartOfWeekImpl>;

void registerFunctionToCustomWeek(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToWeek>();
    factory.registerFunction<FunctionToYearWeek>();
    factory.registerFunction<FunctionToStartOfWeek>();

    /// Compatibility aliases for mysql.
    factory.registerAlias("week", "toWeek", FunctionFactory::CaseInsensitive);
    factory.registerAlias("yearweek", "toYearWeek", FunctionFactory::CaseInsensitive);
}

}
