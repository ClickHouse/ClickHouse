#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToSecond = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToSecondImpl>;

void registerFunctionToSecond(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToSecond>();
    /// MysQL compatibility alias.
    factory.registerFunction<FunctionToSecond>("SECOND", FunctionFactory::CaseInsensitive);
}

}


