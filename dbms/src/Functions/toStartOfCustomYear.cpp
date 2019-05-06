#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CustomDateTransforms.h>
#include <Functions/FunctionCustomDateToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToStartOfCustomYear = FunctionCustomDateToSomething<DataTypeDate, ToStartOfCustomYearImpl>;

void registerFunctionToStartOfCustomYear(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfCustomYear>();
}

}


