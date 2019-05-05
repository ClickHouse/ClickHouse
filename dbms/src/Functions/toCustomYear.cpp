#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

    using FunctionToCustomYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToCustomYearImpl>;

    void registerFunctionToCustomYear(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionToCustomYear>();
    }

}


