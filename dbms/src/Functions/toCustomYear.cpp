#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CustomDateTransforms.h>
#include <Functions/FunctionCustomDateToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

    using FunctionToCustomYear = FunctionCustomDateToSomething<DataTypeUInt16, ToCustomYearImpl>;

    void registerFunctionToCustomYear(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionToCustomYear>();
    }

}


