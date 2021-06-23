#include <Functions/indexHint.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

void registerFunctionIndexHint(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIndexHint>();
}

}
