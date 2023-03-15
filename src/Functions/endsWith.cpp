#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionEndsWith = FunctionStartsEndsWith<NameEndsWith>;

void registerFunctionEndsWith(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEndsWith>();
}

}

