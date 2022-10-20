#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionStartsWith = FunctionStartsEndsWith<NameStartsWith>;

void registerFunctionStartsWith(FunctionFactory & factory)
{
    factory.registerFunction<FunctionStartsWith>();
}

}
