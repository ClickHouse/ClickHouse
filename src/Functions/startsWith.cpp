#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionStartsWith = FunctionStartsEndsWith<NameStartsWith>;

REGISTER_FUNCTION(StartsWith)
{
    factory.registerFunction<FunctionStartsWith>();
}

}
