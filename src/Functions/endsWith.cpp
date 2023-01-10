#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionEndsWith = FunctionStartsEndsWith<NameEndsWith>;

REGISTER_FUNCTION(EndsWith)
{
    factory.registerFunction<FunctionEndsWith>();
}

}

