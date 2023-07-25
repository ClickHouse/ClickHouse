#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionStartsWithUTF8 = FunctionStartsEndsWith<NameStartsWithUTF8>;

REGISTER_FUNCTION(StartsWithUTF8)
{
    factory.registerFunction<FunctionStartsWithUTF8>();
}

}
