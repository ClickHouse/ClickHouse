#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionEndsWithUTF8 = FunctionStartsEndsWith<NameEndsWithUTF8>;

REGISTER_FUNCTION(EndsWithUTF8)
{
    factory.registerFunction<FunctionEndsWithUTF8>();
}

}
