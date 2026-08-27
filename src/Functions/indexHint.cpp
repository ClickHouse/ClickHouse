#include <Functions/indexHint.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

REGISTER_FUNCTION(IndexHint)
{
    factory.registerFunction<FunctionIndexHint>();
}

}
