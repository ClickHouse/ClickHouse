#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <Functions/EmptyImpl.h>


namespace DB
{
namespace
{

struct NameNotEmpty
{
    static constexpr auto name = "notEmpty";
};
using FunctionNotEmpty = FunctionStringOrArrayToT<EmptyImpl<true>, NameNotEmpty, UInt8>;

}

void registerFunctionNotEmpty(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNotEmpty>();
}

}
