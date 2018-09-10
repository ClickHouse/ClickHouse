#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <Functions/EmptyImpl.h>


namespace DB
{

struct NameEmpty
{
    static constexpr auto name = "empty";
};
using FunctionEmpty = FunctionStringOrArrayToT<EmptyImpl<false>, NameEmpty, UInt8>;

void registerFunctionEmpty(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmpty>();
}

}

