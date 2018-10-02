#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperImpl.h>


namespace DB
{

struct NameLower
{
    static constexpr auto name = "lower";
};
using FunctionLower = FunctionStringToString<LowerUpperImpl<'A', 'Z'>, NameLower>;

void registerFunctionLower(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLower>();
}

}
