#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperImpl.h>


namespace DB
{
namespace
{

struct NameLower
{
    static constexpr auto name = "lower";
};
using FunctionLower = FunctionStringToString<LowerUpperImpl<'A', 'Z'>, NameLower>;

}

void registerFunctionLower(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLower>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("lcase", NameLower::name, FunctionFactory::CaseInsensitive);
}

}
