#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperImpl.h>


namespace DB
{

struct NameUpper
{
    static constexpr auto name = "upper";
};
using FunctionUpper = FunctionStringToString<LowerUpperImpl<'a', 'z'>, NameUpper>;

void registerFunctionUpper(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUpper>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("ucase", FunctionUpper::name, FunctionFactory::CaseInsensitive);
}

}
