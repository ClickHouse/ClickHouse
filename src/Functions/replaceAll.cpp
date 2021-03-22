#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceStringImpl.h"


namespace DB
{

struct NameReplaceAll
{
    static constexpr auto name = "replaceAll";
};

using FunctionReplaceAll = FunctionStringReplace<ReplaceStringImpl<false>, NameReplaceAll>;

void registerFunctionReplaceAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplaceAll>();
    factory.registerAlias("replace", NameReplaceAll::name, FunctionFactory::CaseInsensitive);
}

}
