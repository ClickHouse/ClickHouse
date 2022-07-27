#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceRegexpImpl.h"


namespace DB
{
namespace
{

struct NameReplaceRegexpAll
{
    static constexpr auto name = "replaceRegexpAll";
};

using FunctionReplaceRegexpAll = FunctionStringReplace<ReplaceRegexpImpl<false>, NameReplaceRegexpAll>;

}

void registerFunctionReplaceRegexpAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplaceRegexpAll>();
    factory.registerAlias("REGEXP_REPLACE", NameReplaceRegexpAll::name, FunctionFactory::CaseInsensitive);
}

}
