#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceStringImpl.h"


namespace DB
{
namespace
{

struct NameReplaceAll
{
    static constexpr auto name = "replaceAll";
};

using FunctionReplaceAll = FunctionStringReplace<ReplaceStringImpl<false>, NameReplaceAll>;

}

void registerFunctionReplaceAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplaceAll>();
    factory.registerAlias("replace", NameReplaceAll::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("REGEXP_REPLACE", NameReplaceAll::name, FunctionFactory::CaseInsensitive);
}

}
