#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceRegexpImpl.h"


namespace DB
{
namespace
{

struct NameReplaceRegexpOne
{
    static constexpr auto name = "replaceRegexpOne";
};

using FunctionReplaceRegexpOne = FunctionStringReplace<ReplaceRegexpImpl<true>, NameReplaceRegexpOne>;

}

void registerFunctionReplaceRegexpOne(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplaceRegexpOne>();
}

}
