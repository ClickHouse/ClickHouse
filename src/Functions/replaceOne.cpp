#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceStringImpl.h"


namespace DB
{

struct NameReplaceOne
{
    static constexpr auto name = "replaceOne";
};

using FunctionReplaceOne = FunctionStringReplace<ReplaceStringImpl<true>, NameReplaceOne>;

void registerFunctionReplaceOne(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReplaceOne>();
}

}
