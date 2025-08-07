#include "FunctionStringReplace.h"
#include "FunctionFactory.h"
#include "ReplaceStringImpl.h"


namespace DB
{
namespace
{

struct NameReplaceOne
{
    static constexpr auto name = "replaceOne";
};

using FunctionReplaceOne = FunctionStringReplace<ReplaceStringImpl<NameReplaceOne, ReplaceStringTraits::Replace::First>, NameReplaceOne>;

}

REGISTER_FUNCTION(ReplaceOne)
{
    factory.registerFunction<FunctionReplaceOne>();
}

}
