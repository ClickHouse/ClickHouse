#include <Functions/FunctionStringReplace.h>
#include <Functions/FunctionFactory.h>
#include <Functions/ReplaceRegexpImpl.h>


namespace DB
{
namespace
{

struct NameReplaceRegexpOne
{
    static constexpr auto name = "replaceRegexpOne";
};

using FunctionReplaceRegexpOne = FunctionStringReplace<ReplaceRegexpImpl<NameReplaceRegexpOne, ReplaceRegexpTraits::Replace::First>, NameReplaceRegexpOne>;

}

REGISTER_FUNCTION(ReplaceRegexpOne)
{
    factory.registerFunction<FunctionReplaceRegexpOne>();
}

}
