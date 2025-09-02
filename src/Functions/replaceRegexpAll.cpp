#include <Functions/FunctionStringReplace.h>
#include <Functions/FunctionFactory.h>
#include <Functions/ReplaceRegexpImpl.h>


namespace DB
{
namespace
{

struct NameReplaceRegexpAll
{
    static constexpr auto name = "replaceRegexpAll";
};

using FunctionReplaceRegexpAll = FunctionStringReplace<ReplaceRegexpImpl<NameReplaceRegexpAll, ReplaceRegexpTraits::Replace::All>, NameReplaceRegexpAll>;

}

REGISTER_FUNCTION(ReplaceRegexpAll)
{
    factory.registerFunction<FunctionReplaceRegexpAll>();
    factory.registerAlias("REGEXP_REPLACE", NameReplaceRegexpAll::name, FunctionFactory::Case::Insensitive);
}

}
