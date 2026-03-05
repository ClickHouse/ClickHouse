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

using FunctionReplaceAll = FunctionStringReplace<ReplaceStringImpl<NameReplaceAll, ReplaceStringTraits::Replace::All>, NameReplaceAll>;

}

REGISTER_FUNCTION(ReplaceAll)
{
    factory.registerFunction<FunctionReplaceAll>();
    factory.registerAlias("replace", NameReplaceAll::name, FunctionFactory::Case::Insensitive);
}

}
