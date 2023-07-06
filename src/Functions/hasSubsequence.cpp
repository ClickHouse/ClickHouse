#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/HasSubsequenceImpl.h>


namespace DB
{
namespace
{

struct HasSubsequenceCaseSensitiveASCII
{
    static void toLowerIfNeed(std::string & /*s*/) { }
};

struct NameHasSubsequence
{
    static constexpr auto name = "hasSubsequence";
};

using FunctionHasSubsequence = FunctionsStringSearch<HasSubsequenceImpl<NameHasSubsequence, HasSubsequenceCaseSensitiveASCII>>;
}

REGISTER_FUNCTION(hasSubsequence)
{
    factory.registerFunction<FunctionHasSubsequence>({}, FunctionFactory::CaseInsensitive);
}

}
