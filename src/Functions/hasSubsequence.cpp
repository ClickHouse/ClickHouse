#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/HasSubsequenceImpl.h>


namespace DB
{
namespace
{

struct HasSubsequenceCaseSensitiveASCII
{
    static constexpr bool is_utf8 = false;

    static void toLowerIfNeed(String & /*s*/) { }
};

struct NameHasSubsequence
{
    static constexpr auto name = "hasSubsequence";
};

using FunctionHasSubsequence = FunctionsHasSubsequenceImpl<NameHasSubsequence, HasSubsequenceCaseSensitiveASCII>;
}

REGISTER_FUNCTION(hasSubsequence)
{
    factory.registerFunction<FunctionHasSubsequence>({}, FunctionFactory::CaseInsensitive);
}

}
