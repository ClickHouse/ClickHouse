#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/HasSubsequenceImpl.h>

namespace DB
{
namespace
{

struct HasSubsequenceCaseInsensitiveASCII
{
    static constexpr bool is_utf8 = false;

    static void toLowerIfNeed(String & s) { std::transform(std::begin(s), std::end(s), std::begin(s), tolower); }
};

struct NameHasSubsequenceCaseInsensitive
{
    static constexpr auto name = "hasSubsequenceCaseInsensitive";
};

using FunctionHasSubsequenceCaseInsensitive = FunctionsHasSubsequenceImpl<NameHasSubsequenceCaseInsensitive, HasSubsequenceCaseInsensitiveASCII>;
}

REGISTER_FUNCTION(hasSubsequenceCaseInsensitive)
{
    factory.registerFunction<FunctionHasSubsequenceCaseInsensitive>({}, FunctionFactory::CaseInsensitive);
}

}
