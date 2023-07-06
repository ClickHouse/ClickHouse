#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/HasSubsequenceImpl.h>

namespace DB
{
namespace
{

struct HasSubsequenceCaseInsensitiveASCII
{
    static void toLowerIfNeed(std::string & s) { std::transform(std::begin(s), std::end(s), std::begin(s), tolower); }
};

struct NameHasSubsequenceCaseInsensitive
{
    static constexpr auto name = "hasSubsequenceCaseInsensitive";
};

using FunctionHasSubsequenceCaseInsensitive = FunctionsStringSearch<HasSubsequenceImpl<NameHasSubsequenceCaseInsensitive, HasSubsequenceCaseInsensitiveASCII>>;
}

REGISTER_FUNCTION(hasSubsequenceCaseInsensitive)
{
    factory.registerFunction<FunctionHasSubsequenceCaseInsensitive>({}, FunctionFactory::CaseInsensitive);
}

}
