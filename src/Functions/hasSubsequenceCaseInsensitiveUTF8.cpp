#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/HasSubsequenceImpl.h>

namespace DB
{
namespace
{

struct HasSubsequenceCaseInsensitiveUTF8
{
    static void toLowerIfNeed(String & s) { std::transform(std::begin(s), std::end(s), std::begin(s), tolower); }
};

struct NameHasSubsequenceCaseInsensitiveUTF8
{
    static constexpr auto name = "hasSubsequenceCaseInsensitiveUTF8";
};

using FunctionHasSubsequenceCaseInsensitiveUTF8 = FunctionsHasSubsequenceImpl<NameHasSubsequenceCaseInsensitiveUTF8, HasSubsequenceCaseInsensitiveUTF8>;
}

REGISTER_FUNCTION(hasSubsequenceCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionHasSubsequenceCaseInsensitiveUTF8>({}, FunctionFactory::CaseInsensitive);
}

}
