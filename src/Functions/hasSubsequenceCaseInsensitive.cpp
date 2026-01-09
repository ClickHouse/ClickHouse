#include <Functions/FunctionFactory.h>
#include <Functions/HasSubsequenceImpl.h>

namespace DB
{
namespace
{

struct HasSubsequenceCaseInsensitiveASCII
{
    static constexpr bool is_utf8 = false;

    static int toLowerIfNeed(int c) { return std::tolower(c); }
};

struct NameHasSubsequenceCaseInsensitive
{
    static constexpr auto name = "hasSubsequenceCaseInsensitive";
};

using FunctionHasSubsequenceCaseInsensitive = HasSubsequenceImpl<NameHasSubsequenceCaseInsensitive, HasSubsequenceCaseInsensitiveASCII>;
}

REGISTER_FUNCTION(hasSubsequenceCaseInsensitive)
{
    factory.registerFunction<FunctionHasSubsequenceCaseInsensitive>({}, FunctionFactory::Case::Insensitive);
}

}
