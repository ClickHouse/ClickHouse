#include <Functions/FunctionFactory.h>
#include <Functions/HasSubsequenceImpl.h>


namespace DB
{
namespace
{

struct HasSubsequenceCaseSensitiveASCII
{
    static constexpr bool is_utf8 = false;

    static int toLowerIfNeed(int c) { return c; }
};

struct NameHasSubsequence
{
    static constexpr auto name = "hasSubsequence";
};

using FunctionHasSubsequence = HasSubsequenceImpl<NameHasSubsequence, HasSubsequenceCaseSensitiveASCII>;
}

REGISTER_FUNCTION(hasSubsequence)
{
    factory.registerFunction<FunctionHasSubsequence>({}, FunctionFactory::Case::Insensitive);
}

}
