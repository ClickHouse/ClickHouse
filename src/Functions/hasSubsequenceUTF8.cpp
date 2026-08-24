#include <Functions/FunctionFactory.h>
#include <Functions/HasSubsequenceImpl.h>


namespace DB
{
namespace
{

struct HasSubsequenceCaseSensitiveUTF8
{
    static constexpr bool is_utf8 = true;

    static int toLowerIfNeed(int code_point) { return code_point; }
};

struct NameHasSubsequenceUTF8
{
    static constexpr auto name = "hasSubsequenceUTF8";
};

using FunctionHasSubsequenceUTF8 = HasSubsequenceImpl<NameHasSubsequenceUTF8, HasSubsequenceCaseSensitiveUTF8>;
}

REGISTER_FUNCTION(hasSubsequenceUTF8)
{
    factory.registerFunction<FunctionHasSubsequenceUTF8>({}, FunctionFactory::Case::Insensitive);
}

}
