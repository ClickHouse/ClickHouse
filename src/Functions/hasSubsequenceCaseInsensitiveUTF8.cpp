#include <Functions/FunctionFactory.h>
#include <Functions/HasSubsequenceImpl.h>

#include "Poco/Unicode.h"

namespace DB
{
namespace
{

struct HasSubsequenceCaseInsensitiveUTF8
{
    static constexpr bool is_utf8 = true;

    static int toLowerIfNeed(int code_point) { return Poco::Unicode::toLower(code_point); }
};

struct NameHasSubsequenceCaseInsensitiveUTF8
{
    static constexpr auto name = "hasSubsequenceCaseInsensitiveUTF8";
};

using FunctionHasSubsequenceCaseInsensitiveUTF8 = HasSubsequenceImpl<NameHasSubsequenceCaseInsensitiveUTF8, HasSubsequenceCaseInsensitiveUTF8>;
}

REGISTER_FUNCTION(hasSubsequenceCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionHasSubsequenceCaseInsensitiveUTF8>({}, FunctionFactory::Case::Insensitive);
}

}
