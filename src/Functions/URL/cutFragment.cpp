#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/fragment.h>

namespace DB
{

struct NameCutFragment { static constexpr auto name = "cutFragment"; };
using FunctionCutFragment = FunctionStringToString<CutSubstringImpl<ExtractFragment<false>>, NameCutFragment>;

REGISTER_FUNCTION(CutFragment)
{
    factory.registerFunction<FunctionCutFragment>();
}

}
