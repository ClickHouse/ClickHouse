#include <Functions/FunctionFactory.h>
#include "fragment.h"
#include <Functions/FunctionStringToString.h>

namespace DB
{

struct NameCutFragment { static constexpr auto name = "cutFragment"; };
using FunctionCutFragment = FunctionStringToString<CutSubstringImpl<ExtractFragment<false>>, NameCutFragment>;

void registerFunctionCutFragment(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCutFragment>();
}

}
