#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include "fragment.h"

namespace DB
{

struct NameFragment { static constexpr auto name = "fragment"; };
using FunctionFragment = FunctionStringToString<ExtractSubstringImpl<ExtractFragment<true>>, NameFragment>;

void registerFunctionFragment(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFragment>();
}

}
