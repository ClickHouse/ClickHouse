#include "FunctionsStringSearch.h"
#include <Functions/FunctionFactory.h>
#include "HasTokenImpl.h"
#include <Common/Volnitsky.h>


namespace DB
{

struct NameHasTokenCaseInsensitive
{
    static constexpr auto name = "hasTokenCaseInsensitive";
};

using FunctionHasTokenCaseInsensitive
    = FunctionsStringSearch<HasTokenImpl<VolnitskyCaseInsensitiveToken, false>, NameHasTokenCaseInsensitive>;

void registerFunctionHasTokenCaseInsensitive(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHasTokenCaseInsensitive>();
}

}
