#include "FunctionsStringSearch.h"
#include <Functions/FunctionFactory.h>
#include "HasTokenImpl.h"
#include <Common/Volnitsky.h>


namespace DB
{
namespace
{

struct NameHasTokenCaseInsensitive
{
    static constexpr auto name = "hasTokenCaseInsensitive";
};

using FunctionHasTokenCaseInsensitive
    = FunctionsStringSearch<HasTokenImpl<NameHasTokenCaseInsensitive, VolnitskyCaseInsensitiveToken, false>>;

}

void registerFunctionHasTokenCaseInsensitive(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHasTokenCaseInsensitive>();
}

}
