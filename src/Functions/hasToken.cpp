#include "FunctionsStringSearch.h"
#include <Functions/FunctionFactory.h>
#include "HasTokenImpl.h"
#include <Common/Volnitsky.h>


namespace DB
{
namespace
{

struct NameHasToken
{
    static constexpr auto name = "hasToken";
};

using FunctionHasToken = FunctionsStringSearch<HasTokenImpl<NameHasToken, VolnitskyCaseSensitiveToken, false>>;

}

void registerFunctionHasToken(FunctionFactory & factory)
{
    factory.registerFunction<FunctionHasToken>();
}

}
