#include "FunctionFactory.h"
#include "FunctionsStringSearch.h"
#include "HasTokenImpl.h"

#include <Common/Volnitsky.h>

namespace
{
struct NameHasTokenCaseInsensitive
{
    static constexpr auto name = "hasTokenCaseInsensitive";
};

using FunctionHasTokenCaseInsensitive
    = DB::FunctionsStringSearch<DB::HasTokenImpl<NameHasTokenCaseInsensitive, DB::VolnitskyCaseInsensitiveToken, false>>;
using FunctionHasTokenCaseInsensitiveOrNull = DB::FunctionsStringSearch<
    DB::HasTokenImpl<NameHasTokenCaseInsensitive, DB::VolnitskyCaseInsensitiveToken, false>,
    DB::ExecutionErrorPolicy::Null>;
}

REGISTER_FUNCTION(HasTokenCaseInsensitive)
{
    factory.registerFunction<FunctionHasTokenCaseInsensitive>();
    factory.registerFunction<FunctionHasTokenCaseInsensitiveOrNull>();
}
