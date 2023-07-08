#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/HasTokenImpl.h>

#include <Common/Volnitsky.h>

namespace DB
{
struct NameHasTokenCaseInsensitive
{
    static constexpr auto name = "hasTokenCaseInsensitive";
};

struct NameHasTokenCaseInsensitiveOrNull
{
    static constexpr auto name = "hasTokenCaseInsensitiveOrNull";
};

using FunctionHasTokenCaseInsensitive
    = DB::FunctionsStringSearch<DB::HasTokenImpl<NameHasTokenCaseInsensitive, DB::VolnitskyCaseInsensitiveToken, false>>;
using FunctionHasTokenCaseInsensitiveOrNull = DB::FunctionsStringSearch<
    DB::HasTokenImpl<NameHasTokenCaseInsensitiveOrNull, DB::VolnitskyCaseInsensitiveToken, false>,
    DB::ExecutionErrorPolicy::Null>;

REGISTER_FUNCTION(HasTokenCaseInsensitive)
{
    factory.registerFunction<FunctionHasTokenCaseInsensitive>(
        FunctionDocumentation{.description="Performs case insensitive lookup of needle in haystack using tokenbf_v1 index."},
        DB::FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionHasTokenCaseInsensitiveOrNull>(
        FunctionDocumentation{.description="Performs case insensitive lookup of needle in haystack using tokenbf_v1 index. Returns null if needle is ill-formed."},
        DB::FunctionFactory::CaseInsensitive);
}

}
