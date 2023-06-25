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

using FunctionHasTokenCaseInsensitive
    = DB::FunctionsStringSearch<DB::HasTokenImpl<NameHasTokenCaseInsensitive, DB::VolnitskyCaseInsensitiveToken, false>>;

REGISTER_FUNCTION(HasTokenCaseInsensitive)
{
    factory.registerFunction<FunctionHasTokenCaseInsensitive>(
        FunctionDocumentation{.description="Performs case insensitive lookup of needle in haystack using tokenbf_v1 index."},
        DB::FunctionFactory::CaseInsensitive);
}

}
