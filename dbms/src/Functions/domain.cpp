#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/domain.h>

namespace DB
{

struct NameDomain { static constexpr auto name = "domain"; };
using FunctionDomain = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false>>, NameDomain>;


void registerFunctionDomain(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDomain>();
}

}
