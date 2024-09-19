#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
namespace
{

struct NameMultiMatchAny
{
    static constexpr auto name = "multiMatchAny";
};

using FunctionMultiMatchAny = FunctionsMultiStringSearch<MultiMatchAnyImpl<NameMultiMatchAny, /*ResultType*/ UInt8, MultiMatchTraits::Find::Any, /*WithEditDistance*/ false>>;

}

REGISTER_FUNCTION(MultiMatchAny)
{
    factory.registerFunction<FunctionMultiMatchAny>();
}

FunctionOverloadResolverPtr createInternalMultiMatchAnyOverloadResolver(bool allow_hyperscan, size_t max_hyperscan_regexp_length, size_t max_hyperscan_regexp_total_length, bool reject_expensive_hyperscan_regexps)
{
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionMultiMatchAny>(allow_hyperscan, max_hyperscan_regexp_length, max_hyperscan_regexp_total_length, reject_expensive_hyperscan_regexps));
}

}
