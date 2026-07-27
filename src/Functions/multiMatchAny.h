#pragma once
#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

FunctionOverloadResolverPtr createInternalMultiMatchAnyOverloadResolver(bool allow_hyperscan, size_t max_hyperscan_regexp_length, size_t max_hyperscan_regexp_total_length, bool reject_expensive_hyperscan_regexps);

}
