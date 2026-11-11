#pragma once
#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

FunctionOverloadResolverPtr createInternalMultiIfOverloadResolver(bool allow_execute_multiif_columnar, bool use_variant_as_common_type);

}
