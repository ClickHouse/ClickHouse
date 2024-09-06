#pragma once
#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

FunctionOverloadResolverPtr createInternalMultiIfOverloadResolver(
    bool allow_execute_multiif_columnar_v, bool allow_experimental_variant_type_v, bool use_variant_as_common_type_v);
}
