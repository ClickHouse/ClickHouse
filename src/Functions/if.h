#pragma once
#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

FunctionOverloadResolverPtr createInternalFunctionIfOverloadResolver(bool use_variant_as_common_type, bool allow_lossy_numeric_supertype, bool use_low_cardinality_optimisation);

}
