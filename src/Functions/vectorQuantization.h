#pragma once

#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

/// Internal-only function `__quantizeDistance(code, query, method, dimensions, bits, is_l2)`: the approximate distance
/// between a data-independent `Quantize(...)` codec's code and a full-precision query, injected into the query plan by
/// the vector-search optimizer. It is not registered in `FunctionFactory` (not user-callable); the optimizer builds it
/// directly with this resolver.
FunctionOverloadResolverPtr createInternalFunctionQuantizeDistanceResolver();

}
