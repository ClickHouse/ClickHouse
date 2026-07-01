#pragma once

#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

/// Internal-only function `__pqDistance(code, codebook, query, dimensions, m, nbits, is_l2)`: the asymmetric distance
/// between a trained Product-Quantization code (with its per-part codebook) and a full-precision query, injected into
/// the query plan by the vector-search optimizer. It is not registered in `FunctionFactory` (not user-callable); the
/// optimizer builds it directly with this resolver.
FunctionOverloadResolverPtr createInternalFunctionPQDistanceResolver();

}
