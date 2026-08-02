#pragma once
#include <memory>

#include <Interpreters/Context_fwd.h>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

FunctionOverloadResolverPtr createInternalFunctionHasOverloadResolver(ContextPtr context);

}
