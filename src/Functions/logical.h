#pragma once
#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

FunctionOverloadResolverPtr createInternalFunctionOrOverloadResolver();
FunctionOverloadResolverPtr createInternalFunctionAndOverloadResolver();
FunctionOverloadResolverPtr createInternalFunctionXorOverloadResolver();
FunctionOverloadResolverPtr createInternalFunctionNotOverloadResolver();

}
