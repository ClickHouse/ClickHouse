#pragma once
#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

constexpr auto PLANNER_ONLY_FILTER_NAME = "__plannerOnlyFilter";

FunctionOverloadResolverPtr createInternalFunctionPlannerOnlyFilterResolver();

}
