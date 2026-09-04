#pragma once
#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class IFunctionBase;

constexpr auto PLANNER_ONLY_FILTER_NAME = "__plannerOnlyFilter";

FunctionOverloadResolverPtr createInternalFunctionPlannerOnlyFilterResolver();

/// Whether `function` is the planner-only filter marker.
bool isPlannerOnlyFilterFunction(const IFunctionBase & function);

}
