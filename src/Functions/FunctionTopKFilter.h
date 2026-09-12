#pragma once
#include <memory>
#include <string_view>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

struct TopKThresholdTracker;
using TopKThresholdTrackerPtr = std::shared_ptr<TopKThresholdTracker>;

/// The top-k threshold filter is built on demand around a runtime threshold tracker and is not
/// registered in `FunctionFactory`, so a plan can only recognise it by name.
inline constexpr std::string_view TOP_K_FILTER_FUNCTION_NAME = "__topKFilter";

FunctionOverloadResolverPtr createInternalFunctionTopKFilterResolver(TopKThresholdTrackerPtr threshold_tracker_);

}
