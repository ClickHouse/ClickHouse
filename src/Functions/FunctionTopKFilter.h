#pragma once
#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class TopKThresholdTracker;
using TopKThresholdTrackerPtr = std::shared_ptr<TopKThresholdTracker>;

FunctionOverloadResolverPtr createInternalFunctionTopKFilterResolver(TopKThresholdTrackerPtr threshold_tracker_);

}
