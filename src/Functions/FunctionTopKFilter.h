#pragma once
#include <atomic>
#include <memory>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class ITopKThresholdTracker;
using TopKThresholdTrackerPtr = std::shared_ptr<ITopKThresholdTracker>;

/// Pass `rows_equal_to_threshold_dropped` only for a single sort column whose result is the sorted rows: the filter may
/// then drop the rows equal to the threshold, and sets the flag before it returns the first such result.
FunctionOverloadResolverPtr createInternalFunctionTopKFilterResolver(
    TopKThresholdTrackerPtr threshold_tracker_, std::shared_ptr<std::atomic<bool>> rows_equal_to_threshold_dropped = nullptr);

}
