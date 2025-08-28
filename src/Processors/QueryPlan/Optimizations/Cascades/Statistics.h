#pragma once

#include <base/types.h>
#include <memory>
#include <optional>

namespace DB
{

class IOptimizerStatistics
{
public:
    virtual ~IOptimizerStatistics() = default;
    virtual std::optional<UInt64> getNumberOfDistinctValues(const String & column_name) const = 0;
};

using OptimizerStatisticsPtr = std::unique_ptr<IOptimizerStatistics>;

OptimizerStatisticsPtr createEmptyStatistics();
OptimizerStatisticsPtr createTPCH100Statistics();

}
