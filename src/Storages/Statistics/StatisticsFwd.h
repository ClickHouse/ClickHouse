#pragma once

#include <memory>

namespace DB
{

class IStatistics;
using StatisticsPtr = std::shared_ptr<IStatistics>;

}
