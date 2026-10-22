#pragma once

#include <base/types.h>

namespace LZ4
{

/** The read streams of one column usually share the data distribution and therefore the fastest decompression variant.
  * A compressed read buffer that knows which column it reads sets this thread-local key around decompression,
  * and the LZ4 codec then shares its decoder statistics with the other streams of that column on this thread. 0 means unknown.
  */
UInt64 & currentStatisticsHint();

class StatisticsHintScope
{
public:
    explicit StatisticsHintScope(UInt64 hint) : saved(currentStatisticsHint()) { currentStatisticsHint() = hint; }
    ~StatisticsHintScope() { currentStatisticsHint() = saved; }

    StatisticsHintScope(const StatisticsHintScope &) = delete;
    StatisticsHintScope & operator=(const StatisticsHintScope &) = delete;

private:
    UInt64 saved;
};

}
