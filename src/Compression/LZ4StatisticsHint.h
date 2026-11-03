#pragma once

#include <base/types.h>

namespace LZ4
{

/// Thread-local key of the column being decompressed, 0 if unknown. The read streams of one column usually share the
/// fastest decompression variant, so the LZ4 codec shares its decoder statistics between them on this thread.
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
