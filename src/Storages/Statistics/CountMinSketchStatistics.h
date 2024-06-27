#pragma once

#include <Storages/Statistics/Statistics.h>

#include "config.h"

#if USE_DATASKETCHES

#include <count_min.hpp>

namespace DB
{

class CountMinSketchStatistics : public IStatistics
{
public:
    CountMinSketchStatistics(const SingleStatisticsDescription & stat_, DataTypePtr data_type_);

    Float64 estimateEqual(const Field & value) const;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

    void update(const ColumnPtr & column) override;

private:
    static constexpr auto num_hashes = 8uz;
    static constexpr auto num_buckets = 2048uz;

    using Sketch = datasketches::count_min_sketch<UInt64>;
    Sketch sketch;

    DataTypePtr data_type;
};

void CountMinSketchValidator(const SingleStatisticsDescription &, DataTypePtr data_type);
StatisticsPtr CountMinSketchCreator(const SingleStatisticsDescription & stat, DataTypePtr);

}

#endif
