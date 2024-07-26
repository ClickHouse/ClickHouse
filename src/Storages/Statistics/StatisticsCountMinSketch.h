#pragma once

#include <Storages/Statistics/Statistics.h>

#include "config.h"

#if USE_DATASKETCHES

#include <count_min.hpp>

namespace DB
{

class StatisticsCountMinSketch : public IStatistics
{
public:
    StatisticsCountMinSketch(const SingleStatisticsDescription & stat_, DataTypePtr data_type_);

    Float64 estimateEqual(const Field & val) const override;

    void update(const ColumnPtr & column) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

private:
    using Sketch = datasketches::count_min_sketch<UInt64>;
    Sketch sketch;

    DataTypePtr data_type;
};


void countMinSketchValidator(const SingleStatisticsDescription &, DataTypePtr data_type);
StatisticsPtr countMinSketchCreator(const SingleStatisticsDescription & stat, DataTypePtr);

}

#endif
