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
    StatisticsCountMinSketch(const SingleStatisticsDescription & description, const DataTypePtr & data_type_);

    Float64 estimateEqual(const Field & val) const override;

    void build(const ColumnPtr & column) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

private:
    using Sketch = datasketches::count_min_sketch<UInt64>;
    Sketch sketch;

    DataTypePtr data_type;
};


void countMinSketchStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr countMinSketchStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}

#endif
