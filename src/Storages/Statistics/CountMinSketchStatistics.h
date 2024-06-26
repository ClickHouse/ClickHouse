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

    bool checkType(const Field & f);

private:
    static constexpr auto HASH_COUNT = 8uz;
    static constexpr auto BUCKET_COUNT = 2048uz;

    datasketches::count_min_sketch<Float64> sketch;
    DataTypePtr data_type;
};

StatisticsPtr CountMinSketchCreator(const SingleStatisticsDescription & stat, DataTypePtr);
void CountMinSketchValidator(const SingleStatisticsDescription &, DataTypePtr data_type);

}

#endif
