#pragma once

#include <Storages/Statistics/Statistics.h>


namespace DB
{

class StatisticsMinMax : public IStatistics
{
public:
    StatisticsMinMax(const SingleStatisticsDescription & stat_, const DataTypePtr & data_type_);

    Float64 estimateLess(const Field & val) const override;

    void update(const ColumnPtr & column) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

private:
    Float64 min = std::numeric_limits<Float64>::max();
    Float64 max = std::numeric_limits<Float64>::min();
    UInt64 row_count = 0;

    DataTypePtr data_type;
};

void minMaxStatisticsValidator(const SingleStatisticsDescription &, DataTypePtr data_type);
StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type);

}
