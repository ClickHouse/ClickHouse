#pragma once

#include <Storages/Statistics/Statistics.h>


namespace DB
{

class StatisticsMinMax : public IStatistics
{
public:
    StatisticsMinMax(const SingleStatisticsDescription & stat_, const DataTypePtr & data_type_);
    ~StatisticsMinMax() override = default;

    Float64 estimateLess(const Field & val) const override;

    void update(const ColumnPtr & column) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

private:
    Float64 min;
    Float64 max;
    Float64 row_count;

    DataTypePtr data_type;
};

void minMaxValidator(const SingleStatisticsDescription &, DataTypePtr data_type);
StatisticsPtr minMaxCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type);

}
