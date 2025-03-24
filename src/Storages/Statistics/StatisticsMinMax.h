#pragma once

#include <Storages/Statistics/Statistics.h>
#include <DataTypes/IDataType.h>


namespace DB
{

class StatisticsMinMax : public IStatistics
{
public:
    StatisticsMinMax(const SingleStatisticsDescription & statistics_description, const DataTypePtr & data_type_);

    void build(const ColumnPtr & column) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf) override;

    Float64 estimateLess(const Field & val) const override;

private:
    Float64 min = std::numeric_limits<Float64>::max();
    Float64 max = std::numeric_limits<Float64>::min();
    UInt64 row_count = 0;

    DataTypePtr data_type;
};

void minMaxStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr minMaxStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
